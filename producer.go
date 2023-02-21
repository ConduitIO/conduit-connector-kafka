// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:generate mockgen -destination mock/producer.go -package mock -mock_names=Producer=Producer . Producer

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/avast/retry-go"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/kafkaconnect"
	"github.com/segmentio/kafka-go"
)

type Producer interface {
	// Send sends all records to Kafka synchronously.
	Send(ctx context.Context, records []sdk.Record) (int, error)

	// Close this producer and the associated resources (e.g. connections to the broker)
	Close() error
}

type segmentProducer struct {
	writer   *kafka.Writer
	balancer *batchSizeAdjustingBalancer

	keyEncoding func(sdk.Data) ([]byte, error)
}

// NewProducer creates a new Kafka producer.
// The current implementation uses Segment's kafka-go client.
func NewProducer(cfg Config) (Producer, error) {
	if len(cfg.Servers) == 0 {
		return nil, ErrServersMissing
	}
	if cfg.Topic == "" {
		return nil, ErrTopicMissing
	}

	p := &segmentProducer{}
	err := p.init(cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't initialize producer: %w", err)
	}
	return p, nil
}

func (p *segmentProducer) init(cfg Config) error {
	p.balancer = &batchSizeAdjustingBalancer{}
	p.writer = &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Servers...),
		Topic:                  cfg.Topic,
		WriteTimeout:           cfg.DeliveryTimeout,
		RequiredAcks:           cfg.Acks,
		MaxAttempts:            3,
		AllowAutoTopicCreation: true,

		Balancer:     p.balancer,
		BatchTimeout: time.Millisecond, // partial batches will be written after 1 millisecond
	}
	p.balancer.writer = p.writer

	p.keyEncoding = p.keyEncodingBytes
	if cfg.IsRecordFormatDebezium {
		p.keyEncoding = p.keyEncodingDebezium
	}

	err := p.configureSecurity(cfg)
	if err != nil {
		return fmt.Errorf("couldn't configure security: %w", err)
	}
	return nil
}

func (p *segmentProducer) configureSecurity(cfg Config) error {
	transport := &kafka.Transport{}
	// TLS settings
	if cfg.useTLS() {
		tlsCfg, err := newTLSConfig(cfg.ClientCert, cfg.ClientKey, cfg.CACert, cfg.InsecureSkipVerify)
		if err != nil {
			return fmt.Errorf("invalid TLS config: %w", err)
		}
		transport.TLS = tlsCfg
	}

	// SASL
	if cfg.saslEnabled() {
		mechanism, err := newSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return fmt.Errorf("couldn't configure SASL: %w", err)
		}
		transport.SASL = mechanism
	}
	p.writer.Transport = transport

	return nil
}

func (p *segmentProducer) Send(ctx context.Context, records []sdk.Record) (int, error) {
	p.balancer.SetRecordCount(len(records))
	messages := make([]kafka.Message, len(records))
	for i, r := range records {
		encodedKey, err := p.keyEncoding(r.Key)
		if err != nil {
			return 0, nil
		}
		messages[i] = kafka.Message{
			Key:   encodedKey,
			Value: r.Bytes(),
		}
	}

	err := p.sendRetryable(ctx, messages)
	if err != nil {
		werr, ok := err.(kafka.WriteErrors)
		if !ok {
			return 0, fmt.Errorf("failed to produce messages: %w", err)
		}
		// multiple errors occurred, we loop through the errors and fetch the
		// first non-nil error, we log all others
		count := 0
		err = nil
		for i := range werr {
			switch {
			case werr[i] != nil && err == nil:
				// the first message that failed to be produced - we will return
				// this one
				count = i
				err = werr[i]
			case werr[i] != nil && err != nil:
				// a message that failed to be produced after an already failed
				// message - we will log this one
				sdk.Logger(ctx).Err(werr[i]).Bytes("record_position", records[i].Position).Msg("failed to produce message")
			case werr[i] == nil && err != nil:
				// a message that we successfully produced after a message that
				// failed - we need to log a warning, this one will be
				// duplicated if it's reprocessed
				sdk.Logger(ctx).Warn().Bytes("record_position", records[i].Position).Msg("this message was produced after a previous message failed to be produced - if you restart the pipeline this message will be produced again")
			}
		}
		return count, err
	}
	return len(records), nil
}

func (p *segmentProducer) sendRetryable(ctx context.Context, messages []kafka.Message) error {
	return retry.Do(
		func() error {
			return p.writer.WriteMessages(
				context.Background(),
				messages...,
			)
		},
		retry.RetryIf(func(err error) bool {
			// this can happen when the topic doesn't exist and the broker has auto-create enabled
			// we give it some time to process topic metadata and retry
			kErr, ok := err.(kafka.Error)
			return ok && kErr.Temporary()
		}),
		retry.OnRetry(func(n uint, err error) {
			sdk.Logger(ctx).
				Info().
				Err(err).
				Msgf("retrying write, attempt #%v", n)
		}),
		retry.Delay(time.Second),
		retry.Attempts(10),
		retry.LastErrorOnly(true),
	)
}

func (p *segmentProducer) Close() error {
	if p.writer == nil {
		return nil
	}
	// this will also make the loops in the reader goroutines stop
	err := p.writer.Close()
	if err != nil {
		return fmt.Errorf("couldn't close writer: %w", err)
	}

	// close idle connections if possible
	closeIdleTransport, ok := p.writer.Transport.(interface {
		CloseIdleConnections()
	})
	if ok {
		closeIdleTransport.CloseIdleConnections()
	}

	return nil
}

func (p *segmentProducer) keyEncodingBytes(k sdk.Data) ([]byte, error) {
	return k.Bytes(), nil
}

func (p *segmentProducer) keyEncodingDebezium(d sdk.Data) ([]byte, error) {
	d = p.sanitizeData(d)
	s := kafkaconnect.Reflect(d)
	if s == nil {
		// s is nil, let's write an empty struct in the schema
		s = &kafkaconnect.Schema{
			Type:     kafkaconnect.TypeStruct,
			Optional: true,
		}
	}

	e := kafkaconnect.Envelope{
		Schema:  *s,
		Payload: d,
	}
	// TODO add support for other encodings than JSON
	return json.Marshal(e)
}

// sanitizeData tries its best to return StructuredData.
func (p *segmentProducer) sanitizeData(d sdk.Data) sdk.Data {
	switch d := d.(type) {
	case nil:
		return nil
	case sdk.StructuredData:
		return d
	case sdk.RawData:
		sd, err := p.parseRawDataAsJSON(d)
		if err != nil {
			// oh well, can't be done
			return d
		}
		return sd
	default:
		// should not be possible
		panic(fmt.Errorf("unknown data type: %T", d))
	}
}
func (p *segmentProducer) parseRawDataAsJSON(d sdk.RawData) (sdk.StructuredData, error) {
	// We have raw data, we need structured data.
	// We can do our best and try to convert it if RawData is carrying raw JSON.
	var sd sdk.StructuredData
	err := json.Unmarshal(d, &sd)
	if err != nil {
		return nil, fmt.Errorf("could not parse RawData as JSON: %w", err)
	}
	return sd, nil
}

// batchSizeAdjustingBalancer is a balancer that adjusts the batch size of the
// writer based on the number of messages that we want to write.
// Before calling Writer.WriteMessages you can call SetRecordCount on the
// balancer to let it know how many messages will be written. First time the
// Balance method is called it will calculate the appropriate batch size based
// on the number of partitions and adjust the setting in the writer.
// The actual balancing logic after that is delegated to kafka.RoundRobin.
//
// When the record count is divisible by the number of partitions, this balancer
// will ensure that all batches are flushed instantly. If it's not divisible and
// the record count is greater than the number of partitions, some batches will
// be flushed only after the batch delay is reached.
//
// This is a workaround and best approximation for achieving synchronous writes,
// because the segment client doesn't allow you to write without batching.
// For more info see https://github.com/segmentio/kafka-go/issues/852.
type batchSizeAdjustingBalancer struct {
	kafka.RoundRobin
	writer *kafka.Writer

	recordCount int
	once        sync.Once
}

func (b *batchSizeAdjustingBalancer) SetRecordCount(count int) {
	b.recordCount = count
	b.once = sync.Once{} // make sure new batch size is calculated
}

// Balance satisfies the kafka.Balancer interface.
func (b *batchSizeAdjustingBalancer) Balance(msg kafka.Message, partitions ...int) int {
	b.once.Do(func() {
		// kafka.Writer.BatchSize is the batch size for a single partition. We
		// know the number of messages we are writing and because we are using
		// the round robin balancer we know that a single partition will receive
		// at most ceil(recordCount/partitions) messages. We set the batch size
		// accordingly.
		// trick for ceil without cast to float: ceil(a/b) = (a+b-1)/b
		b.writer.BatchSize = (b.recordCount + len(partitions) - 1) / len(partitions)
	})
	return b.RoundRobin.Balance(msg, partitions...)
}
