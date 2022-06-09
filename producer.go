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
	"fmt"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/segmentio/kafka-go"
)

const kafkaMessageIDHeader = "conduit-id"

type Producer interface {
	// Send sends a message to Kafka asynchronously.
	// `messageID` parameter uniquely identifies a message.
	// `ackFunc` is called when the produces actually sends the messages
	// (successfully or unsuccessfully).
	Send(key []byte, payload []byte, messageID []byte, ackFunc sdk.AckFunc) error

	// Close this producer and the associated resources (e.g. connections to the broker)
	Close() error
}

type segmentProducer struct {
	writer *kafka.Writer
	// ackFuncs is a map of message IDs (i.e. Conduit positions)
	// to respective ack functions.
	ackFuncs map[string]sdk.AckFunc
	// m synchronizes access to ackFuncs
	m sync.Mutex
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

	p := &segmentProducer{
		ackFuncs: make(map[string]sdk.AckFunc),
	}
	err := p.newWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't create writer: %w", err)
	}
	return p, nil
}

func (p *segmentProducer) newWriter(cfg Config) error {
	p.writer = &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Servers...),
		Topic:                  cfg.Topic,
		BatchSize:              1,
		WriteTimeout:           cfg.DeliveryTimeout,
		RequiredAcks:           cfg.Acks,
		MaxAttempts:            3,
		AllowAutoTopicCreation: true,
		Async:                  true,
		Completion:             p.onMessageDelivery,
	}
	err := p.configureSecurity(cfg)
	if err != nil {
		return fmt.Errorf("couldn't configure security: %w", err)
	}
	return nil
}

// onMessageDelivery is a callback function for kafka-go's writer
// which is calling the ack functions for the messages which were
// (successfully or unsuccessfully) delivered to Kafka.
func (p *segmentProducer) onMessageDelivery(messages []kafka.Message, err error) {
	if len(messages) == 0 {
		return
	}
	for _, m := range messages {
		// accessed also when in Send(), when messages need to be sent
		p.m.Lock()
		ackFunc, ok := p.ackFuncs[p.getID(m)]
		delete(p.ackFuncs, p.getID(m))
		p.m.Unlock()

		if !ok {
			sdk.Logger(context.Background()).
				Error().
				Msgf("no ack function for %v registered", p.getID(m))
			// Either Conduit didn't send an AckFunc (pretty unlikely)
			// or this connector got an AckFunc, but didn't store it.
			// Either way, without this message being acknowledged,
			// the source record cannot be ack'd either, which means
			// that no position (progress) past this will be persisted.
			// That implies that after a restart, everything after this position
			// will be redone anyway.
			panic(fmt.Errorf("ack func for %v not registered", p.getID(m)))
		}
		ackErr := ackFunc(err)
		// This means one of two:
		// (1) bug in Conduit itself
		// (2) for standalone plugins: the gRPC stream failed
		// Either way, it makes sense for the connector to continue working.
		if ackErr != nil {
			sdk.Logger(context.Background()).
				Err(ackErr).
				Msg("ack function returned an error")
			panic(fmt.Errorf("ack func for %v failed: %w", p.getID(m), ackErr))
		}
	}
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

func (p *segmentProducer) Send(key []byte, payload []byte, id []byte, ackFunc sdk.AckFunc) error {
	// accessed also in onMessageDelivery, which is
	// calling and removing the ack functions
	p.m.Lock()
	p.ackFuncs[string(id)] = ackFunc
	p.m.Unlock()

	err := p.writer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   key,
			Value: payload,
			Headers: []kafka.Header{
				{
					Key:   kafkaMessageIDHeader,
					Value: id,
				},
			},
		},
	)

	if err != nil {
		return fmt.Errorf("message not delivered: %w", err)
	}
	return nil
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

	return nil
}

func (p *segmentProducer) getID(m kafka.Message) string {
	for _, h := range m.Headers {
		if h.Key == kafkaMessageIDHeader {
			return string(h.Value)
		}
	}
	return ""
}
