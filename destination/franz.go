// Copyright © 2023 Meroxa, Inc.
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

package destination

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/kafkaconnect"
	"github.com/goccy/go-json"
	"github.com/twmb/franz-go/pkg/kgo"
)

type FranzProducer struct {
	client     *kgo.Client
	keyEncoder dataEncoder
}

var _ Producer = (*FranzProducer)(nil)

func NewFranzProducer(ctx context.Context, cfg Config) (*FranzProducer, error) {
	opts := cfg.FranzClientOpts(sdk.Logger(ctx))
	opts = append(opts, []kgo.Opt{
		kgo.AllowAutoTopicCreation(),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.RequiredAcks(cfg.RequiredAcks()),
		kgo.ProducerBatchCompression(cfg.CompressionCodecs()...),
		kgo.ProducerBatchMaxBytes(cfg.BatchBytes),
	}...)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	var keyEncoder dataEncoder = bytesEncoder{}
	if cfg.useKafkaConnectKeyFormat {
		keyEncoder = kafkaConnectEncoder{}
	}

	return &FranzProducer{
		client:     cl,
		keyEncoder: keyEncoder,
	}, nil
}

func (p *FranzProducer) Produce(ctx context.Context, records []sdk.Record) (int, error) {
	kafkaRecs := make([]*kgo.Record, len(records))
	for i, r := range records {
		encodedKey, err := p.keyEncoder.Encode(r.Key)
		if err != nil {
			return 0, fmt.Errorf("could not encode key of record %v: %w", i, err)
		}
		kafkaRecs[i] = &kgo.Record{
			Key:   encodedKey,
			Value: r.Bytes(),
		}
	}

	results := p.client.ProduceSync(ctx, kafkaRecs...)
	for i, r := range results {
		if r.Err != nil {
			return i, r.Err
		}
	}

	return len(results), nil
}

func (p *FranzProducer) Close(_ context.Context) error {
	if p.client != nil {
		p.client.Close()
	}
	return nil
}

// dataEncoder is similar to a sdk.Encoder, which takes data and encodes it in
// a certain format. The producer uses this to encode the key of the kafka
// message.
type dataEncoder interface {
	Encode(sdk.Data) ([]byte, error)
}

// bytesEncoder is a dataEncoder that simply calls data.Bytes().
type bytesEncoder struct{}

func (bytesEncoder) Encode(data sdk.Data) ([]byte, error) {
	return data.Bytes(), nil
}

// kafkaConnectEncoder encodes the data into a kafka connect JSON with schema
// (NB: this is not the same as JSONSchema).
type kafkaConnectEncoder struct{}

func (e kafkaConnectEncoder) Encode(data sdk.Data) ([]byte, error) {
	sd := e.toStructuredData(data)
	schema := kafkaconnect.Reflect(sd)
	if schema == nil {
		// s is nil, let's write an empty struct in the schema
		schema = &kafkaconnect.Schema{
			Type:     kafkaconnect.TypeStruct,
			Optional: true,
		}
	}

	env := kafkaconnect.Envelope{
		Schema:  *schema,
		Payload: sd,
	}
	// TODO add support for other encodings than JSON
	return json.Marshal(env)
}

// toStructuredData tries its best to return StructuredData.
func (kafkaConnectEncoder) toStructuredData(d sdk.Data) sdk.Data {
	switch d := d.(type) {
	case nil:
		return nil
	case sdk.StructuredData:
		return d
	case sdk.RawData:
		// try parsing the raw data as json
		var sd sdk.StructuredData
		err := json.Unmarshal(d, &sd)
		if err != nil {
			// it's not JSON, nothing more we can do
			return d
		}
		return sd
	default:
		// should not be possible
		panic(fmt.Errorf("unknown data type: %T", d))
	}
}
