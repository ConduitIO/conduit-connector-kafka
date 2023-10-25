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
	cfg    Config
	client *kgo.Client
}

var _ Producer = (*FranzProducer)(nil)

func NewFranzProducer(_ context.Context, cfg Config) (*FranzProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Servers...),
		kgo.ClientID(cfg.ClientID),

		kgo.AllowAutoTopicCreation(),
		kgo.DefaultProduceTopic(cfg.Topic),
	)
	if err != nil {
		return nil, err
	}

	return &FranzProducer{
		client: cl,
	}, nil
}

func (p *FranzProducer) Produce(ctx context.Context, records []sdk.Record) (int, error) {
	messages := make([]*kgo.Record, len(records))
	for i, r := range records {
		messages[i] = &kgo.Record{
			Key:   r.Key.Bytes(),
			Value: r.Bytes(),
		}
	}

	results := p.client.ProduceSync(ctx, messages...)
	for i, r := range results {
		if r.Err != nil {
			return i, r.Err
		}
	}

	return len(results), nil
}

func (p *FranzProducer) Close(_ context.Context) error {
	if p.client == nil {
		return nil
	}
	p.client.Close()
	return nil
}

func (p *FranzProducer) keyEncodingBytes(k sdk.Data) ([]byte, error) {
	return k.Bytes(), nil
}

func (p *FranzProducer) keyEncodingDebezium(d sdk.Data) ([]byte, error) {
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
func (p *FranzProducer) sanitizeData(d sdk.Data) sdk.Data {
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
func (p *FranzProducer) parseRawDataAsJSON(d sdk.RawData) (sdk.StructuredData, error) {
	// We have raw data, we need structured data.
	// We can do our best and try to convert it if RawData is carrying raw JSON.
	var sd sdk.StructuredData
	err := json.Unmarshal(d, &sd)
	if err != nil {
		return nil, fmt.Errorf("could not parse RawData as JSON: %w", err)
	}
	return sd, nil
}
