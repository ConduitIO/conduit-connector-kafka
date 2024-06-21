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

package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	producer destination.Producer
	config   destination.Config
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() config.Parameters {
	return destination.Config{}.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		return err
	}
	err = d.config.Validate()
	if err != nil {
		return err
	}

	recordFormat := cfg[sdk.DestinationWithRecordFormat{}.RecordFormatParameterName()]
	if recordFormat != "" {
		recordFormatType, _, _ := strings.Cut(recordFormat, "/")
		if recordFormatType == (sdk.DebeziumConverter{}.Name()) {
			d.config = d.config.WithKafkaConnectKeyFormat()
		}
	}

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	err := d.config.TryDial(ctx)
	if err != nil {
		return fmt.Errorf("failed to dial broker: %w", err)
	}

	d.producer, err = destination.NewFranzProducer(ctx, d.config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	return d.producer.Produce(ctx, records)
}

// Teardown shuts down the Kafka client.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.producer != nil {
		err := d.producer.Close(ctx)
		if err != nil {
			return fmt.Errorf("failed closing Kafka producer: %w", err)
		}
	}
	return nil
}
