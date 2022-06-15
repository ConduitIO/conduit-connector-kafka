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

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	Producer Producer
	Config   Config
}

func NewDestination() sdk.Destination {
	return &Destination{}
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring a Kafka Destination...")
	parsed, err := Parse(cfg)
	if err != nil {
		return fmt.Errorf("config is invalid: %w", err)
	}
	d.Config = parsed
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	err := d.Config.Test(ctx)
	if err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	producer, err := NewProducer(d.Config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	d.Producer = producer
	return nil
}

func (d *Destination) WriteAsync(ctx context.Context, record sdk.Record, ackFunc sdk.AckFunc) error {
	err := d.Producer.Send(
		ctx,
		record.Key.Bytes(),
		record.Payload.Bytes(),
		record.Position,
		ackFunc,
	)
	if err != nil {
		// Producer.Send() will call ackFunc, so there's no need to do it here too.
		return fmt.Errorf("message not delivered: %w", err)
	}

	return nil
}

func (d *Destination) Flush(context.Context) error {
	return nil
}

// Teardown shuts down the Kafka client.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down a Kafka Destination...")
	if d.Producer != nil {
		_ = d.Producer.Close()
	}
	return nil
}
