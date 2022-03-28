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
	"errors"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/segmentio/kafka-go"
)

const writeRetry = 15 * time.Second

type Destination struct {
	sdk.UnimplementedDestination

	Client Producer
	Config Config
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
	client, err := NewProducer(d.Config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}

	d.Client = client
	return nil
}

func (d *Destination) Write(ctx context.Context, record sdk.Record) error {
	return d.writeInternal(ctx, record, true)
}

func (d *Destination) writeInternal(ctx context.Context, record sdk.Record, retry bool) error {
	err := d.Client.Send(
		record.Key.Bytes(),
		record.Payload.Bytes(),
	)
	// this can happen when the topic doesn't exist and the broker has auto-create enabled
	// we give it some time to process topic metadata and retry
	if retry && errors.Is(err, kafka.LeaderNotAvailable) {
		sdk.Logger(ctx).
			Info().
			Err(err).
			Msgf("leader for topic unavailable, will retry in %v", writeRetry)
		time.Sleep(writeRetry)
		return d.writeInternal(ctx, record, false)
	}
	if err != nil {
		return fmt.Errorf("message not delivered %w", err)
	}
	return nil
}

func (d *Destination) Flush(context.Context) error {
	return nil
}

// Teardown shuts down the Kafka client.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down a Kafka Destination...")
	if d.Client != nil {
		d.Client.Close()
	}
	return nil
}
