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
	"github.com/segmentio/kafka-go"
)

type Source struct {
	sdk.UnimplementedSource

	Consumer Consumer
	Config   Config
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring a source...")
	parsed, err := Parse(cfg)
	if err != nil {
		return fmt.Errorf("config is invalid: %w", err)
	}
	s.Config = parsed
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	sdk.Logger(ctx).Info().Bytes("position", pos).Msg("Opening a source...")

	err := s.Config.Test(ctx)
	if err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	client, err := NewConsumer()
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}
	s.Consumer = client

	err = s.Consumer.StartFrom(s.Config, pos)
	if err != nil {
		return fmt.Errorf("couldn't open source at position %v: %w", string(pos), err)
	}

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	message, pos, err := s.Consumer.Get(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed getting a message %w", err)
	}
	if message == nil {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
	rec, err := toRecord(message, pos)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("couldn't transform record %w", err)
	}
	return rec, nil
}

func toRecord(message *kafka.Message, position []byte) (sdk.Record, error) {
	return sdk.Record{
		Position:  position,
		CreatedAt: message.Time,
		Key:       sdk.RawData(message.Key),
		Payload:   sdk.RawData(message.Value),
	}, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return s.Consumer.Ack(position)
}

func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down a Kafka Source...")
	if s.Consumer != nil {
		err := s.Consumer.Close()
		if err != nil {
			return fmt.Errorf("failed closing Kafka consumer: %w", err)
		}
	}
	return nil
}
