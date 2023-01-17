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

	"github.com/segmentio/kafka-go"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// MetadataKafkaTopic is the metadata key for storing the kafka topic
	MetadataKafkaTopic = "kafka.topic"
)

type Source struct {
	sdk.UnimplementedSource

	Consumer Consumer
	Config   Config
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		Servers: {
			Default:     "",
			Required:    true,
			Description: "A list of bootstrap servers to which the plugin will connect.",
		},
		Topic: {
			Default:     "",
			Required:    true,
			Description: "The topic to which records will be written to.",
		},
		ReadFromBeginning: {
			Default:     "false",
			Required:    false,
			Description: "Whether or not to read a topic from beginning (i.e. existing messages or only new messages).",
		},
		GroupID: {
			Default:     "",
			Required:    false,
			Description: "The Consumer Group ID to use for the Kafka Consumer on the Source connector.",
		},
		ClientCert: {
			Default:     "",
			Required:    false,
			Description: "A certificate for the Kafka client, in PEM format. If provided, the private key needs to be provided too.",
		},
		ClientKey: {
			Default:     "",
			Required:    false,
			Description: "A private key for the Kafka client, in PEM format. If provided, the certificate needs to be provided too.",
		},
		CACert: {
			Default:     "",
			Required:    false,
			Description: "The Kafka broker's certificate, in PEM format.",
		},
		InsecureSkipVerify: {
			Default:  "false",
			Required: false,
			Description: "Controls whether a client verifies the server's certificate chain and host name. " +
				"If `true`, accepts any certificate presented by the server and any host name in that certificate.",
		},
		SASLMechanism: {
			Default:     "",
			Required:    false,
			Description: "SASL mechanism to be used. Possible values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.",
		},
		SASLUsername: {
			Default:     "",
			Required:    false,
			Description: "SASL username. required if saslMechanism is provided.",
		},
		SASLPassword: {
			Default:     "",
			Required:    false,
			Description: "SASL password. required if saslMechanism is provided.",
		},
	}
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
		return sdk.Record{}, fmt.Errorf("failed getting a message: %w", err)
	}
	if message == nil {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
	rec := s.buildRecord(message, pos)
	return rec, nil
}

func (s *Source) buildRecord(message *kafka.Message, position []byte) sdk.Record {
	metadata := sdk.Metadata{
		MetadataKafkaTopic: message.Topic,
	}
	metadata.SetCreatedAt(message.Time)

	return sdk.Util.Source.NewRecordCreate(
		position,
		metadata,
		sdk.RawData(message.Key),
		sdk.RawData(message.Value),
	)
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
