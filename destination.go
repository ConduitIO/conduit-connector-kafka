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
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		Servers: {
			Default:     "",
			Validations: []sdk.Validation{sdk.ValidationRequired{}},
			Description: "A list of bootstrap servers to which the plugin will connect.",
		},
		Topic: {
			Default:     "",
			Validations: []sdk.Validation{sdk.ValidationRequired{}},
			Description: "The topic to which records will be written to.",
		},
		Acks: {
			Default:     "all",
			Description: "The number of acknowledgments required before considering a record written to Kafka. Valid values: none, one, all.",
		},
		DeliveryTimeout: {
			Default:     "10s",
			Description: "Message delivery timeout.",
		},
		ClientID: {
			Default:     "",
			Description: "A Kafka client ID.",
		},
		ClientCert: {
			Default:     "",
			Description: "A certificate for the Kafka client, in PEM format. If provided, the private key needs to be provided too.",
		},
		ClientKey: {
			Default:     "",
			Description: "A private key for the Kafka client, in PEM format. If provided, the certificate needs to be provided too.",
		},
		CACert: {
			Default:     "",
			Description: "The Kafka broker's certificate, in PEM format.",
		},
		InsecureSkipVerify: {
			Default: "false",
			Description: "Controls whether a client verifies the server's certificate chain and host name. " +
				"If `true`, accepts any certificate presented by the server and any host name in that certificate.",
		},
		SASLMechanism: {
			Default:     "",
			Description: "SASL mechanism to be used. Possible values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.",
		},
		SASLUsername: {
			Default:     "",
			Description: "SASL username. required if saslMechanism is provided.",
		},
		SASLPassword: {
			Default:     "",
			Description: "SASL password. required if saslMechanism is provided.",
		},
		Compression: {
			Default:     "",
			Description: "Compression applied to messages. Possible values: gzip, snappy, lz4, zstd.",
		},
		BatchBytes: {
			Default:     "1048576",
			Description: "The maximum size of a request in bytes before being sent to a partition.",
		},
	}
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Kafka Destination...")
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

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	return d.Producer.Send(ctx, records)
}

// Teardown shuts down the Kafka client.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down Kafka Destination...")
	if d.Producer != nil {
		err := d.Producer.Close()
		if err != nil {
			return fmt.Errorf("failed closing Kafka producer: %w", err)
		}
	}
	return nil
}
