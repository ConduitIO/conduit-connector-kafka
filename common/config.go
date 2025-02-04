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

package common

import (
	"context"
	"errors"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO make the timeout configurable
var connectionTimeout = time.Second * 10

// Config contains common configuration parameters.
type Config struct {
	// Servers is a list of Kafka bootstrap servers, which will be used to
	// discover all the servers in a cluster.
	Servers []string `json:"servers" validate:"required"`

	// ClientID is a unique identifier for client connections established by
	// this connector.
	ClientID string `json:"clientID" default:"conduit-connector-kafka"`

	ConfigSASL
	ConfigTLS

	franzClientOpts []kgo.Opt
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Config) Validate(ctx context.Context) error {
	var multierr []error

	if err := c.ConfigSASL.Validate(ctx); err != nil {
		multierr = append(multierr, err)
	}
	if err := c.ConfigTLS.Validate(ctx); err != nil {
		multierr = append(multierr, err)
	}

	return errors.Join(multierr...)
}

// TryDial tries to establish a connection to brokers and returns nil if it
// succeeds to connect to at least one broker.
func (c Config) TryDial(ctx context.Context) error {
	opts := c.FranzClientOpts(sdk.Logger(ctx))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	start := time.Now()
	for time.Since(start) < connectionTimeout {
		err = cl.Ping(ctx)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Second):
			sdk.Logger(ctx).Warn().Msg("failed to dial broker, trying again...")
			// ping again
		}
	}
	return err
}
