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
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestConfig_Validate(t *testing.T) {
	// Note that we are testing custom validations. Required fields and simple
	// validations are already executed by the SDK via parameter specifications.
	testCases := []struct {
		name    string
		cfg     Config
		wantErr any
	}{{
		name: "empty SASL username",
		cfg: Config{
			ConfigSASL: ConfigSASL{
				Mechanism: "PLAIN",
				Username:  "", // empty
				Password:  "not empty",
			},
		},
		wantErr: ErrSASLInvalidAuth,
	}, {
		name: "empty SASL password",
		cfg: Config{
			ConfigSASL: ConfigSASL{
				Mechanism: "PLAIN",
				Username:  "not empty",
				Password:  "", // empty
			},
		},
		wantErr: ErrSASLInvalidAuth,
	}, {
		name: "invalid Client cert",
		cfg: Config{
			ConfigTLS: ConfigTLS{
				TLSEnabled: true,
				ClientCert: "foo",
			},
		},
		wantErr: "tls: failed to find any PEM data in certificate input",
	}, {
		name: "invalid Client key",
		cfg: Config{
			ConfigTLS: ConfigTLS{
				TLSEnabled: true,
				ClientKey:  "foo",
			},
		},
		wantErr: "tls: failed to find any PEM data in certificate input",
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			err := tc.cfg.Validate(context.Background())
			is.True(err != nil)
			if actualErr, ok := tc.wantErr.(error); ok {
				is.True(errors.Is(err, actualErr))
			} else {
				// workaround for errors that are created on the fly
				errMsg := fmt.Sprint(tc.wantErr)
				is.True(strings.Contains(err.Error(), errMsg))
			}
		})
	}
}

func TestConfig_TryDial(t *testing.T) {
	is := is.New(t)
	t.Parallel()

	cfg := Config{
		Servers: []string{"localhost:12345"}, // Kafka is not running on this port
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := cfg.TryDial(ctx)

	var opErr *net.OpError
	is.True(errors.As(err, &opErr))

	is.Equal(opErr.Op, "dial")
	is.Equal(opErr.Net, "tcp")
}
