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

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var ErrSASLInvalidAuth = errors.New("invalid sasl auth, please specify both saslUsername and saslPassword")

type ConfigSASL struct {
	// Mechanism configures the connector to use SASL authentication. If
	// empty, no authentication will be performed.
	Mechanism string `json:"saslMechanism" validate:"inclusion=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512"`
	// Username sets up the username used with SASL authentication.
	Username string `json:"saslUsername"`
	// Password sets up the password used with SASL authentication.
	Password string `json:"saslPassword"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c ConfigSASL) Validate(context.Context) error {
	var multierr []error

	if _, err := c.sasl(); err != nil {
		multierr = append(multierr, err)
	}
	if (c.Username == "") != (c.Password == "") {
		multierr = append(multierr, ErrSASLInvalidAuth)
	}

	return errors.Join(multierr...)
}

// SASL returns the SASL mechanism or nil.
func (c ConfigSASL) SASL() sasl.Mechanism {
	m, _ := c.sasl()
	return m
}

func (c ConfigSASL) sasl() (sasl.Mechanism, error) {
	switch c.Mechanism {
	case "PLAIN":
		return plain.Auth{
			User: c.Username,
			Pass: c.Password,
		}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{
			User: c.Username,
			Pass: c.Password,
		}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{
			User: c.Username,
			Pass: c.Password,
		}.AsSha512Mechanism(), nil
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid SASL mechanism %q", c.Mechanism)
	}
}
