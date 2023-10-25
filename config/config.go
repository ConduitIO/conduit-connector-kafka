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

package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	// TODO make the timeout configurable
	connectionTimeout = time.Second * 10
)

// Config contains common configuration parameters.
type Config struct {
	// Servers is a list of Kafka bootstrap servers, which will be used to
	// discover all the servers in a cluster.
	Servers []string `json:"servers" validate:"required"`
	// Topic is the Kafka topic.
	Topic string `json:"topic" validate:"required"`

	// ClientID is a unique identifier for client connections established by
	// this connector.
	ClientID string `json:"clientID" default:"conduit-connector-kafka"`

	// -- TLS --

	// ClientCert is the Kafka client's certificate.
	ClientCert string `json:"clientCert"`
	// ClientKey is the Kafka client's private key.
	ClientKey string `json:"clientKey"`
	// CACert is the Kafka broker's certificate.
	CACert string `json:"caCert"`
	// InsecureSkipVerify defines whether to validate the broker's certificate
	// chain and host name. If 'true', accepts any certificate presented by the
	// server and any host name in that certificate.
	InsecureSkipVerify bool `json:"insecureSkipVerify"`

	// -- SASL --

	// SASLMechanism configures the connector to use SASL authentication. If
	// empty, no authentication will be performed.
	SASLMechanism string `json:"saslMechanism" validate:"inclusion=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512"`
	// SASLUsername sets up the username used with SASL authentication.
	SASLUsername string `json:"saslUsername"`
	// SASLPassword sets up the password used with SASL authentication.
	SASLPassword string `json:"saslPassword"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Config) Validate() error {
	var multierr []error

	if _, err := c.sasl(); err != nil {
		multierr = append(multierr, err)
	}
	if _, err := c.tls(); err != nil {
		multierr = append(multierr, err)
	}

	return errors.Join(multierr...)
}

// TryDial tries to establish a connection to brokers and returns nil if it
// succeeds to connect to at least one broker.
func (c Config) TryDial(ctx context.Context) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.Servers...))
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
			// ping again
		}
	}
	return err
}

// SASL returns the SASL mechanism or nil.
func (c Config) SASL() sasl.Mechanism {
	m, _ := c.sasl()
	return m
}

func (c Config) sasl() (sasl.Mechanism, error) {
	switch c.SASLMechanism {
	case "PLAIN":
		return plain.Auth{
			User: c.SASLUsername,
			Pass: c.SASLPassword,
		}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{
			User: c.SASLUsername,
			Pass: c.SASLPassword,
		}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{
			User: c.SASLUsername,
			Pass: c.SASLPassword,
		}.AsSha512Mechanism(), nil
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid SASL mechanism %q", c.SASLMechanism)
	}
}

// TLS returns the TLS config or nil.
func (c Config) TLS() *tls.Config {
	t, _ := c.tls()
	return t
}

func (c Config) tls() (*tls.Config, error) {
	if c.ClientCert == "" && c.CACert == "" && c.ClientKey == "" {
		return nil, nil
	}

	var certificates []tls.Certificate
	if c.ClientCert != "" || c.ClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(c.ClientCert), []byte(c.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("could not configure client TLS: %w", err)
		}
		certificates = []tls.Certificate{cert}
	}

	var rootCAs *x509.CertPool
	if c.CACert != "" {
		rootCAs, _ = x509.SystemCertPool()
		if rootCAs == nil {
			// fall back to an empty cert pool
			rootCAs = x509.NewCertPool()
		}
		rootCAs.AppendCertsFromPEM([]byte(c.CACert))
	}

	return &tls.Config{
		Certificates:       certificates,
		RootCAs:            rootCAs,
		InsecureSkipVerify: c.InsecureSkipVerify, //nolint:gosec // it's the users decision to turn this on
	}, nil
}
