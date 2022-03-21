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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func newTLSDialer(cfg Config) (*kafka.Dialer, error) {
	tlsCfg, err := newTLSConfig(cfg.ClientCert, cfg.ClientKey, cfg.CACert, cfg.InsecureSkipVerify)
	if err != nil {
		return nil, fmt.Errorf("invalid TLS config: %w", err)
	}
	return &kafka.Dialer{
		ClientID:  "",
		DualStack: true,
		TLS:       tlsCfg,
	}, nil
}

func newTLSConfig(clientCert, clientKey, caCert string, serverNoVerify bool) (*tls.Config, error) {
	tlsConfig := tls.Config{MinVersion: tls.VersionTLS12}

	// Load client cert
	cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
	if err != nil {
		return nil, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caCert))
	tlsConfig.RootCAs = caCertPool

	tlsConfig.InsecureSkipVerify = serverNoVerify
	return &tlsConfig, err
}

func dialerWithSASL(dialer *kafka.Dialer, cfg Config) error {
	if dialer == nil {
		return errors.New("dialer cannot be nil")
	}
	if !cfg.saslEnabled() {
		return errors.New("input config has no SASL parameters")
	}
	dialer.SASLMechanism = getSASLMechanism(cfg)
	return nil
}

func getSASLMechanism(cfg Config) plain.Mechanism {
	return plain.Mechanism{
		Username: cfg.SASLUsername,
		Password: cfg.SASLPassword,
	}
}

func transportWithSASL(transport *kafka.Transport, cfg Config) error {
	if transport == nil {
		return errors.New("transport cannot be nil")
	}
	if !cfg.saslEnabled() {
		return errors.New("input config has no SASL parameters")
	}
	transport.SASL = getSASLMechanism(cfg)
	return nil
}
