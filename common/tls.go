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
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

type ConfigTLS struct {
	// TLSEnabled defines whether TLS is needed to communicate with the Kafka cluster.
	TLSEnabled bool `json:"tls.enabled"`
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
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c ConfigTLS) Validate(context.Context) error {
	_, err := c.tls()
	return err
}

// TLS returns the TLS config or nil.
func (c ConfigTLS) TLS() *tls.Config {
	t, _ := c.tls()
	return t
}

func (c ConfigTLS) tls() (*tls.Config, error) {
	if !c.TLSEnabled {
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
