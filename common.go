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
	"fmt"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func newTLSConfig(clientCert, clientKey, caCert string, serverNoVerify bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	// Load client cert
	err := loadClientCert(tlsConfig, clientCert, clientKey)
	if err != nil {
		return nil, fmt.Errorf("couldn't confligure client TLS: %w", err)
	}

	caCertPool, err := buildCertPool(caCert)
	if err != nil {
		return nil, fmt.Errorf("couldn't build root CAs pool: %w", err)
	}
	tlsConfig.RootCAs = caCertPool

	tlsConfig.InsecureSkipVerify = serverNoVerify
	return tlsConfig, err
}

// buildCertPool builds a cert pool with the given CA cert.
// The basis for it is a copy of the system's cert pool, if present.
// Otherwise, a new cert pool is created.
func buildCertPool(caCert string) (*x509.CertPool, error) {
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("couldn't get system cert pool: %w", err)
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	if caCert != "" {
		rootCAs.AppendCertsFromPEM([]byte(caCert))

	}
	return rootCAs, nil
}

func loadClientCert(tlsConfig *tls.Config, clientCert string, clientKey string) error {
	if clientCert == "" && clientKey == "" {
		return nil
	}
	cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
	if err != nil {
		return err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}
	return nil
}

func newSASLMechanism(mechanismString, username, password string) (sasl.Mechanism, error) {
	var mechanism sasl.Mechanism
	switch mechanismString {
	case "", "PLAIN":
		mechanism = plain.Mechanism{
			Username: username,
			Password: password,
		}
	default:
		m, err := newSCRAMMechanism(mechanismString, username, password)
		if err != nil {
			return nil, fmt.Errorf("couldn't configure SASL/SCRAM with %v due to: %w", mechanismString, err)
		}
		mechanism = m
	}
	return mechanism, nil
}

func newSCRAMMechanism(mechanism, username, password string) (sasl.Mechanism, error) {
	var algo scram.Algorithm
	switch mechanism {
	case "SCRAM-SHA-256":
		algo = scram.SHA256
	case "SCRAM-SHA-512":
		algo = scram.SHA512
	default:
		return nil, fmt.Errorf("unknown mechanism %q", mechanism)
	}
	m, err := scram.Mechanism(algo, username, password)
	if err != nil {
		return nil, fmt.Errorf("error creating %v mechanism: %w", mechanism, err)
	}
	return m, nil
}
