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

func newSASLMechanism(mechanismString string, username string, password string) (sasl.Mechanism, error) {
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
