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

	"github.com/segmentio/kafka-go"
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
