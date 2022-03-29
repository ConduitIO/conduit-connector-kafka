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
	"testing"

	"github.com/matryer/is"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func TestSegmentReader_TLSOnly(t *testing.T) {
	is := is.New(t)

	caCert := readFile("test/server.cer.pem", t)
	clientKeyPem := readFile("test/client.key.pem", t)
	clientCerPem := readFile("test/client.cer.pem", t)

	config := Config{
		Servers:            []string{"test-host:9092"},
		Topic:              "test-topic",
		CACert:             caCert,
		ClientKey:          clientKeyPem,
		ClientCert:         clientCerPem,
		InsecureSkipVerify: true,
	}
	c := &segmentConsumer{}
	err := c.newReader(config, "group-id")
	is.NoErr(err)
	underTest := c.reader

	tlsConfig := underTest.Config().Dialer.TLS
	is.True(tlsConfig != nil)
	is.True(tlsConfig.InsecureSkipVerify == config.InsecureSkipVerify)
}

func TestSegmentReader_SASL_Plain(t *testing.T) {
	is := is.New(t)
	config := Config{
		Servers:      []string{"test-host:9092"},
		Topic:        "test-topic",
		SASLUsername: "sasl-username",
		SASLPassword: "sasl-password",
	}
	c := &segmentConsumer{}
	err := c.newReader(config, "group-id")
	is.NoErr(err)
	underTest := c.reader

	mechanism := underTest.Config().Dialer.SASLMechanism
	is.True(mechanism != nil)
	plainMechanism, ok := mechanism.(plain.Mechanism)
	is.True(ok)
	is.Equal(config.SASLUsername, plainMechanism.Username)
	is.Equal(config.SASLPassword, plainMechanism.Password)
}

func TestSegmentReader_SASL_SCRAM_SHA_256(t *testing.T) {
	is := is.New(t)
	config := Config{
		Servers:       []string{"test-host:9092"},
		Topic:         "test-topic",
		SASLMechanism: "SCRAM-SHA-256",
		SASLUsername:  "sasl-username",
		SASLPassword:  "sasl-password",
	}
	c := &segmentConsumer{}
	err := c.newReader(config, "group-id")
	is.NoErr(err)
	underTest := c.reader

	mechanism := underTest.Config().Dialer.SASLMechanism
	is.True(mechanism != nil)
	is.Equal("SCRAM-SHA-256", mechanism.Name())
}

func TestSegmentReader_SASL_SCRAM_SHA_512(t *testing.T) {
	is := is.New(t)
	config := Config{
		Servers:       []string{"test-host:9092"},
		Topic:         "test-topic",
		SASLMechanism: "SCRAM-SHA-512",
		SASLUsername:  "sasl-username",
		SASLPassword:  "sasl-password",
	}
	c := &segmentConsumer{}
	err := c.newReader(config, "group-id")
	is.NoErr(err)
	underTest := c.reader

	mechanism := underTest.Config().Dialer.SASLMechanism
	is.True(mechanism != nil)
	is.Equal("SCRAM-SHA-512", mechanism.Name())
}
