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
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func TestSegmentWriter_TLSOnly(t *testing.T) {
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
	p := &segmentProducer{}
	err := p.init(config)
	is.NoErr(err)
	underTest := p.writer

	transport, ok := underTest.Transport.(*kafka.Transport)
	is.True(ok)
	is.True(transport.TLS != nil)
	is.True(transport.TLS.InsecureSkipVerify == config.InsecureSkipVerify)
}

func TestSegmentWriter_ClientTLS(t *testing.T) {
	is := is.New(t)

	clientKeyPem := readFile("test/client.key.pem", t)
	clientCerPem := readFile("test/client.cer.pem", t)

	config := Config{
		Servers:            []string{"test-host:9092"},
		Topic:              "test-topic",
		ClientKey:          clientKeyPem,
		ClientCert:         clientCerPem,
		InsecureSkipVerify: true,
	}
	p := &segmentProducer{}
	err := p.init(config)
	is.NoErr(err)
	underTest := p.writer

	transport, ok := underTest.Transport.(*kafka.Transport)
	is.True(ok)
	is.True(transport.TLS != nil)
	is.True(transport.TLS.InsecureSkipVerify == config.InsecureSkipVerify)
}

func TestSegmentWriter_ServerTLS(t *testing.T) {
	is := is.New(t)

	caCert := readFile("test/server.cer.pem", t)

	config := Config{
		Servers:            []string{"test-host:9092"},
		Topic:              "test-topic",
		CACert:             caCert,
		InsecureSkipVerify: true,
	}
	p := &segmentProducer{}
	err := p.init(config)
	is.NoErr(err)
	underTest := p.writer

	transport, ok := underTest.Transport.(*kafka.Transport)
	is.True(ok)
	is.True(transport.TLS != nil)
	is.True(transport.TLS.InsecureSkipVerify == config.InsecureSkipVerify)
}

func TestSegmentWriter_SASL_Plain(t *testing.T) {
	is := is.New(t)
	config := Config{
		Servers:      []string{"test-host:9092"},
		Topic:        "test-topic",
		SASLUsername: "sasl-username",
		SASLPassword: "sasl-password",
	}
	p := &segmentProducer{}
	err := p.init(config)
	is.NoErr(err)
	underTest := p.writer

	transport, ok := underTest.Transport.(*kafka.Transport)
	is.True(ok)

	mechanism := transport.SASL
	is.True(mechanism != nil)

	plainMechanism, ok := mechanism.(plain.Mechanism)
	is.True(ok)
	is.Equal(config.SASLUsername, plainMechanism.Username)
	is.Equal(config.SASLPassword, plainMechanism.Password)
}

func TestSegmentWriter_SASL_SCRAM_SHA_256(t *testing.T) {
	is := is.New(t)
	config := Config{
		Servers:       []string{"test-host:9092"},
		Topic:         "test-topic",
		SASLMechanism: "SCRAM-SHA-256",
		SASLUsername:  "sasl-username",
		SASLPassword:  "sasl-password",
	}
	p := &segmentProducer{}
	err := p.init(config)
	is.NoErr(err)
	underTest := p.writer

	transport, ok := underTest.Transport.(*kafka.Transport)
	is.True(ok)

	mechanism := transport.SASL
	is.True(mechanism != nil)
	is.Equal("SCRAM-SHA-256", mechanism.Name())
}

func TestSegmentWriter_SASL_SCRAM_SHA_512(t *testing.T) {
	is := is.New(t)
	config := Config{
		Servers:       []string{"test-host:9092"},
		Topic:         "test-topic",
		SASLMechanism: "SCRAM-SHA-512",
		SASLUsername:  "sasl-username",
		SASLPassword:  "sasl-password",
	}
	p := &segmentProducer{}
	err := p.init(config)
	is.NoErr(err)
	underTest := p.writer

	transport, ok := underTest.Transport.(*kafka.Transport)
	is.True(ok)

	mechanism := transport.SASL
	is.True(mechanism != nil)
	is.Equal("SCRAM-SHA-512", mechanism.Name())
}
