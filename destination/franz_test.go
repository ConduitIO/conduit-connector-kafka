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

package destination

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-redpanda/common"
	"github.com/conduitio-labs/conduit-connector-redpanda/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

func TestFranzProducer_Opts(t *testing.T) {
	is := is.New(t)

	clientCert, clientKey, caCert := test.Certificates(t)

	cfg := Config{
		Config: common.Config{
			Servers:  []string{"test-host:9092"},
			ClientID: "test-client-id",

			ConfigSASL: common.ConfigSASL{
				Mechanism: "PLAIN",
				Username:  "user",
				Password:  "pass",
			},
			ConfigTLS: common.ConfigTLS{
				ClientCert: clientCert,
				ClientKey:  clientKey,
				CACert:     caCert,
			},
		},
		Topic:           "test-topic",
		BatchBytes:      512,
		DeliveryTimeout: time.Second,
		Acks:            "all",
	}

	p, err := NewFranzProducer(context.Background(), cfg)
	is.NoErr(err)

	is.Equal(p.client.OptValue(kgo.DefaultProduceTopic), cfg.Topic)
	is.Equal(p.client.OptValue(kgo.AllowAutoTopicCreation), true)
	is.Equal(p.client.OptValue(kgo.RecordDeliveryTimeout), cfg.DeliveryTimeout)
	is.Equal(p.client.OptValue(kgo.RequiredAcks), kgo.AllISRAcks())
	is.Equal(p.client.OptValue(kgo.DisableIdempotentWrite), false)
	is.Equal(p.client.OptValue(kgo.ProducerBatchCompression), cfg.CompressionCodecs())
	is.Equal(p.client.OptValue(kgo.ProducerBatchMaxBytes), cfg.BatchBytes)

	is.Equal(p.client.OptValue(kgo.ClientID), cfg.ClientID)
	is.Equal(cmp.Diff(p.client.OptValue(kgo.DialTLSConfig), cfg.TLS(), cmpopts.IgnoreUnexported(tls.Config{})), "")
	is.Equal(p.client.OptValue(kgo.SASL).([]sasl.Mechanism)[0].Name(), cfg.SASL().Name())
}

func TestFranzProducer_Opts_AcksDisableIdempotentWrite(t *testing.T) {
	// minimal valid config
	cfg := Config{
		Config:     common.Config{Servers: []string{"test-host:9092"}},
		Topic:      "foo",
		BatchBytes: 512,
	}

	testCases := []struct {
		acks                       string
		wantRequiredAcks           kgo.Acks
		wantDisableIdempotentWrite bool
	}{{
		acks:                       "all",
		wantRequiredAcks:           kgo.AllISRAcks(),
		wantDisableIdempotentWrite: false,
	}, {
		acks:                       "one",
		wantRequiredAcks:           kgo.LeaderAck(),
		wantDisableIdempotentWrite: true,
	}, {
		acks:                       "none",
		wantRequiredAcks:           kgo.NoAck(),
		wantDisableIdempotentWrite: true,
	}}

	for _, tc := range testCases {
		t.Run(tc.acks, func(t *testing.T) {
			is := is.New(t)
			cfg.Acks = tc.acks
			p, err := NewFranzProducer(context.Background(), cfg)
			is.NoErr(err)

			is.Equal(p.client.OptValue(kgo.RequiredAcks), tc.wantRequiredAcks)
			is.Equal(p.client.OptValue(kgo.DisableIdempotentWrite), tc.wantDisableIdempotentWrite)
		})
	}
}

func TestFranzProducer_Opts_Topic(t *testing.T) {
	// minimal valid config
	cfg := Config{
		Config:     common.Config{Servers: []string{"test-host:9092"}},
		BatchBytes: 512,
	}

	t.Run("static topic", func(t *testing.T) {
		is := is.New(t)
		cfg.Topic = "foo"
		p, err := NewFranzProducer(context.Background(), cfg)
		is.NoErr(err)

		is.Equal(p.client.OptValue(kgo.DefaultProduceTopic), cfg.Topic)
		is.True(p.getTopic == nil)
	})

	t.Run("template topic", func(t *testing.T) {
		is := is.New(t)
		cfg.Topic = `{{ index .Metadata "foo" }}`
		p, err := NewFranzProducer(context.Background(), cfg)
		is.NoErr(err)

		is.Equal(p.client.OptValue(kgo.DefaultProduceTopic), "")
		is.True(p.getTopic != nil)

		topic, err := p.getTopic(opencdc.Record{Metadata: map[string]string{"foo": "bar"}})
		is.NoErr(err)
		is.Equal(topic, "bar")
	})
}
