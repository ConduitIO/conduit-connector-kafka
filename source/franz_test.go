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

package source

import (
	"context"
	"crypto/tls"
	"regexp"
	"testing"

	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

func TestFranzConsumer_Opts(t *testing.T) {
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
		Topics:  []string{"test-topic"},
		GroupID: "test-group-id",
	}

	c, err := NewFranzConsumer(context.Background(), cfg)
	is.NoErr(err)

	is.Equal(c.client.OptValue(kgo.ConsumeTopics), map[string]*regexp.Regexp{cfg.Topics[0]: nil})
	is.Equal(c.client.OptValue(kgo.ConsumerGroup), cfg.GroupID)

	is.Equal(c.client.OptValue(kgo.ClientID), cfg.ClientID)
	is.Equal(cmp.Diff(c.client.OptValue(kgo.DialTLSConfig), cfg.TLS(), cmpopts.IgnoreUnexported(tls.Config{})), "")
	is.Equal(c.client.OptValue(kgo.SASL).([]sasl.Mechanism)[0].Name(), cfg.SASL().Name())
}
