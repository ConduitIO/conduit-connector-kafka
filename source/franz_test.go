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
	"os"
	"testing"

	"github.com/conduitio/conduit-connector-kafka/config"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

func TestFranzConsumer_Opts(t *testing.T) {
	is := is.New(t)

	certs := readFiles(t,
		"../test/client.cer.pem", // ClientCert
		"../test/client.key.pem", // ClientKey
		"../test/server.cer.pem", // CACert
	)

	cfg := Config{
		Config: config.Config{
			Servers:  []string{"test-host:9092"},
			Topic:    "test-topic",
			ClientID: "test-client-id",

			ClientCert: string(certs[0]),
			ClientKey:  string(certs[1]),
			CACert:     string(certs[2]),

			SASLMechanism: "PLAIN",
			SASLUsername:  "user",
			SASLPassword:  "pass",
		},
		GroupID: "test-group-id",
	}

	c, err := NewFranzConsumer(context.Background(), cfg)
	is.NoErr(err)

	is.Equal(c.client.OptValue(kgo.ConsumerGroup), cfg.GroupID)
	is.Equal(c.client.OptValue(kgo.ClientID), cfg.ClientID)
	is.Equal(cmp.Diff(c.client.OptValue(kgo.DialTLSConfig), cfg.TLS(), cmpopts.IgnoreUnexported(tls.Config{})), "")
	is.Equal(c.client.OptValue(kgo.SASL).([]sasl.Mechanism)[0].Name(), cfg.SASL().Name())
}

func readFiles(t *testing.T, paths ...string) [][]byte {
	files := make([][]byte, len(paths))
	for i, path := range paths {
		bytes, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = bytes
	}
	return files
}
