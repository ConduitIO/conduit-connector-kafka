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
	"strings"
	"testing"

	"github.com/matryer/is"
	"github.com/segmentio/kafka-go"
)

func TestParse_Nil(t *testing.T) {
	is := is.New(t)
	parsed, err := Parse(nil)
	is.Equal(Config{}, parsed)
	is.True(err != nil)
}

func TestParse_Empty(t *testing.T) {
	is := is.New(t)
	parsed, err := Parse(make(map[string]string))
	is.Equal(Config{}, parsed)
	is.True(err != nil)
}

func TestParse_ServersMissing(t *testing.T) {
	is := is.New(t)
	parsed, err := Parse(map[string]string{"something-irrelevant": "even less relevant"})
	is.Equal(Config{}, parsed)
	is.True(err != nil)
}

func TestNewProducer_InvalidServers(t *testing.T) {
	is := is.New(t)
	testCases := []struct {
		name   string
		config map[string]string
		exp    string
	}{
		{
			name: "empty server string in the middle",
			config: map[string]string{
				Servers: "host1:1111,,host2:2222",
				Topic:   "topic",
			},
			exp: "invalid servers: empty 1. server",
		},
		{
			name: "single blank server string",
			config: map[string]string{
				Servers: "     ",
				Topic:   "topic",
			},
			exp: "invalid servers: empty 0. server",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := Parse(tc.config)
			is.Equal(Config{}, parsed)
			is.True(err != nil)
			is.Equal(tc.exp, err.Error())
		})
	}
}

func TestParse_OneMissing_OnePresent(t *testing.T) {
	is := is.New(t)
	parsed, err := Parse(map[string]string{
		Servers: "localhost:9092",
	})
	is.Equal(Config{}, parsed)
	is.True(err != nil)
}

func TestParse_FullRequired(t *testing.T) {
	is := is.New(t)
	parsed, err := Parse(map[string]string{
		Servers: "localhost:9092",
		Topic:   "hello-world-topic",
	})

	is.NoErr(err)
	is.Equal([]string{"localhost:9092"}, parsed.Servers)
	is.Equal("hello-world-topic", parsed.Topic)
}

func TestParse_TLSConfig(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name     string
		cfg      map[string]string
		assertFn func(t *testing.T, config Config, err error)
	}{
		{
			name: "Possible to provider server certificate only",
			cfg: map[string]string{
				Servers: "localhost:9092",
				Topic:   "hello-world-topic",
				CACert:  "CACert",
			},
			assertFn: func(t *testing.T, config Config, err error) {
				is.NoErr(err)
				is.Equal("CACert", config.CACert)
			},
		},
		{
			name: "Possible to configure client TLS only",
			cfg: map[string]string{
				Servers:    "localhost:9092",
				Topic:      "hello-world-topic",
				ClientCert: "ClientCert",
				ClientKey:  "ClientKey",
			},
			assertFn: func(t *testing.T, config Config, err error) {
				is.NoErr(err)
				is.Equal("ClientCert", config.ClientCert)
				is.Equal("ClientKey", config.ClientKey)
			},
		},
		{
			name: "Client certificate provided, client key missing",
			cfg: map[string]string{
				Servers:    "localhost:9092",
				Topic:      "hello-world-topic",
				ClientCert: "ClientCert",
			},
			assertFn: func(t *testing.T, config Config, err error) {
				is.True(err != nil)
			},
		},
		{
			name: "Client certificate missing, client key provided",
			cfg: map[string]string{
				Servers:   "localhost:9092",
				Topic:     "hello-world-topic",
				ClientKey: "ClientKey",
			},
			assertFn: func(t *testing.T, config Config, err error) {
				is.True(err != nil)
			},
		},
		{
			name: "InsecureSkipVerify is false by default",
			cfg: map[string]string{
				Servers:    "localhost:9092",
				Topic:      "hello-world-topic",
				ClientCert: "ClientCert",
				ClientKey:  "ClientKey",
				CACert:     "CACert",
			},
			assertFn: func(t *testing.T, config Config, err error) {
				is.NoErr(err)
				is.Equal(false, config.InsecureSkipVerify)
			},
		},
		{
			name: "InsecureSkipVerify can be set to true",
			cfg: map[string]string{
				Servers:            "localhost:9092",
				Topic:              "hello-world-topic",
				ClientCert:         "ClientCert",
				ClientKey:          "ClientKey",
				CACert:             "CACert",
				InsecureSkipVerify: "true",
			},
			assertFn: func(t *testing.T, config Config, err error) {
				is.NoErr(err)
				is.Equal(true, config.InsecureSkipVerify)
			},
		},
		{
			name: "invalid value for InsecureSkipVerify ",
			cfg: map[string]string{
				Servers:            "localhost:9092",
				Topic:              "hello-world-topic",
				ClientCert:         "ClientCert",
				ClientKey:          "ClientKey",
				CACert:             "CACert",
				InsecureSkipVerify: "     false",
			},
			assertFn: func(t *testing.T, config Config, err error) {
				is.True(err != nil)
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := Parse(tc.cfg)
			tc.assertFn(t, parsed, err)
		})
	}
}

func TestParse_InvalidDeliveryTimeout(t *testing.T) {
	is := is.New(t)
	parsed, err := Parse(map[string]string{
		Servers:         "localhost:9092",
		Topic:           "hello-world-topic",
		DeliveryTimeout: "nope, no integer here",
	})
	is.True(err != nil)
	is.Equal(
		`invalid delivery timeout: duration cannot be parsed: time: invalid duration "nope, no integer here"`,
		err.Error(),
	)
	is.Equal(Config{}, parsed)
}

func TestParse_ZeroDeliveryTimeout(t *testing.T) {
	is := is.New(t)
	parsed, err := Parse(map[string]string{
		Servers:         "localhost:9092",
		Topic:           "hello-world-topic",
		DeliveryTimeout: "0ms",
	})
	is.True(err != nil)
	is.True(
		strings.HasPrefix(err.Error(), "invalid delivery timeout: has to be > 0ms"),
	)
	is.Equal(Config{}, parsed)
}

func TestParse_Full(t *testing.T) {
	is := is.New(t)
	parsed, err := Parse(map[string]string{
		Servers:           "localhost:9092",
		Topic:             "hello-world-topic",
		SecurityProtocol:  "SASL_SSL",
		Acks:              "all",
		DeliveryTimeout:   "1s2ms",
		ReadFromBeginning: "true",
		ClientCert:        "ClientCert",
		ClientKey:         "ClientKey",
		CACert:            "CACert",
	})

	is.NoErr(err)
	is.Equal([]string{"localhost:9092"}, parsed.Servers)
	is.Equal("hello-world-topic", parsed.Topic)
	is.Equal(kafka.RequireAll, parsed.Acks)
	is.Equal(int64(1002), parsed.DeliveryTimeout.Milliseconds())
	is.Equal(true, parsed.ReadFromBeginning)
	is.Equal("ClientCert", parsed.ClientCert)
	is.Equal("ClientKey", parsed.ClientKey)
	is.Equal("CACert", parsed.CACert)
}

func TestParse_Ack(t *testing.T) {
	is := is.New(t)
	testCases := []struct {
		name     string
		ackInput string
		ackExp   kafka.RequiredAcks
		err      string
	}{
		{
			name:     "default returned",
			ackInput: "",
			ackExp:   kafka.RequireAll,
		},
		{
			name:     "parse none",
			ackInput: "none",
			ackExp:   kafka.RequireNone,
		},
		{
			name:     "parse 0",
			ackInput: "0",
			ackExp:   kafka.RequireNone,
		},
		{
			name:     "parse one",
			ackInput: "one",
			ackExp:   kafka.RequireOne,
		},
		{
			name:     "parse 1",
			ackInput: "1",
			ackExp:   kafka.RequireOne,
		},
		{
			name:     "all",
			ackInput: "all",
			ackExp:   kafka.RequireAll,
		},
		{
			name:     "invalid",
			ackInput: "qwerty",
			err:      `couldn't parse ack: unknown ack mode: required acks must be one of none, one, or all, not "qwerty"`,
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := Parse(map[string]string{
				Servers: "localhost:9092",
				Topic:   "hello-world-topic",
				Acks:    tc.ackInput,
			})
			if tc.err != "" {
				is.True(err != nil)
				// todo without string comparisons
				is.Equal(tc.err, err.Error())
			} else {
				is.NoErr(err)
				is.Equal(tc.ackExp, parsed.Acks)
			}
		})
	}
}
