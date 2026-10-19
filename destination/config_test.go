// Copyright © 2024 Meroxa, Inc.
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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestConfig_Validate(t *testing.T) {
	testCases := []struct {
		name    string
		config  Config
		wantErr string
	}{{
		name: "valid",
		config: Config{
			Topic: strings.Repeat("a", 249),
		},
	}, {
		name: "topic too long",
		config: Config{
			Topic: strings.Repeat("a", 250),
		},
		wantErr: "topic is too long, maximum length is 249",
	}, {
		name: "invalid topic characters",
		config: Config{
			Topic: "foo?",
		},
		wantErr: "topic is neither a valid static Kafka topic nor a valid Go template",
	}, {
		name: "invalid Go template 1",
		config: Config{
			Topic: "}} foo {{",
		},
		wantErr: "topic is neither a valid static Kafka topic nor a valid Go template",
	}, {
		name: "invalid Go template 2",
		config: Config{
			Topic: "}}foo",
		},
		wantErr: "topic is neither a valid static Kafka topic nor a valid Go template",
	}, {
		name: "invalid Go template 3",
		config: Config{
			Topic: "{{ foo }}",
		},
		wantErr: "topic is neither a valid static Kafka topic nor a valid Go template",
	}, {
		name: "valid Go template",
		config: Config{
			Topic: "{{ .Metadata.foo }}",
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			err := tc.config.Validate(context.Background())

			if tc.wantErr != "" && err == nil {
				t.Errorf("expected error, got nil")
			}
			if tc.wantErr == "" && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err != nil {
				is.True(strings.Contains(err.Error(), tc.wantErr))
			}
		})
	}
}

func TestConfig_ParseTopic_DoesErrorOnTopicNotFound(t *testing.T) {
	is := is.New(t)
	template := `{{ index .Metadata "topiccc" }}`

	cfg := Config{Topic: template}
	topic, getTopic, err := cfg.ParseTopic()
	is.NoErr(err)

	is.Equal(topic, "")

	rec := opencdc.Record{
		Key: opencdc.RawData("testkey"),
		Metadata: map[string]string{
			"topic": "testtopic",
		},
	}
	topic, err = getTopic(rec)
	is.True(err != nil)                                                 // expected error on topic not found
	is.True(strings.Contains(err.Error(), "topic not found on record")) // expected topic not found error

	is.Equal(topic, "")
}

func TestConfig_ParseTopic_SimpleMetadataIndexFastPath(t *testing.T) {
	// The default topic template must resolve through the fast path to the same
	// value the text/template execution would have produced, and must fail the
	// same way when the metadata key is missing.
	is := is.New(t)

	cfg := Config{Topic: `{{ index .Metadata "opencdc.collection" }}`}
	topic, getTopic, err := cfg.ParseTopic()
	is.NoErr(err)
	is.Equal(topic, "")

	rec := opencdc.Record{
		Metadata: map[string]string{"opencdc.collection": "testtopic"},
	}
	topic, err = getTopic(rec)
	is.NoErr(err)
	is.Equal(topic, "testtopic")

	// A missing key must error the same way as the template path.
	_, err = getTopic(opencdc.Record{Metadata: map[string]string{}})
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "topic not found on record"))
}

func TestSimpleMetadataIndex(t *testing.T) {
	testCases := []struct {
		name string
		src  string
		key  string
		ok   bool
	}{
		{name: "default collection", src: `{{ index .Metadata "opencdc.collection" }}`, key: "opencdc.collection", ok: true},
		{name: "no spaces", src: `{{index .Metadata "x"}}`, key: "x", ok: true},
		{name: "dollar root", src: `{{ index $.Metadata "x" }}`, key: "x", ok: true},
		{name: "escaped key", src: `{{ index .Metadata "a\nb" }}`, key: "a\nb", ok: true},
		{name: "field access is not an index", src: `{{ .Metadata.foo }}`, ok: false},
		{name: "surrounding text", src: `topic-{{ index .Metadata "x" }}`, ok: false},
		{name: "two actions", src: `{{ index .Metadata "a" }}-{{ index .Metadata "b" }}`, ok: false},
		{name: "missing key arg", src: `{{ index .Metadata }}`, ok: false},
		{name: "non-metadata object", src: `{{ index .Payload "x" }}`, ok: false},
		{name: "non-constant key", src: `{{ index .Metadata (print "x") }}`, ok: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := simpleMetadataIndex(tc.src)
			if ok != tc.ok || (ok && key != tc.key) {
				t.Fatalf("simpleMetadataIndex(%q) = (%q, %v), want (%q, %v)", tc.src, key, ok, tc.key, tc.ok)
			}
		})
	}
}

func TestConfig_ValidateMirror(t *testing.T) {
	base := func() Config {
		return Config{
			Topic:          "topic",
			Acks:           "all",
			ProduceTimeout: 2 * time.Minute,
		}
	}
	withFormat := func(f string) *string { return &f }

	testCases := []struct {
		name    string
		mutate  func(*Config)
		wantErr error
	}{
		{"valid", func(*Config) {}, nil},
		{
			"acks not all",
			func(c *Config) { c.Acks = "one" },
			ErrCodeMirrorAcks,
		},
		{
			"deliveryTimeout set",
			func(c *Config) { c.DeliveryTimeout = time.Second },
			ErrCodeDeliveryTimeoutWithTS,
		},
		{
			"record format set",
			func(c *Config) { c.RecordFormat = withFormat("template") },
			ErrCodeMirrorFormatSet,
		},
		{
			"record format options set",
			func(c *Config) { c.RecordFormatOptions = "{{ .Payload.After }}" },
			ErrCodeMirrorFormatSet,
		},
		{
			"produceTimeout below minimum",
			func(c *Config) { c.ProduceTimeout = 29 * time.Second },
			ErrCodeProduceTimeoutOutOfRange,
		},
		{
			"produceTimeout zero",
			func(c *Config) { c.ProduceTimeout = 0 },
			ErrCodeProduceTimeoutOutOfRange,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			cfg := base()
			cfg.Mirror = true
			tc.mutate(&cfg)

			err := cfg.Validate(context.Background())
			if tc.wantErr == nil {
				is.NoErr(err)
				return
			}
			is.True(err != nil)
			is.True(errors.Is(err, tc.wantErr))
		})
	}
}

func TestConfig_Validate_AllowLossyHeadersRequiresMirror(t *testing.T) {
	is := is.New(t)
	cfg := Config{Topic: "topic", AllowLossyHeaders: true}
	err := cfg.Validate(context.Background())
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "allowLossyHeaders requires mirror=true"))
}
