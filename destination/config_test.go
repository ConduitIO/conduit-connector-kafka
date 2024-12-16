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
	"strings"
	"testing"

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
