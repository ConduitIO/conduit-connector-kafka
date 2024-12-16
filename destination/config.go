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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	topicRegex     = regexp.MustCompile(`^[a-zA-Z0-9._\-]+$`)
	maxTopicLength = 249
)

type Config struct {
	sdk.DefaultDestinationMiddleware
	common.Config

	// Topic is the Kafka topic. It can contain a [Go template](https://pkg.go.dev/text/template)
	// that will be executed for each record to determine the topic. By default,
	// the topic is the value of the `opencdc.collection` metadata field.
	Topic string `json:"topic" default:"{{ index .Metadata \"opencdc.collection\" }}"`
	// Acks defines the number of acknowledges from partition replicas required
	// before receiving a response to a produce request.
	// None = fire and forget, one = wait for the leader to acknowledge the
	// writes, all = wait for the full ISR to acknowledge the writes.
	Acks string `json:"acks" default:"all" validate:"inclusion=none|one|all"`
	// DeliveryTimeout for write operation performed by the Writer.
	DeliveryTimeout time.Duration `json:"deliveryTimeout"`
	// Compression set the compression codec to be used to compress messages.
	Compression string `json:"compression" default:"snappy" validate:"inclusion=none|gzip|snappy|lz4|zstd"`
	// BatchBytes limits the maximum size of a request in bytes before being
	// sent to a partition. This mirrors Kafka's max.message.bytes.
	BatchBytes int32 `json:"batchBytes" default:"1000012"`

	// useKafkaConnectKeyFormat defines if the produced key in a kafka message
	// should be in the kafka connect format (i.e. JSON with schema).
	useKafkaConnectKeyFormat bool
}

type TopicFn func(opencdc.Record) (string, error)

func (c Config) WithKafkaConnectKeyFormat() Config {
	c.useKafkaConnectKeyFormat = true
	return c
}

func (c Config) RequiredAcks() kgo.Acks {
	switch c.Acks {
	case "none":
		return kgo.NoAck()
	case "one":
		return kgo.LeaderAck()
	case "all":
		return kgo.AllISRAcks()
	default:
		// it shouldn't be possible to get here because of the config validation
		return kgo.AllISRAcks()
	}
}

func (c Config) CompressionCodecs() []kgo.CompressionCodec {
	switch c.Compression {
	case "none":
		return []kgo.CompressionCodec{kgo.NoCompression()}
	case "gzip":
		return []kgo.CompressionCodec{kgo.GzipCompression(), kgo.NoCompression()}
	case "snappy":
		return []kgo.CompressionCodec{kgo.SnappyCompression(), kgo.NoCompression()}
	case "lz4":
		return []kgo.CompressionCodec{kgo.Lz4Compression(), kgo.NoCompression()}
	case "zstd":
		return []kgo.CompressionCodec{kgo.ZstdCompression(), kgo.NoCompression()}
	default:
		// it shouldn't be possible to get here because of the config validation
		return []kgo.CompressionCodec{kgo.SnappyCompression(), kgo.NoCompression()}
	}
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Config) Validate(ctx context.Context) error {
	var multierr []error

	err := c.Config.Validate(ctx)
	if err != nil {
		multierr = append(multierr, err)
	}

	_, _, err = c.ParseTopic()
	if err != nil {
		multierr = append(multierr, err)
	}

	return errors.Join(multierr...)
}

// ParseTopic returns either a static topic or a function that determines the
// topic for each record individually. If the topic is neither static nor a
// template, an error is returned.
func (c Config) ParseTopic() (topic string, f TopicFn, err error) {
	if topicRegex.MatchString(c.Topic) {
		// The topic is static, check length.
		if len(c.Topic) > maxTopicLength {
			return "", nil, fmt.Errorf("topic is too long, maximum length is %d", maxTopicLength)
		}
		return c.Topic, nil, nil
	}

	// The topic must be a template, check if it contains at least one action {{ }},
	// to prevent allowing invalid static topic names.
	if !strings.Contains(c.Topic, "{{") || !strings.Contains(c.Topic, "}}") {
		return "", nil, fmt.Errorf("topic is neither a valid static Kafka topic nor a valid Go template")
	}

	// Try to parse the topic
	t, err := template.New("topic").Funcs(sprig.FuncMap()).Parse(c.Topic)
	if err != nil {
		// The topic is not a valid Go template.
		return "", nil, fmt.Errorf("topic is neither a valid static Kafka topic nor a valid Go template: %w", err)
	}

	// The topic is a valid template, return TopicFn.
	var sb strings.Builder
	return "", func(r opencdc.Record) (string, error) {
		sb.Reset()
		if err := t.Execute(&sb, r); err != nil {
			return "", fmt.Errorf("failed to execute topic template: %w", err)
		}
		topic := sb.String()
		if topic == "" {
			return "", fmt.Errorf(
				"topic not found on record %s using template %s",
				string(r.Key.Bytes()), c.Topic,
			)
		}

		return topic, nil
	}, nil
}
