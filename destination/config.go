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

//go:generate paramgen -output=paramgen.go Config

package destination

import (
	"time"

	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	common.Config
	
	// Topic is the Kafka topic.
	Topic string `json:"topic" validate:"required"`
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
func (c Config) Validate() error {
	return c.Config.Validate()
}
