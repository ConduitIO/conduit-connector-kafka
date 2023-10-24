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
	"errors"
	"time"

	"github.com/conduitio/conduit-connector-kafka/config"
)

type Config struct {
	config.Config

	// Acks defines the number of acknowledges from partition replicas required
	// before receiving a response to a produce request.
	// None = fire and forget, one = wait for the leader to acknowledge the
	// writes, all = wait for the full ISR to acknowledge the writes.
	Acks string `json:"acks" default:"all" validate:"inclusion=none|one|all"`
	// DeliveryTimeout for write operation performed by the Writer.
	DeliveryTimeout time.Duration `json:"deliveryTimeout"`
	// Compression set the compression codec to be used to compress messages.
	Compression string `json:"compression" validate:"inclusion=none|gzip|snappy|lz4|zstd"`
	// BatchBytes limits the maximum size of a request in bytes before being
	// sent to a partition.
	BatchBytes int64 `json:"batchBytes" default:"1048576"`

	// isRecordFormatDebezium detects if the connector middleware is configured
	// to produce debezium records.
	isRecordFormatDebezium bool
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Config) Validate() error {
	var multierr error

	err := c.Config.Validate()
	if err != nil {
		multierr = errors.Join(multierr, err)
	}

	return multierr
}
