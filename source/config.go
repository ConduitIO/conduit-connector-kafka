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

	"github.com/conduitio/conduit-connector-kafka/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Config struct {
	sdk.DefaultSourceMiddleware
	common.Config

	// Topics is a comma separated list of Kafka topics to read from.
	Topics []string `json:"topics" validate:"required"`
	// ReadFromBeginning determines from whence the consumer group should begin
	// consuming when it finds a partition without a committed offset. If this
	// options is set to true it will start with the first message in that
	// partition.
	ReadFromBeginning bool `json:"readFromBeginning"`
	// GroupID defines the consumer group id.
	GroupID string `json:"groupID"`
	// RetryGroupJoinErrors determines whether the connector will continually retry on group join errors.
	RetryGroupJoinErrors bool `json:"retryGroupJoinErrors" default:"true"`
}

func (c *Config) Validate(ctx context.Context) error {
	// custom validation can be added here
	return c.DefaultSourceMiddleware.Validate(ctx)
}
