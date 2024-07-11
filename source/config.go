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

package source

import (
	"context"
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-kafka/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Config struct {
	common.Config

	// Topics is a comma separated list of Kafka topics to read from.
	Topics []string `json:"topics"`
	// Topic {WARN will be deprecated soon} the kafka topic to read from.
	Topic string `json:"topic"`
	// ReadFromBeginning determines from whence the consumer group should begin
	// consuming when it finds a partition without a committed offset. If this
	// options is set to true it will start with the first message in that
	// partition.
	ReadFromBeginning bool `json:"readFromBeginning"`
	// GroupID defines the consumer group id.
	GroupID string `json:"groupID"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c *Config) Validate(ctx context.Context) error {
	var multierr []error
	err := c.Config.Validate()
	if err != nil {
		multierr = append(multierr, err)
	}
	// validate and set the topics.
	if len(c.Topic) == 0 && len(c.Topics) == 0 {
		multierr = append(multierr, fmt.Errorf("required parameter missing: %q", "topics"))
	}
	if len(c.Topic) > 0 && len(c.Topics) > 0 {
		multierr = append(multierr, fmt.Errorf(`can't provide both "topic" and "topics" parameters, "topic" is deprecated and will be removed, use the "topics" parameter instead`))
	}
	if len(c.Topic) > 0 && len(c.Topics) == 0 {
		sdk.Logger(ctx).Warn().Msg(`"topic" parameter is deprecated and will be removed, please use "topics" instead.`)
		// add the topic value to the topics slice.
		c.Topics = make([]string, 1)
		c.Topics[0] = c.Topic
	}
	return errors.Join(multierr...)
}
