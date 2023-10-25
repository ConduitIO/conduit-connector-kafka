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
	"errors"

	"github.com/conduitio/conduit-connector-kafka/common"
)

type Config struct {
	common.Config
	// ReadFromBeginning determines from whence the consumer group should begin
	// consuming when it finds a partition without a committed offset. If this
	// options is set to true it will start with the first message in that
	// partition.
	ReadFromBeginning bool `json:"readFromBeginning"`
	// GroupID defines the consumer group id.
	GroupID string `json:"groupID"`
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
