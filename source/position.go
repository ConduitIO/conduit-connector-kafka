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
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/goccy/go-json"
)

type Position struct {
	GroupID   string
	Topic     string
	Partition int32
	Offset    int64
}

func ParseSDKPosition(sdkPos opencdc.Position) (Position, error) {
	var p Position
	err := json.Unmarshal(sdkPos, &p)
	if err != nil {
		return p, fmt.Errorf("invalid position: %w", err)
	}
	return p, nil
}

func (p Position) ToSDKPosition() opencdc.Position {
	b, err := json.Marshal(p)
	if err != nil {
		// this error should not be possible
		panic(fmt.Errorf("error marshaling position to JSON: %w", err))
	}
	return b
}
