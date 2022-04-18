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
	"context"
	"fmt"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestSource_Restart(t *testing.T) {
	t.Parallel()
	is := is.New(t)

	cfgMap := testConfigMap()
	cfg, err := Parse(cfgMap)
	is.NoErr(err)
	createTopic(t, cfg.Topic)

	sendTestMessages(t, cfg, 1, 3)
	pos := testRead(t, cfgMap, 1, 3, nil, false)
	// Restart
	sendTestMessages(t, cfg, 4, 6)
	testRead(t, cfgMap, 4, 6, pos, false)
}

func TestSource_Ack(t *testing.T) {
	t.Parallel()
	is := is.New(t)

	cfgMap := testConfigMap()
	cfg, err := Parse(cfgMap)
	is.NoErr(err)
	createTopic(t, cfg.Topic)

	sendTestMessages(t, cfg, 1, 3)
	pos := testRead(t, cfgMap, 1, 3, nil, true)
	// Restart
	sendTestMessages(t, cfg, 4, 6)
	testRead(t, cfgMap, 2, 6, pos, false)
}

// testRead reads and acks messages in range [from,to].
// If `ackFirst`, only the first message will be acknowledged.
// Returns the position of the last message read.
func testRead(t *testing.T, cfgMap map[string]string, from int, to int, pos sdk.Position, ackFirstOnly bool) sdk.Position {
	is := is.New(t)
	ctx := context.Background()

	underTest := NewSource()
	defer underTest.Teardown(ctx) //nolint:errcheck // not interested at this point

	err := underTest.Configure(ctx, cfgMap)
	is.NoErr(err)
	err = underTest.Open(ctx, pos)
	is.NoErr(err)

	var positions []sdk.Position
	for i := from; i <= to; i++ {
		rec, err := underTest.Read(ctx)
		is.NoErr(err)
		is.Equal(fmt.Sprintf("test-key-%d", i), string(rec.Key.Bytes()))

		positions = append(positions, rec.Position)
	}

	for i, p := range positions {
		if i > 0 && ackFirstOnly {
			continue
		}
		err = underTest.Ack(ctx, p)
		is.NoErr(err)
	}

	return positions[len(positions)-1]
}
