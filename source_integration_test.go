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
	"testing"

	"github.com/conduitio/conduit-connector-kafka/config"

	"github.com/conduitio/conduit-connector-kafka/test"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestSource_Integration_RestartFull(t *testing.T) {
	t.Parallel()

	cfgMap := test.SourceConfigMap(t)
	cfg := test.ParseConfigMap[config.Config](t, cfgMap)

	test.Produce(t, cfg, test.GenerateRecords(1, 3))
	pos := testSource_Integration_Read(t, cfgMap, 1, 3, nil, false)

	// produce more records and restart source from fully acked position
	test.Produce(t, cfg, test.GenerateRecords(4, 6))
	testSource_Integration_Read(t, cfgMap, 4, 6, pos, false)
}

func TestSource_Integration_RestartPartial(t *testing.T) {
	t.Parallel()

	cfgMap := test.SourceConfigMap(t)
	cfg := test.ParseConfigMap[config.Config](t, cfgMap)

	test.Produce(t, cfg, test.GenerateRecords(1, 3))
	pos := testSource_Integration_Read(t, cfgMap, 1, 3, nil, true)

	// produce more records and restart source from partially acked position
	test.Produce(t, cfg, test.GenerateRecords(4, 6))
	testSource_Integration_Read(t, cfgMap, 2, 6, pos, false)
}

// testSource_Integration_Read reads and acks messages in range [from,to].
// If ackFirst is true, only the first message will be acknowledged.
// Returns the position of the last message read.
func testSource_Integration_Read(t *testing.T, cfgMap map[string]string, from int, to int, pos sdk.Position, ackFirstOnly bool) sdk.Position {
	is := is.New(t)
	ctx := context.Background()

	underTest := NewSource()
	defer underTest.Teardown(ctx)

	err := underTest.Configure(ctx, cfgMap)
	is.NoErr(err)
	err = underTest.Open(ctx, pos)
	is.NoErr(err)

	want := test.GenerateRecords(from, to)
	var positions []sdk.Position
	for i := from; i <= to; i++ {
		rec, err := underTest.Read(ctx)
		is.NoErr(err)
		is.Equal(want[i-from].Key, rec.Key.Bytes())

		positions = append(positions, rec.Position)
	}

	for i, p := range positions {
		if i > 0 && ackFirstOnly {
			break
		}
		err = underTest.Ack(ctx, p)
		is.NoErr(err)
	}

	return positions[len(positions)-1]
}
