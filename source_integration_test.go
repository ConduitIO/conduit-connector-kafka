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
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func TestSource_Restart(t *testing.T) {
	is := is.New(t)

	cfgMap := integrationCfgMap()
	cfg, err := Parse(cfgMap)
	is.NoErr(err)
	createTopic(t, cfg.Topic)

	pos := testRead(t, cfg, cfgMap, 1, 3, nil)
	// Restart
	testRead(t, cfg, cfgMap, 4, 6, pos)
}

func testRead(t *testing.T, cfg Config, cfgMap map[string]string, from int, to int, pos sdk.Position) sdk.Position {
	is := is.New(t)
	ctx := context.Background()
	sendTestMessages(t, cfg, from, to)

	underTest := NewSource()
	defer underTest.Teardown(ctx) //nolint:errcheck // not interested at this point

	err := underTest.Configure(ctx, cfgMap)
	is.NoErr(err)
	err = underTest.Open(ctx, pos)
	is.NoErr(err)

	var lastPos sdk.Position
	for i := from; i <= to; i++ {
		rec, err := underTest.Read(ctx)
		is.NoErr(err)
		is.Equal(fmt.Sprintf("test-key-%d", i), string(rec.Key.Bytes()))

		lastPos = rec.Position
		err = underTest.Ack(ctx, lastPos)
		is.NoErr(err)
	}

	err = underTest.Teardown(ctx)
	is.NoErr(err)
	return lastPos
}

func integrationCfgMap() map[string]string {
	return map[string]string{
		Servers:           "localhost:9092",
		Topic:             "test-topic-" + uuid.NewString(),
		ReadFromBeginning: "true",
	}
}
