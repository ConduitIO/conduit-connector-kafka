// Copyright © 2024 Meroxa, Inc.
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

package redpanda

import (
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-redpanda/source"
	"github.com/conduitio-labs/conduit-connector-redpanda/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSource_Integration_RestartFull(t *testing.T) {
	t.Parallel()

	cfgMap := test.SourceConfigMap(t, true)
	cfg := test.ParseConfigMap[source.Config](t, cfgMap)

	recs1 := test.GenerateFranzRecords(1, 3)
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs1)
	lastPosition := testSourceIntegrationRead(t, cfgMap, nil, recs1, false)

	// produce more records and restart source from last position
	recs2 := test.GenerateFranzRecords(4, 6)
	test.Produce(t, cfg.Servers, cfg.Topics[1], recs2)
	testSourceIntegrationRead(t, cfgMap, lastPosition, recs2, false)
}

func TestSource_Integration_RestartPartial(t *testing.T) {
	t.Parallel()

	cfgMap := test.SourceConfigMap(t, true)
	cfg := test.ParseConfigMap[source.Config](t, cfgMap)

	recs1 := test.GenerateFranzRecords(1, 3)
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs1)
	lastPosition := testSourceIntegrationRead(t, cfgMap, nil, recs1, true)

	// only first record was acked, produce more records and expect to resume
	// from last acked record
	recs2 := test.GenerateFranzRecords(4, 6)
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs2)

	var wantRecs []*kgo.Record
	wantRecs = append(wantRecs, recs1[1:]...)
	wantRecs = append(wantRecs, recs2...)
	testSourceIntegrationRead(t, cfgMap, lastPosition, wantRecs, false)
}

// testSourceIntegrationRead reads and acks messages in range [from,to].
// If ackFirst is true, only the first message will be acknowledged.
// Returns the position of the last message read.
func testSourceIntegrationRead(
	t *testing.T,
	cfgMap map[string]string,
	startFrom sdk.Position,
	wantRecords []*kgo.Record,
	ackFirstOnly bool,
) sdk.Position {
	is := is.New(t)
	ctx := context.Background()

	underTest := NewSource()
	defer func() {
		err := underTest.Teardown(ctx)
		is.NoErr(err)
	}()

	err := underTest.Configure(ctx, cfgMap)
	is.NoErr(err)
	err = underTest.Open(ctx, startFrom)
	is.NoErr(err)

	var positions []sdk.Position
	for _, wantRecord := range wantRecords {
		rec, err := underTest.Read(ctx)
		is.NoErr(err)
		is.Equal(wantRecord.Key, rec.Key.Bytes())
		collection, err := rec.Metadata.GetCollection()
		is.NoErr(err)
		is.Equal(wantRecord.Topic, collection)

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
