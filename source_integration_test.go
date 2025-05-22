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
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/source"
	"github.com/conduitio/conduit-connector-kafka/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSource_Integration_RestartFull(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	ctx := context.Background()

	cfgMap := test.SourceConfigMap(t, true, false)
	cfg := test.ParseConfigMap[source.Config](t, cfgMap)

	recs1 := test.GenerateFranzRecords(1, 3)
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs1)
	underTest, lastPosition := testSourceIntegrationRead(t, cfgMap, nil, recs1, false)
	err := underTest.Teardown(ctx)
	is.NoErr(err)

	// produce more records and restart source from last position
	recs2 := test.GenerateFranzRecords(4, 6)
	test.Produce(t, cfg.Servers, cfg.Topics[1], recs2)
	underTest, _ = testSourceIntegrationRead(t, cfgMap, lastPosition, recs2, false)
	err = underTest.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_Integration_RestartPartial(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	ctx := context.Background()

	cfgMap := test.SourceConfigMap(t, true, false)
	cfg := test.ParseConfigMap[source.Config](t, cfgMap)

	recs1 := test.GenerateFranzRecords(1, 3)
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs1)
	underTest, lastPosition := testSourceIntegrationRead(t, cfgMap, nil, recs1, true)
	err := underTest.Teardown(ctx)
	is.NoErr(err)

	// only first record was acked, produce more records and expect to resume
	// from last acked record
	recs2 := test.GenerateFranzRecords(4, 6)
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs2)

	var wantRecs []*kgo.Record
	wantRecs = append(wantRecs, recs1[1:]...)
	wantRecs = append(wantRecs, recs2...)
	testSourceIntegrationRead(t, cfgMap, lastPosition, wantRecs, false)
}

func TestSource_Integration_CommitOnTeardown(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	cfgMap := test.SourceConfigMap(t, false, false)
	cfg := test.ParseConfigMap[source.Config](t, cfgMap)

	recs1 := test.GenerateFranzRecords(1, 3)
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs1)

	// When the source is torn down, all acks should be flushed
	// (i.e., all Kafka messages should be committed)
	underTest, posSDK := testSourceIntegrationRead(t, cfgMap, nil, recs1, false)
	err := underTest.Teardown(ctx)
	is.NoErr(err)

	pos, err := source.ParseSDKPosition(posSDK)
	is.NoErr(err)

	offsets := test.ListCommittedOffsets(t, cfg.Servers, pos.GroupID, cfg.Topics[0])
	// sanity check, our test utils should create topics with only 1 partition
	is.Equal(1, len(offsets))
	is.Equal(int64(3), offsets[0].At)
}

func TestSource_Integration_CommitPeriodically(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	cfgMap := test.SourceConfigMap(t, false, false)
	cfgMap["commitOffsetsDelay"] = "1s"
	cfg := test.ParseConfigMap[source.Config](t, cfgMap)

	recs1 := test.GenerateFranzRecords(1, 3)
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs1)

	// The source should send all collected acks every 5 seconds
	// (i.e., the acked Kafka messages should be committed)
	underTest, posSDK := testSourceIntegrationRead(t, cfgMap, nil, recs1, false)
	defer func() {
		err := underTest.Teardown(ctx)
		is.NoErr(err)
	}()

	pos, err := source.ParseSDKPosition(posSDK)
	is.NoErr(err)

	time.Sleep(1500 * time.Millisecond)

	offsets := test.ListCommittedOffsets(t, cfg.Servers, pos.GroupID, cfg.Topics[0])
	// sanity check, our test utils should create topics with only 1 partition
	is.Equal(1, len(offsets))
	is.Equal(int64(3), offsets[0].At)
}

// testSourceIntegrationRead reads and acks messages in range [from,to].
// If ackFirst is true, only the first message will be acknowledged.
// Returns the position of the last message read.
func testSourceIntegrationRead(
	t *testing.T,
	cfgMap map[string]string,
	startFrom opencdc.Position,
	wantRecords []*kgo.Record,
	ackFirstOnly bool,
) (sdk.Source, opencdc.Position) {
	is := is.New(t)
	ctx := context.Background()

	underTest := NewSource()

	err := sdk.Util.ParseConfig(ctx, cfgMap, underTest.Config(), Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(ctx, startFrom)
	is.NoErr(err)

	var positions []opencdc.Position
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

	return underTest, positions[len(positions)-1]
}
