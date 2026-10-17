// Copyright © 2026 Meroxa, Inc.
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
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/destination"
	"github.com/conduitio/conduit-connector-kafka/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestMirror_Integration_PreservesRecord copies records across a Kafka-to-Kafka
// mirror and checks that each record lands in its source partition with its
// key, value, headers and timestamp byte for byte. It runs on both Kafka and
// Redpanda (see the Makefile).
func TestMirror_Integration_PreservesRecord(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	const partitions = 3
	srcTopic := "mirror-src-" + uuid.NewString()
	dstTopic := "mirror-dst-" + uuid.NewString()
	createTopicWithPartitions(t, srcTopic, partitions)
	createTopicWithPartitions(t, dstTopic, partitions)

	servers := test.ServerList()

	// Records with explicit partitions, binary headers, repeated keys, and a
	// fixed timestamp.
	fixed := time.UnixMilli(1760000000000)
	records := []*kgo.Record{
		{
			Topic:     srcTopic,
			Partition: 0,
			Key:       []byte("key-0"),
			Value:     []byte("value-0"),
			Timestamp: fixed,
			Headers: []kgo.RecordHeader{
				{Key: "a", Value: []byte("b")},
				{Key: "bin", Value: []byte{0xff, 0xfe, 0x00}},
			},
		},
		{
			Topic:     srcTopic,
			Partition: 1,
			Key:       []byte("key-1"),
			Value:     []byte{},
			Timestamp: fixed.Add(time.Millisecond),
			Headers: []kgo.RecordHeader{
				{Key: "n", Value: nil},
				{Key: "r", Value: []byte("1")},
				{Key: "r", Value: []byte("2")}, // repeated key stays separate
			},
		},
		{
			Topic:     srcTopic,
			Partition: 2,
			Key:       []byte{},
			Value:     []byte("value-2"),
			Timestamp: fixed.Add(2 * time.Millisecond),
			Headers:   nil,
		},
	}
	produceToPartitions(t, servers, records)

	// Source: emit mirror metadata; destination: mirror mode.
	srcCfg := map[string]string{
		"servers":            test.Servers(),
		"topics":             srcTopic,
		"readFromBeginning":  "true",
		"emitMirrorMetadata": "true",
	}
	dstCfg := map[string]string{
		"servers": test.Servers(),
		"topic":   dstTopic,
		"mirror":  "true",
		"acks":    "all",
	}

	src := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, srcCfg, src.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(src.Open(ctx, nil))
	defer func() { is.NoErr(src.Teardown(ctx)) }()

	dst := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(ctx, dstCfg, dst.Config(), Connector.NewSpecification().DestinationParams))
	is.NoErr(dst.Open(ctx))
	defer func() { is.NoErr(dst.Teardown(ctx)) }()

	for range records {
		rec, err := src.Read(ctx)
		is.NoErr(err)
		n, err := dst.Write(ctx, []opencdc.Record{rec})
		is.NoErr(err)
		is.Equal(n, 1)
		is.NoErr(src.Ack(ctx, rec.Position))
	}

	// Read the destination topic and compare record by record on key.
	got := consumeAll(t, servers, dstTopic, len(records))
	byKey := map[string]*kgo.Record{}
	for _, r := range got {
		byKey[string(r.Key)] = r
	}
	for _, want := range records {
		r, ok := byKey[string(want.Key)]
		is.True(ok) // key %q was copied
		if !ok {
			continue
		}
		is.Equal(r.Partition, want.Partition)
		is.Equal(r.Value, want.Value)
		is.Equal(r.Timestamp.UnixMilli(), want.Timestamp.UnixMilli())
		is.Equal(r.Headers, want.Headers)
	}
}

// TestMirror_Integration_PartitionCountMismatch fails on the first record when
// the destination has fewer partitions than the source.
func TestMirror_Integration_PartitionCountMismatch(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	srcTopic := "mirror-src-" + uuid.NewString()
	dstTopic := "mirror-dst-" + uuid.NewString()
	createTopicWithPartitions(t, srcTopic, 3)
	createTopicWithPartitions(t, dstTopic, 2) // fewer than the source

	servers := test.ServerList()
	produceToPartitions(t, servers, []*kgo.Record{{
		Topic:     srcTopic,
		Partition: 0,
		Key:       []byte("k"),
		Value:     []byte("v"),
	}})

	srcCfg := map[string]string{
		"servers":            test.Servers(),
		"topics":             srcTopic,
		"readFromBeginning":  "true",
		"emitMirrorMetadata": "true",
	}
	dstCfg := map[string]string{
		"servers": test.Servers(),
		"topic":   dstTopic,
		"mirror":  "true",
		"acks":    "all",
	}

	src := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, srcCfg, src.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(src.Open(ctx, nil))
	defer func() { is.NoErr(src.Teardown(ctx)) }()

	dst := NewDestination()
	is.NoErr(sdk.Util.ParseConfig(ctx, dstCfg, dst.Config(), Connector.NewSpecification().DestinationParams))
	is.NoErr(dst.Open(ctx))
	defer func() { is.NoErr(dst.Teardown(ctx)) }()

	rec, err := src.Read(ctx)
	is.NoErr(err)
	_, err = dst.Write(ctx, []opencdc.Record{rec})
	is.True(err != nil)
	is.True(errors.Is(err, destination.ErrCodePartitionCountMismatch))
}

// createTopicWithPartitions creates a topic with the given partition count and
// deletes it when the test ends.
func createTopicWithPartitions(t *testing.T, topic string, partitions int32) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	withAdmin(t, func(adm *kadm.Client) {
		resp, err := adm.CreateTopic(ctx, partitions, 1, nil, topic)
		is.NoErr(err)
		is.NoErr(resp.Err)
	})

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		withAdmin(t, func(adm *kadm.Client) {
			if _, err := adm.DeleteTopics(ctx, topic); err != nil {
				t.Logf("failed to delete topic %q: %v", topic, err)
			}
		})
	})
}

// produceToPartitions produces each record to its explicit partition.
func produceToPartitions(t *testing.T, servers []string, records []*kgo.Record) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(servers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	is.NoErr(err)
	defer cl.Close()

	results := cl.ProduceSync(ctx, records...)
	is.NoErr(results.FirstErr())
}

// consumeAll reads up to limit records from the topic across all partitions.
func consumeAll(t *testing.T, servers []string, topic string, limit int) []*kgo.Record {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(servers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	is.NoErr(err)
	defer cl.Close()

	var out []*kgo.Record
	for len(out) < limit {
		fetches := cl.PollFetches(ctx)
		is.NoErr(fetches.Err())
		out = append(out, fetches.Records()...)
	}
	return out[:limit]
}
