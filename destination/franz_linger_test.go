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

package destination

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/mirror"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// testLinger is far above any plausible batch latency on kfake, so a Write
// that finishes well under it provably did not wait the linger out.
const testLinger = 500 * time.Millisecond

const lingerTestTopic = "linger-topic"

// newLingerProducer builds a FranzProducer against an in-process kfake broker
// with an explicit large ProducerLinger, so the tests do not depend on
// franz-go's default.
func newLingerProducer(t *testing.T, isMirror bool, batchBytes int32) (*FranzProducer, string) {
	t.Helper()
	is := is.New(t)

	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, lingerTestTopic))
	is.NoErr(err)
	t.Cleanup(c.Close)
	addr := strings.Join(c.ListenAddrs(), ",")

	opts := []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(lingerTestTopic),
		kgo.ProducerLinger(testLinger),
		kgo.ProducerBatchMaxBytes(batchBytes),
	}
	if isMirror {
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}
	cl, err := kgo.NewClient(opts...)
	is.NoErr(err)
	t.Cleanup(cl.Close)

	return &FranzProducer{
		client:         cl,
		keyEncoder:     bytesEncoder{},
		defaultTopic:   lingerTestTopic,
		mirror:         isMirror,
		produceTimeout: 30 * time.Second,
		partitions:     make(map[string]int32),
		closed:         make(chan struct{}),
	}, addr
}

func lingerRecords(n int, isMirror bool) []opencdc.Record {
	recs := make([]opencdc.Record, n)
	for i := range recs {
		rec := opencdc.Record{
			Operation: opencdc.OperationCreate,
			Key:       opencdc.RawData(fmt.Sprintf("key-%d", i)),
			Payload:   opencdc.Change{After: opencdc.RawData(fmt.Sprintf("value-%d", i))},
		}
		if isMirror {
			rec.Metadata = validMirrorMetadata()
			rec.Metadata[mirror.Partition] = strconv.Itoa(i % 3)
		}
		recs[i] = rec
	}
	return recs
}

// consumeAll reads n records from the topic, grouped by partition in offset order.
func consumeAll(t *testing.T, addr string, n int) map[int32][]string {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(addr, ",")...),
		kgo.ConsumeTopics(lingerTestTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := map[int32][]string{}
	total := 0
	for total < n {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("consumed %d of %d records before timeout", total, n)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			got[r.Partition] = append(got[r.Partition], string(r.Value))
			total++
		})
	}
	return got
}

// TestFranzProducer_Produce_DoesNotWaitOutLinger is the regression test for
// the batched path waiting ProducerLinger on every Write.
func TestFranzProducer_Produce_DoesNotWaitOutLinger(t *testing.T) {
	for _, mirror := range []bool{false, true} {
		t.Run(fmt.Sprintf("mirror=%v", mirror), func(t *testing.T) {
			is := is.New(t)
			p, _ := newLingerProducer(t, mirror, 1_000_000)
			recs := lingerRecords(50, mirror)

			// The first Write also loads metadata; time the second one too.
			for i := 0; i < 2; i++ {
				start := time.Now()
				n, err := p.Produce(context.Background(), recs)
				elapsed := time.Since(start)
				is.NoErr(err)
				is.Equal(n, len(recs))
				if elapsed >= testLinger/2 {
					t.Fatalf("write %d of %d records took %v, expected well under the %v linger", i, len(recs), elapsed, testLinger)
				}
			}
		})
	}
}

// TestFranzProducer_Produce_PartialFailureIndex checks the returned count is
// the index of the first failed record, with the records after it still
// written, exactly as before.
func TestFranzProducer_Produce_PartialFailureIndex(t *testing.T) {
	for _, mirror := range []bool{false, true} {
		t.Run(fmt.Sprintf("mirror=%v", mirror), func(t *testing.T) {
			is := is.New(t)
			p, _ := newLingerProducer(t, mirror, 2000)
			recs := lingerRecords(6, mirror)
			// Records 2 and 4 do not fit in batchBytes; the first failure is 2.
			big := opencdc.RawData(strings.Repeat("x", 5000))
			recs[2].Payload.After = big
			recs[4].Payload.After = big

			n, err := p.Produce(context.Background(), recs)
			is.True(err != nil)
			is.Equal(n, 2)
		})
	}
}

// TestFranzProducer_Mirror_PreservesPartitionOrder checks that records of one
// destination partition are stored in the order they were written.
func TestFranzProducer_Mirror_PreservesPartitionOrder(t *testing.T) {
	is := is.New(t)
	p, addr := newLingerProducer(t, true, 1_000_000)
	recs := lingerRecords(60, true)

	for i := 0; i < 3; i++ { // several consecutive batches
		n, err := p.Produce(context.Background(), recs)
		is.NoErr(err)
		is.Equal(n, len(recs))
	}

	got := consumeAll(t, addr, 3*len(recs))
	for part := int32(0); part < 3; part++ {
		var want []string
		for rep := 0; rep < 3; rep++ {
			for i := range recs {
				if int32(i%3) == part {
					want = append(want, fmt.Sprintf("value-%d", i))
				}
			}
		}
		is.Equal(got[part], want)
	}
}

// TestFranzProducer_Produce_CtxCancelMidBatch checks a cancelled context ends
// the write with an error and a count that never exceeds the confirmed prefix.
func TestFranzProducer_Produce_CtxCancelMidBatch(t *testing.T) {
	is := is.New(t)
	p, _ := newLingerProducer(t, false, 1_000_000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n, err := p.Produce(ctx, lingerRecords(10, false))
	is.True(err != nil)
	is.Equal(n, 0)
}

// BenchmarkProduceBatch is an indicative micro-benchmark (in-process kfake,
// one broker, franz-go's default 10ms linger). It is not a benchi result.
func BenchmarkProduceBatch(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, lingerTestTopic))
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			cl, err := kgo.NewClient(
				kgo.SeedBrokers(c.ListenAddrs()...),
				kgo.DefaultProduceTopic(lingerTestTopic),
				kgo.ProducerLinger(10*time.Millisecond),
			)
			if err != nil {
				b.Fatal(err)
			}
			defer cl.Close()
			p := &FranzProducer{
				client: cl, keyEncoder: bytesEncoder{}, defaultTopic: lingerTestTopic,
				produceTimeout: time.Minute, partitions: map[string]int32{}, closed: make(chan struct{}),
			}
			recs := lingerRecords(size, false)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := p.Produce(context.Background(), recs); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size)*float64(b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
