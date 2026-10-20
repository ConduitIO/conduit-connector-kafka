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
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/mirror"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

// holdProduces makes the kfake cluster sit on every Produce request until the
// returned release func is called (also run at cleanup, before the cluster
// closes), so the unlingering Flush has records in flight that never resolve.
func holdProduces(t *testing.T, c *kfake.Cluster) func() {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		c.SleepControl(func() { <-release })
		return nil, nil, false
	})
	t.Cleanup(rel)
	return rel
}

// TestFranzProducer_Mirror_FlushConcurrency covers the Flush that runs next
// to the bounded wait in mirror mode while the broker never answers: every
// way the wait can end must nack the whole batch (n == 0), never ack, and
// leave no goroutine behind.
func TestFranzProducer_Mirror_FlushConcurrency(t *testing.T) {
	tests := []struct {
		name  string
		setup func(p *FranzProducer) (context.Context, context.CancelFunc, func())
		check func(t *testing.T, p *FranzProducer, err error, elapsed time.Duration)
	}{
		{
			name: "produceTimeout expires while Flush runs",
			setup: func(p *FranzProducer) (context.Context, context.CancelFunc, func()) {
				p.produceTimeout = 200 * time.Millisecond
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel, func() {}
			},
			check: func(t *testing.T, p *FranzProducer, err error, _ time.Duration) {
				is := is.New(t)
				is.True(errors.Is(err, ErrCodeProduceTimeout))
				is.True(p.poisoned.Load())
			},
		},
		{
			name: "ctx deadline mid-flight",
			setup: func(*FranzProducer) (context.Context, context.CancelFunc, func()) {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				return ctx, cancel, func() {}
			},
			check: func(t *testing.T, _ *FranzProducer, err error, elapsed time.Duration) {
				is := is.New(t)
				is.True(errors.Is(err, context.DeadlineExceeded))
				is.True(elapsed < 5*time.Second)
			},
		},
		{
			name: "concurrent Close mid-flight",
			setup: func(p *FranzProducer) (context.Context, context.CancelFunc, func()) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel, func() {
					time.Sleep(200 * time.Millisecond)
					_ = p.Close(context.Background())
				}
			},
			check: func(t *testing.T, _ *FranzProducer, err error, _ time.Duration) {
				is := is.New(t)
				is.True(strings.Contains(err.Error(), "producer closed"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			baseline := runtime.NumGoroutine()

			func() {
				c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, lingerTestTopic), kfake.SleepOutOfOrder())
				is.NoErr(err)
				defer c.Close()
				cl, err := kgo.NewClient(
					kgo.SeedBrokers(c.ListenAddrs()...),
					kgo.DefaultProduceTopic(lingerTestTopic),
					kgo.ProducerLinger(testLinger),
					kgo.RecordPartitioner(kgo.ManualPartitioner()),
				)
				is.NoErr(err)
				p := &FranzProducer{
					client: cl, keyEncoder: bytesEncoder{}, defaultTopic: lingerTestTopic, mirror: true,
					produceTimeout: 30 * time.Second, partitions: map[string]int32{}, closed: make(chan struct{}),
				}
				release := holdProduces(t, c)
				defer release()
				defer func() { _ = p.Close(context.Background()) }()

				ctx, cancel, during := tc.setup(p)
				defer cancel()
				go during()

				start := time.Now()
				n, err := p.Produce(ctx, lingerRecords(10, true))
				elapsed := time.Since(start)
				is.Equal(n, 0) // nothing was confirmed, nothing may be acked
				is.True(err != nil)
				tc.check(t, p, err, elapsed)
			}()

			// Teardown must not leak the Flush goroutine or its helpers.
			deadline := time.Now().Add(5 * time.Second)
			for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			is.True(runtime.NumGoroutine() <= baseline)
		})
	}
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
