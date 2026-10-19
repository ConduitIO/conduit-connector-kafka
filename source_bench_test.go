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
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-kafka/source"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeConsumer is a source.Consumer that serves pre-built records so the Read
// conversion path can be benchmarked in isolation (no network).
type fakeConsumer struct {
	rec *source.Record
}

func (f *fakeConsumer) Consume(context.Context) (*source.Record, error) { return f.rec, nil }
func (f *fakeConsumer) Ack(context.Context) error                       { return nil }
func (f *fakeConsumer) Close(context.Context) error                     { return nil }
func (f *fakeConsumer) PartitionCount(context.Context, string) (int32, error) {
	return 1, nil
}

func buildSourceRecord(i int) *source.Record {
	r := &kgo.Record{
		Topic:     "bench-topic",
		Partition: 0,
		Offset:    int64(i),
		Key:       []byte(fmt.Sprintf("key-%d", i)),
		Value:     []byte(fmt.Sprintf("value-%d", i)),
		Timestamp: time.Unix(1700000000, 0),
		Headers: []kgo.RecordHeader{
			{Key: "h1", Value: []byte("v1")},
			{Key: "h2", Value: []byte("v2")},
		},
	}
	return (*source.Record)(r)
}

func newBenchSource() *Source {
	s := newSource()
	s.consumer = &fakeConsumer{rec: buildSourceRecord(0)}
	return s
}

// BenchmarkSourceRead measures the per-record cost of the kgo.Record→
// opencdc.Record conversion in Source.Read.
func BenchmarkSourceRead(b *testing.B) {
	s := newBenchSource()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Read(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
