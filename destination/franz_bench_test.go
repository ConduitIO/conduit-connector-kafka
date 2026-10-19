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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/common"
)

// buildBenchRecord builds a representative opencdc.Record the way the SDK
// hands one to the destination: RawData key, StructuredData payload, and the
// collection/created-at metadata the source normally emits.
func buildBenchRecord(i int) opencdc.Record {
	metadata := opencdc.Metadata{}
	metadata.SetCollection("bench-topic")
	metadata.SetCreatedAt(time.Unix(1700000000, 0))

	return opencdc.Record{
		Position:  opencdc.Position(fmt.Sprintf("pos-%d", i)),
		Operation: opencdc.OperationCreate,
		Metadata:  metadata,
		Key:       opencdc.RawData(fmt.Sprintf("key-%d", i)),
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"id":    i,
				"name":  fmt.Sprintf("name-%d", i),
				"value": fmt.Sprintf("value-%d", i),
			},
		},
	}
}

// newBenchProducer returns a FranzProducer for a given topic config. A nil
// client is fine here because prepareRecord never touches the network.
func newBenchProducer(topic string) *FranzProducer {
	c := Config{
		Topic:  topic,
		Config: common.Config{Servers: []string{"localhost:9092"}},
	}
	t, f, err := c.ParseTopic()
	if err != nil {
		panic(err)
	}
	return &FranzProducer{
		keyEncoder:     bytesEncoder{},
		getTopic:       f,
		defaultTopic:   t,
		produceTimeout: time.Minute,
		partitions:     make(map[string]int32),
		closed:         make(chan struct{}),
	}
}

// BenchmarkPrepareRecordStatic measures the per-record cost of the
// record→kmsg conversion with a static topic (no topic resolution).
func BenchmarkPrepareRecordStatic(b *testing.B) {
	p := newBenchProducer("bench-topic")
	rec := buildBenchRecord(0)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.Position = opencdc.Position(fmt.Sprintf("pos-%d", i))
		if _, err := p.prepareRecord(ctx, rec); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPrepareRecordTemplate measures the per-record cost with the default
// templated topic ({{ index .Metadata "opencdc.collection" }}), which is what
// every default-configured destination resolves per record.
func BenchmarkPrepareRecordTemplate(b *testing.B) {
	p := newBenchProducer(`{{ index .Metadata "opencdc.collection" }}`)
	rec := buildBenchRecord(0)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.Position = opencdc.Position(fmt.Sprintf("pos-%d", i))
		if _, err := p.prepareRecord(ctx, rec); err != nil {
			b.Fatal(err)
		}
	}
}
