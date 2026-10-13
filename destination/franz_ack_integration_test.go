// Copyright © 2023 Meroxa, Inc.
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
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestFranzProducer_Produce_FailedRecordNotAcked is the regression test for
// #308. Produce reports "the first n records were written"; a failed record
// must never fall inside that prefix, whichever order the broker completes
// the per-record promises in.
//
// Record 0 goes to a topic whose broker-side max.message.bytes it exceeds, so
// the broker rejects it. Records 1..9 go to a healthy topic and are
// written. The rejection arrives after the healthy records have completed in
// at least some runs, which put the failed record's error last in
// completion order.
func TestFranzProducer_Produce_FailedRecordNotAcked(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	servers := []string{"localhost:9092"}
	suffix := uuid.NewString()
	badTopic, goodTopic := "bad-"+suffix, "good-"+suffix

	adm, err := kgo.NewClient(kgo.SeedBrokers(servers...))
	is.NoErr(err)
	defer adm.Close()
	kadmCl := kadm.NewClient(adm)
	small := "1024"
	resp, err := kadmCl.CreateTopic(ctx, 1, 1, map[string]*string{"max.message.bytes": &small}, badTopic)
	is.NoErr(err)
	is.NoErr(resp.Err)
	resp, err = kadmCl.CreateTopic(ctx, 1, 1, nil, goodTopic)
	is.NoErr(err)
	is.NoErr(resp.Err)
	t.Cleanup(func() {
		_, _ = kadmCl.DeleteTopics(context.Background(), badTopic, goodTopic)
	})

	cfg := test.ParseConfigMap[Config](t, map[string]string{
		"servers":    "localhost:9092",
		"topic":      `{{ index .Metadata "t" }}`,
		"batchBytes": "1000012",
		"acks":       "all",
	})
	cfg.Config = test.ConfigWithIntegrationTestOptions(cfg.Config)
	p, err := NewFranzProducer(ctx, cfg)
	is.NoErr(err)
	defer func() { is.NoErr(p.Close(ctx)) }()

	big := make([]byte, 200*1024)
	_, _ = rand.Read(big)
	recs := make([]opencdc.Record, 10)
	for i := range recs {
		topic, payload := goodTopic, []byte(fmt.Sprintf("record-%d", i))
		if i == 0 {
			topic, payload = badTopic, big
		}
		recs[i] = opencdc.Record{
			Position:  opencdc.Position(fmt.Sprint(i)),
			Operation: opencdc.OperationCreate,
			Metadata:  opencdc.Metadata{"t": topic},
			Key:       opencdc.RawData(fmt.Sprint(i)),
			Payload:   opencdc.Change{After: opencdc.RawData(payload)},
		}
	}

	// Whether the broker rejects record 0 before or after it acknowledges the
	// others is a race, so repeat: before the fix roughly a third of the
	// attempts reported n=9 here.
	for attempt := 0; attempt < 30; attempt++ {
		n, err := p.Produce(ctx, recs)
		is.True(err != nil)
		// record 0 failed, so no record may be reported as written
		if n != 0 {
			t.Fatalf("attempt %d: Produce returned n=%d (err=%v), but record 0 failed; it would be acked upstream", attempt, n, err)
		}
	}
}
