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
	"strings"
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
// The record at badIndex goes to a topic whose broker-side max.message.bytes it exceeds, so
// the broker rejects it. The other records go to a healthy topic and are
// written. The rejection arrives after the healthy records have completed in
// at least some runs, which put the failed record's error last in
// completion order.
func TestFranzProducer_Produce_FailedRecordNotAcked(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	servers := test.ServerList()
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
		"servers":    test.Servers(),
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
	const badIndex = 4 // mid-batch: records before and after it are healthy
	recs := make([]opencdc.Record, 10)
	for i := range recs {
		topic, payload := goodTopic, []byte(fmt.Sprintf("record-%d", i))
		if i == badIndex {
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

	// The broker rejects record 4 independently of how it acknowledges the
	// others, and promises complete in broker order, so repeat: before the fix
	// roughly a third of attempts returned n=9 (the completion position of the
	// error) instead of 4.
	for attempt := 0; attempt < 30; attempt++ {
		n, err := p.Produce(ctx, recs)
		if err == nil {
			t.Fatalf("attempt %d: expected an error", attempt)
		}
		if n != badIndex {
			t.Fatalf("attempt %d: Produce returned n=%d (err=%v), want exactly %d: only records[:%d] may be acked", attempt, n, err, badIndex, badIndex)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("failed to produce record %d:", badIndex)) {
			t.Fatalf("attempt %d: error does not name record %d: %v", attempt, badIndex, err)
		}
	}
}
