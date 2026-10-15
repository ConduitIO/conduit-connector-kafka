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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/source"
	"github.com/conduitio/conduit-connector-kafka/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// acceptanceV2Partitions is the partition count of every topic the v2 suite
// uses. More than one, so that the ordering and resume cases run against
// key-hash partitioning and a consumer group that owns several partitions,
// as in production.
const acceptanceV2Partitions = 3

// acceptanceV2MaxBytes is the destination batchBytes and the topic
// max.message.bytes used by the v2 suite (4 MiB, see createTopic).
const acceptanceV2MaxBytes = 4 << 20

// TestAcceptanceV2 runs acceptance suite v2 (SDK v0.16.0, revision 2.0.0)
// against the broker named by test.Servers (Kafka or Redpanda, see Makefile).
func TestAcceptanceV2(t *testing.T) {
	srcCfg := map[string]string{
		"servers":           test.Servers(),
		"readFromBeginning": "true",
	}
	destCfg := map[string]string{
		"servers":     test.Servers(),
		"batchBytes":  strconv.Itoa(acceptanceV2MaxBytes),
		"acks":        "all",
		"compression": "snappy",
	}

	sources := &killedSources{}
	conn := Connector
	conn.NewSource = sources.newSource

	sdk.AcceptanceTestV2(t, acceptanceV2Driver{
		sources: sources,
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         conn,
				SourceConfig:      srcCfg,
				DestinationConfig: destCfg,

				BeforeTest: func(t *testing.T) {
					lastSlash := strings.LastIndex(t.Name(), "/")
					topic := t.Name()[lastSlash+1:] + uuid.NewString()
					srcCfg["topics"] = topic
					destCfg["topic"] = topic
					createTopic(t, topic, acceptanceV2Partitions)
				},

				WriteTimeout: 10 * time.Second,
				ReadTimeout:  10 * time.Second,

				// Claims, and why each is made or not. The delivery
				// semantics section of the README states the same.
				Capabilities: sdk.AcceptanceCapabilities{
					Source: sdk.SourceCapabilities{
						// Records with one key are in one partition, and a
						// partition is read in offset order. Redelivery after
						// a restart starts at the committed offset of the
						// partition, so it repeats a suffix in order.
						OrdersByKey: true,
						// Not ExactResume: offsets are committed in batches
						// of commitOffsetsSize acks (default 1000) or every
						// commitOffsetsDelay (default 5s), so after a crash
						// up to commitOffsetsSize-1 acked records are
						// delivered again. The bound is above the default of
						// 10, which needs a maintainer waiver.
						MaxRedelivery: 1000,
					},
					// Waiver approved by DeVaris Brown on 2026-10-15
					// (ConduitIO/conduit-connector-kafka#324); the
					// registry entry carries the approval.
					Waivers: []string{"max_redelivery"},
					Destination: sdk.DestinationCapabilities{
						// Replaying a record produces it again: the
						// idempotent producer deduplicates retries of one
						// produce request, not a record sent twice.
						Idempotence: sdk.IdempotenceNo,
						// OrdersByKey is not claimed although DST-06
						// passes: a record the broker rejects after it was
						// enqueued is nacked together with the later records
						// of its batch, which were written, so the replay
						// stores it after them (see the README, delivery
						// semantics).
					},
				},
			},
		},
	})
}

type acceptanceV2Driver struct {
	sdk.ConfigurableAcceptanceTestDriver
	sources *killedSources
}

var (
	_ sdk.AcceptanceReadAllDriver      = acceptanceV2Driver{}
	_ sdk.AcceptanceProcessDeathDriver = acceptanceV2Driver{}
)

// KilledSessionID names the consumer group membership of the source the suite
// just killed. See killedSources for why the membership has to be released.
func (d acceptanceV2Driver) KilledSessionID(*testing.T) string {
	return d.sources.sessionID()
}

// ReleaseSession ends the group membership of the killed source, as the death
// of its process would.
func (d acceptanceV2Driver) ReleaseSession(_ context.Context, sessionID string) error {
	return d.sources.release(sessionID)
}

// ReadFromDestination reads the topic directly instead of through the source
// connector, whose consumer group makes reads slow.
func (d acceptanceV2Driver) ReadFromDestination(t *testing.T, records []opencdc.Record) []opencdc.Record {
	all := d.ReadAllFromDestination(t)
	if len(all) > len(records) {
		all = all[:len(records)]
	}
	return all
}

// ReadAllFromDestination returns every record in the topic, duplicates
// included, in partition order: all of partition 0 in offset order, then
// partition 1, and so on. It reads up to the end offsets listed when it
// starts, so records still being produced are not waited for.
func (d acceptanceV2Driver) ReadAllFromDestination(t *testing.T) []opencdc.Record {
	is := is.New(t)
	cfg := test.ParseConfigMap[source.Config](t, d.SourceConfig(t))
	topic := cfg.Topics[0]

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adm, err := kgo.NewClient(kgo.SeedBrokers(cfg.Servers...))
	is.NoErr(err)
	defer adm.Close()
	ends, err := kadm.NewClient(adm).ListEndOffsets(ctx, topic)
	is.NoErr(err)
	is.NoErr(ends.Error())

	want := map[int32]int64{} // partition -> end offset
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			want[o.Partition] = o.Offset
		}
	})

	byPartition := map[int32][]*kgo.Record{}
	if len(want) > 0 {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.Servers...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		is.NoErr(err)
		defer cl.Close()

		done := func() bool {
			for p, end := range want {
				recs := byPartition[p]
				if len(recs) == 0 || recs[len(recs)-1].Offset < end-1 {
					return false
				}
			}
			return true
		}
		for !done() {
			fetches := cl.PollFetches(ctx)
			is.NoErr(ctx.Err()) // timed out reading the topic to its end offsets
			is.NoErr(fetches.Err())
			fetches.EachRecord(func(r *kgo.Record) {
				if r.Offset < want[r.Partition] {
					byPartition[r.Partition] = append(byPartition[r.Partition], r)
				}
			})
		}
	}

	var out []opencdc.Record
	for p := int32(0); p < acceptanceV2Partitions; p++ {
		for _, rec := range byPartition[p] {
			metadata := opencdc.Metadata{}
			metadata.SetCollection(rec.Topic)
			metadata.SetCreatedAt(rec.Timestamp)
			out = append(out, sdk.Util.Source.NewRecordCreate(
				source.Position{
					Topic:     rec.Topic,
					Partition: rec.Partition,
					Offset:    rec.Offset,
				}.ToSDKPosition(),
				metadata,
				opencdc.RawData(rec.Key),
				opencdc.RawData(rec.Value),
			))
		}
	}
	return out
}

// createTopic creates the topic with the given partition count and deletes
// it when the test ends. The admin client is closed before returning, so the
// suite's goroutine leak checks, which run before t.Cleanup, do not see it.
func createTopic(t *testing.T, topic string, partitions int32) {
	is := is.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The large record cases (DST-08, SRC-06) send a 1 MiB payload, which the
	// default opencdc/json value format inflates by about a third. The broker
	// default max.message.bytes (about 1 MiB) rejects it, so the topic allows
	// acceptanceV2MaxBytes, matching the destination's batchBytes.
	maxBytes := strconv.Itoa(acceptanceV2MaxBytes)
	withAdmin(t, func(adm *kadm.Client) {
		resp, err := adm.CreateTopic(ctx, partitions, 1, map[string]*string{"max.message.bytes": &maxBytes}, topic)
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

func withAdmin(t *testing.T, fn func(*kadm.Client)) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(test.ServerList()...))
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}
	defer cl.Close()
	fn(kadm.NewClient(cl))
}

// killedSources tracks the sources the suite opens, so that the group
// membership of a killed one can be ended (AcceptanceProcessDeathDriver).
//
// The suite kills a source in-process: it cancels the context and never calls
// Teardown. The source's franz-go client is then still running, and it keeps
// heartbeating and keeps the partitions the group assigned to it. The source
// the suite opens next joins the same group and is assigned only the remaining
// partitions, so records on the dead instance's partitions are never read and
// SRC-03 reports a gap. When a real process dies its connections close, the
// broker drops the member after session.timeout.ms and the group reassigns its
// partitions.
//
// The hook was accepted by DeVaris Brown on 2026-10-15
// (ConduitIO/conduit-connector-kafka#324).
//
// release closes the killed source's franz-go client and nothing else. It does
// not call the source's Teardown or flush its acks: with auto-commit disabled
// (the source always disables it when it uses a group), franz-go's Close leaves
// the group without committing an offset, so offsets stay where the killed
// source left them. Using the hook is recorded in the report as the exception
// "process_death_hook"; the kill -9 chaos test covers a real process death.
type killedSources struct {
	mu   sync.Mutex
	open []*trackedSource
}

type trackedSource struct {
	*Source
	// tornDown is set by Teardown, which the SDK calls from its own
	// goroutine, and by release.
	tornDown atomic.Bool
}

func (k *killedSources) newSource() sdk.Source {
	ts := &trackedSource{Source: newSource()}
	k.mu.Lock()
	k.open = append(k.open, ts)
	k.mu.Unlock()
	return sdk.SourceWithMiddleware(ts)
}

// Teardown marks the source as torn down, so release leaves it alone.
func (ts *trackedSource) Teardown(ctx context.Context) error {
	ts.tornDown.Store(true)
	return ts.Source.Teardown(ctx)
}

// killed returns the opened sources that were neither torn down nor released.
// Only one source runs at a time in the cases that kill, so this is the one
// that was killed.
func (k *killedSources) killed() []*trackedSource {
	var out []*trackedSource
	for _, ts := range k.open {
		if !ts.tornDown.Load() && ts.consumer != nil {
			out = append(out, ts)
		}
	}
	return out
}

func (k *killedSources) sessionID() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	var ids []string
	for _, ts := range k.killed() {
		ids = append(ids, ts.config.GroupID)
	}
	return strings.Join(ids, ",")
}

func (k *killedSources) release(sessionID string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	var released []string
	for _, ts := range k.killed() {
		fc, ok := ts.consumer.(*source.FranzConsumer)
		if !ok {
			return fmt.Errorf("unexpected consumer type %T", ts.consumer)
		}
		franzClient(fc).Close()
		ts.tornDown.Store(true) // released: never close it twice
		released = append(released, ts.config.GroupID)
	}
	if got := strings.Join(released, ","); got != sessionID {
		return fmt.Errorf("released group memberships %q, expected %q", got, sessionID)
	}
	return nil
}

// franzClient returns the unexported franz-go client of the consumer. It is
// read with reflection so that the hook needs no API that would let production
// code close the client without the consumer's own Close. It panics if the
// field is renamed, which fails the test.
func franzClient(fc *source.FranzConsumer) source.Client {
	f := reflect.ValueOf(fc).Elem().FieldByName("client")
	return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Interface().(source.Client)
}
