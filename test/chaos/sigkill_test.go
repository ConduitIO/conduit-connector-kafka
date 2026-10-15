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

//go:build chaos

// Package chaos kills a Kafka-to-Kafka pipeline built from this connector with
// SIGKILL, restarts it, and checks data integrity invariants 1 to 3 (and the
// per-key order the source claims) against what is actually in the topics.
//
// Run it with "make test-chaos-kafka" or "make test-chaos-redpanda".
package chaos

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	partitions = 3
	keys       = 50
	// commitOffsetsSize is the source default; the redelivery bound below
	// depends on it.
	commitOffsetsSize = 1000
	batch             = 100
	// childMaxLifetime bounds every child process.
	childMaxLifetime = 10 * time.Minute
)

func TestMain(m *testing.M) {
	if raw := os.Getenv(envChild); raw != "" {
		os.Exit(runChild(raw))
	}
	os.Exit(m.Run())
}

// TestSIGKILL_ResumeNoGap pre-fills a source topic, runs the pipeline in a
// child process, kills it with SIGKILL at several points while it is copying,
// restarts it each time with the same consumer group, and lets the last run
// finish. After every kill, before the restart, it checks:
//
//   - Invariant 1: every source record below the group's committed offset is
//     already in the destination topic. A commit ahead of the destination
//     would be an early ack.
//   - Invariant 2: committed offsets never move backwards.
//
// After the last run:
//
//   - Invariant 3: every source record is in the destination topic.
//   - Redelivery is bounded: duplicates per kill are at most commitOffsetsSize
//     acked-but-uncommitted records plus one in-flight batch.
//   - Per-key order: in each destination partition, no record of a key appears
//     for the first time after a later record of the same key.
//   - A graceful stop commits everything: committed offsets equal the end
//     offsets of the source topic.
func TestSIGKILL_ResumeNoGap(t *testing.T) {
	const total = 20000
	killAfter := []int{150, 2500, 1200, 4000} // acks in that run before SIGKILL

	servers := test.ServerList()
	in, out := "chaos-in-"+uuid.NewString(), "chaos-out-"+uuid.NewString()
	createTopics(t, servers, in, out)
	produceSource(t, servers, in, total)
	inRecs := readAll(t, servers, in) // partition -> records in offset order
	if n := count(inRecs); n != total {
		t.Fatalf("source topic holds %d records, want %d", n, total)
	}

	cfg := childConfig{
		Servers: test.Servers(),
		In:      in,
		Out:     out,
		GroupID: "chaos-" + uuid.NewString(),
		Batch:   batch,
	}

	committed := map[int32]int64{}
	for i, k := range killAfter {
		c := startChild(t, cfg)
		c.waitAcked(t, k, 3*time.Minute)
		c.kill(t)

		now := committedOffsets(t, servers, cfg.GroupID, in)
		for p, o := range committed {
			if now[p] < o {
				t.Fatalf("kill %d: committed offset of partition %d went back from %d to %d (invariant 2)", i, p, o, now[p])
			}
		}
		committed = now
		stored := values(unwrap(t, readAll(t, servers, out)))
		for p, recs := range inRecs {
			for _, r := range recs {
				if r.Offset >= committed[p] {
					break
				}
				if _, ok := stored[string(r.Value)]; !ok {
					t.Fatalf("kill %d: partition %d offset %d is committed (offset %d) but not in the destination (invariant 1)", i, p, r.Offset, committed[p])
				}
			}
		}
		t.Logf("kill %d after %d acks: committed %v, destination holds %d distinct records", i, k, committed, len(stored))
	}

	// last run: until the source is drained, then a graceful stop
	final := cfg
	// The killed instance stays in the group until its session times out
	// (45s by default), and the restarted one gets no records until then, so
	// "idle" must outlast that pause.
	final.IdleExit = 90 * time.Second
	c := startChild(t, final)
	c.waitDone(t, 5*time.Minute)

	outRecs := unwrap(t, readAll(t, servers, out))
	stored := values(outRecs)
	var missing []string
	for _, recs := range inRecs {
		for _, r := range recs {
			if _, ok := stored[string(r.Value)]; !ok {
				missing = append(missing, string(r.Value))
			}
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d of %d source records never reached the destination (invariant 3), first: %s", len(missing), total, missing[0])
	}

	dups := count(outRecs) - total
	if limit := len(killAfter) * (commitOffsetsSize + batch); dups > limit {
		t.Errorf("%d duplicates after %d kills, more than the bound of %d", dups, len(killAfter), limit)
	}
	t.Logf("%d records, %d duplicates after %d kills", total, dups, len(killAfter))

	checkKeyOrder(t, outRecs)

	ends := endOffsets(t, servers, in)
	committed = committedOffsets(t, servers, cfg.GroupID, in)
	for p, end := range ends {
		if committed[p] != end {
			t.Errorf("after the graceful stop partition %d is committed at %d, end offset %d", p, committed[p], end)
		}
	}
}

// checkKeyOrder fails if, in any destination partition, a record of a key
// shows up for the first time after a record of the same key that was written
// to the source later. Re-delivered records (seen before) are allowed.
func checkKeyOrder(t *testing.T, outRecs map[int32][]*kgo.Record) {
	t.Helper()
	for p, recs := range outRecs {
		seen := map[string]bool{}
		maxSeq := map[string]int{}
		for _, r := range recs {
			v := string(r.Value)
			seq := seqOf(t, v)
			key := string(r.Key)
			if !seen[v] && seq < maxSeq[key] {
				t.Fatalf("partition %d: %s first appears after record %d of key %s (per-key order)", p, v, maxSeq[key], key)
			}
			seen[v] = true
			if seq > maxSeq[key] {
				maxSeq[key] = seq
			}
		}
	}
}

// ---- child process control ----

type child struct {
	cmd    *exec.Cmd
	lines  chan string
	done   chan error
	exited chan struct{} // closed when the process is gone and its output read
}

func startChild(t *testing.T, cfg childConfig) *child {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Every child is killed after childMaxLifetime at the latest, even if
	// the parent dies without running its cleanup.
	ctx, cancel := context.WithTimeout(context.Background(), childMaxLifetime)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), envChild+"="+string(raw))
	cmd.Stderr = &prefixWriter{t: t}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := &child{cmd: cmd, lines: make(chan string, 1024), done: make(chan error, 1), exited: make(chan struct{})}
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			select {
			case c.lines <- sc.Text():
			default: // the parent only needs the latest lines
			}
		}
		err := cmd.Wait() // returns after the child's stderr is fully copied
		close(c.exited)
		c.done <- err
		close(c.lines)
	}()
	// Kill and wait, so no stderr line of the child reaches t.Logf after the
	// test has ended.
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-c.exited
	})
	return c
}

func (c *child) waitAcked(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				t.Fatalf("child exited before acking %d records: %v", n, <-c.done)
			}
			if v, found := strings.CutPrefix(line, lineAcked+" "); found {
				if got, _ := strconv.Atoi(v); got >= n {
					return
				}
			}
		case <-deadline:
			t.Fatalf("child did not ack %d records within %v", n, timeout)
		}
	}
}

func (c *child) waitDone(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				if err := <-c.done; err != nil {
					t.Fatalf("child failed: %v", err)
				}
				return
			}
			_ = line
		case <-deadline:
			t.Fatalf("child did not finish within %v", timeout)
		}
	}
}

// kill sends SIGKILL (exec.Cmd.Process.Kill) and waits for the process to be
// gone, so the parent never inspects state a dying child could still change.
func (c *child) kill(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	for range c.lines {
		// drain until the reader goroutine ends
	}
	<-c.done
}

type prefixWriter struct{ t *testing.T }

func (w *prefixWriter) Write(p []byte) (int, error) {
	w.t.Logf("child: %s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// ---- Kafka helpers ----

func admin(t *testing.T, servers []string) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(servers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

func createTopics(t *testing.T, servers []string, topics ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adm := admin(t, servers)
	resp, err := adm.CreateTopics(ctx, partitions, 1, nil, topics...)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Error(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, topics...)
	})
}

// produceSource writes total records, "seq-<i>" with key "key-<i mod keys>",
// in order, so that each key's records sit in one partition in seq order.
func produceSource(t *testing.T, servers []string, topic string, total int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(servers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	recs := make([]*kgo.Record, total)
	for i := range recs {
		recs[i] = &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i%keys)),
			Value: []byte(fmt.Sprintf("seq-%d", i)),
		}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func endOffsets(t *testing.T, servers []string, topic string) map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ends, err := admin(t, servers).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if err := ends.Error(); err != nil {
		t.Fatal(err)
	}
	out := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) { out[o.Partition] = o.Offset })
	return out
}

func committedOffsets(t *testing.T, servers []string, group, topic string) map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	offs, err := admin(t, servers).FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		t.Fatal(err)
	}
	out := map[int32]int64{}
	for p, o := range offs[topic] {
		if o.Err != nil {
			t.Fatalf("committed offset of partition %d: %v", p, o.Err)
		}
		out[p] = o.At // -1 when nothing is committed
	}
	return out
}

// readAll reads the topic up to its current end offsets.
func readAll(t *testing.T, servers []string, topic string) map[int32][]*kgo.Record {
	t.Helper()
	ends := endOffsets(t, servers, topic)
	out := map[int32][]*kgo.Record{}
	pending := 0
	for _, e := range ends {
		if e > 0 {
			pending++
		}
	}
	if pending == 0 {
		return out
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(servers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for pending > 0 {
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("reading %s: %v", topic, err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			end := ends[r.Partition]
			if r.Offset >= end {
				return
			}
			out[r.Partition] = append(out[r.Partition], r)
			if r.Offset == end-1 {
				pending--
			}
		})
	}
	return out
}

// values maps each source value to how often it is in recs.
func values(recs map[int32][]*kgo.Record) map[string]int {
	out := map[string]int{}
	for _, rs := range recs {
		for _, r := range rs {
			out[string(r.Value)]++
		}
	}
	return out
}

// unwrap replaces each destination record's value, an OpenCDC record in the
// destination's default opencdc/json format, with the source value it carries
// (payload.after, raw bytes, base64 in JSON).
func unwrap(t *testing.T, recs map[int32][]*kgo.Record) map[int32][]*kgo.Record {
	t.Helper()
	for _, rs := range recs {
		for _, r := range rs {
			var env struct {
				Payload struct {
					After []byte `json:"after"`
				} `json:"payload"`
			}
			if err := json.Unmarshal(r.Value, &env); err != nil {
				t.Fatalf("destination record at %d/%d is not an opencdc/json record: %v", r.Partition, r.Offset, err)
			}
			r.Value = env.Payload.After
		}
	}
	return recs
}

func count(recs map[int32][]*kgo.Record) int {
	n := 0
	for _, rs := range recs {
		n += len(rs)
	}
	return n
}

func seqOf(t *testing.T, v string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimPrefix(v, "seq-"))
	if err != nil {
		t.Fatalf("unexpected value %q", v)
	}
	return n
}
