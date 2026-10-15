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

package chaos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	kafka "github.com/conduitio/conduit-connector-kafka"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rs/zerolog"
)

// envChild makes the test binary run as a child pipeline instead of running
// tests. Its value is the JSON-encoded childConfig.
const envChild = "KAFKA_CHAOS_CHILD"

// Lines the child prints on stdout. The parent reads nothing else.
const (
	lineReady = "READY" // source and destination are open
	lineAcked = "ACKED" // "ACKED <n>": n records acked in this run so far
	lineDone  = "DONE"  // the child stopped on its own, after a graceful Teardown
)

// childConfig is what the parent passes to a child.
type childConfig struct {
	Servers string `json:"servers"`
	In      string `json:"in"`
	Out     string `json:"out"`
	GroupID string `json:"groupID"`
	// Batch is the most records the child reads before one destination Write.
	Batch int `json:"batch"`
	// IdleExit makes the child stop gracefully once no record arrived for
	// this long. Zero means run until killed.
	IdleExit time.Duration `json:"idleExit"`
}

// runChild is a minimal pipeline: Kafka source to Kafka destination, through
// the connectors' public sdk.Source and sdk.Destination (with middleware), in
// one process. It acks a record to the source only after the destination's
// Write returned it as written, which is the order Conduit guarantees. What is
// under test is the connectors: that the source commits only acked offsets and
// resumes from them, and that the destination returns a record as written only
// once the broker confirmed it. Conduit's own ack ordering is covered by
// conduit's tests/chaos.
func runChild(raw string) int {
	var cfg childConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		fmt.Fprintln(os.Stderr, "child: bad config:", err)
		return 2
	}

	logger := zerolog.New(os.Stderr).Level(zerolog.WarnLevel).With().Timestamp().Logger()
	ctx, cancel := signal.NotifyContext(logger.WithContext(context.Background()), syscall.SIGTERM)
	defer cancel()

	if err := pipeline(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "child:", err)
		return 1
	}
	fmt.Println(lineDone)
	return 0
}

func pipeline(ctx context.Context, cfg childConfig) (err error) {
	spec := kafka.Connector.NewSpecification()

	src := kafka.NewSource()
	if err := sdk.Util.ParseConfig(ctx, config.Config{
		"servers":           cfg.Servers,
		"topics":            cfg.In,
		"groupID":           cfg.GroupID,
		"readFromBeginning": "true",
	}, src.Config(), spec.SourceParams); err != nil {
		return fmt.Errorf("configure source: %w", err)
	}
	if err := src.Open(ctx, nil); err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	// A teardown runs only on a graceful stop. A SIGKILL never gets here,
	// which is the point.
	defer func() {
		err = errors.Join(err, src.Teardown(context.WithoutCancel(ctx)))
	}()

	dst := kafka.NewDestination()
	if err := sdk.Util.ParseConfig(ctx, config.Config{
		"servers": cfg.Servers,
		"topic":   cfg.Out,
		"acks":    "all",
	}, dst.Config(), spec.DestinationParams); err != nil {
		return fmt.Errorf("configure destination: %w", err)
	}
	if err := dst.Open(ctx); err != nil {
		return fmt.Errorf("open destination: %w", err)
	}
	defer func() {
		err = errors.Join(err, dst.Teardown(context.WithoutCancel(ctx)))
	}()

	fmt.Println(lineReady)

	acked := 0
	lastRecord := time.Now()
	for ctx.Err() == nil {
		batch, err := readBatch(ctx, src, cfg.Batch)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			if cfg.IdleExit > 0 && time.Since(lastRecord) > cfg.IdleExit {
				return nil
			}
			continue
		}
		lastRecord = time.Now()

		n, err := dst.Write(ctx, batch)
		if err != nil {
			return fmt.Errorf("write: %d of %d written: %w", n, len(batch), err)
		}
		if n != len(batch) {
			return fmt.Errorf("write: %d of %d written without an error", n, len(batch))
		}
		// Invariant 1: ack upstream only what the destination confirmed.
		for _, r := range batch {
			if err := src.Ack(ctx, r.Position); err != nil {
				return fmt.Errorf("ack: %w", err)
			}
		}
		acked += len(batch)
		fmt.Printf("%s %d\n", lineAcked, acked)
	}
	return nil
}

// readBatch reads up to n records: it waits up to a second for the first one
// and then takes what arrives within 20ms of the previous one.
func readBatch(ctx context.Context, src sdk.Source, n int) ([]opencdc.Record, error) {
	var out []opencdc.Record
	wait := time.Second
	for len(out) < n {
		rctx, cancel := context.WithTimeout(ctx, wait)
		r, err := src.Read(rctx)
		cancel()
		switch {
		case err == nil:
			out = append(out, r)
			wait = 20 * time.Millisecond
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sdk.ErrBackoffRetry):
			return out, nil
		case ctx.Err() != nil:
			return out, nil
		default:
			return out, fmt.Errorf("read: %w", err)
		}
	}
	return out, nil
}
