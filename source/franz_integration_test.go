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

package source

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzConsumer_Consume_FromBeginning(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	ctx := context.Background()

	cfg := test.ParseConfigMap[Config](t, test.SourceConfigMap(t, false, false))
	cfg.ReadFromBeginning = true

	records := test.GenerateFranzRecords(1, 6)
	test.CreateTopics(t, cfg.Servers, cfg.Topics)
	test.Produce(t, cfg.Servers, cfg.Topics[0], records)

	c, err := NewFranzConsumer(ctx, cfg)
	is.NoErr(err)
	defer func() {
		err := c.Close(ctx)
		is.NoErr(err)
	}()

	for i := 0; i < len(records); i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		got, err := c.Consume(ctx)
		is.NoErr(err)
		is.Equal(got.Key, records[i].Key)
	}
}

func TestFranzConsumer_Consume_LastOffset(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	ctx := context.Background()

	cfg := test.ParseConfigMap[Config](t, test.SourceConfigMap(t, false, false))
	cfg.ReadFromBeginning = false

	records := test.GenerateFranzRecords(1, 6)
	test.CreateTopics(t, cfg.Servers, cfg.Topics)
	test.Produce(t, cfg.Servers, cfg.Topics[0], records)

	c, err := NewFranzConsumer(ctx, cfg)
	is.NoErr(err)
	defer func() {
		err := c.Close(ctx)
		is.NoErr(err)
	}()

	// The consumer joins the group and resolves its "end" offset asynchronously.
	// Records produced before that happens fall before the resolved offset and
	// are never delivered, so producing a fixed 200ms after creating the
	// consumer is a race (observed on slower brokers/CI). Produce marker records
	// until the consumer delivers one: from then on its position is established
	// and everything produced afterwards must be delivered.
	marker := []byte("marker")
	deadline := time.Now().Add(15 * time.Second)
	for positioned := false; !positioned; {
		is.True(time.Now().Before(deadline)) // consumer never received a marker record
		test.Produce(t, cfg.Servers, cfg.Topics[0], []*kgo.Record{{Key: marker, Value: marker}})

		ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		got, err := c.Consume(ctxTimeout)
		cancel()
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			// not positioned yet, produce another marker
		case err != nil:
			is.NoErr(err)
		default:
			// pre-existing records (1-6) must never be delivered
			is.Equal(got.Key, marker)
			positioned = true
		}
	}

	records = test.GenerateFranzRecords(7, 9)
	test.Produce(t, cfg.Servers, cfg.Topics[0], records)

	for i := 0; i < len(records); {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		got, err := c.Consume(ctx)
		cancel()
		is.NoErr(err)
		if bytes.Equal(got.Key, marker) {
			continue // leftover marker produced while waiting for the position
		}
		is.Equal(got.Key, records[i].Key)
		i++
	}
}

func TestFranzConsumer_Consume_MultipleTopics(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	ctx := context.Background()

	cfg := test.ParseConfigMap[Config](t, test.SourceConfigMap(t, true, false))
	cfg.ReadFromBeginning = true

	records := test.GenerateFranzRecords(1, 6)
	test.CreateTopics(t, cfg.Servers, cfg.Topics)
	test.Produce(t, cfg.Servers, cfg.Topics[0], records[0:3])
	test.Produce(t, cfg.Servers, cfg.Topics[1], records[3:])

	c, err := NewFranzConsumer(ctx, cfg)
	is.NoErr(err)
	defer func() {
		err := c.Close(ctx)
		is.NoErr(err)
	}()

	topic1 := 0
	topic2 := 0
	for i := 0; i < len(records); i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		got, err := c.Consume(ctx)
		is.NoErr(err)
		switch got.Topic {
		case cfg.Topics[0]:
			topic1++
		case cfg.Topics[1]:
			topic2++
		default:
			is.Fail() // unknown topic
		}
	}
	is.Equal(topic1, 3)
	is.Equal(topic2, 3)
}
