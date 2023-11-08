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
	"context"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/matryer/is"
)

func TestFranzConsumer_Consume_FromBeginning(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	ctx := context.Background()

	cfg := test.ParseConfigMap[Config](t, test.SourceConfigMap(t))
	cfg.ReadFromBeginning = true

	records := test.GenerateFranzRecords(1, 6)
	test.CreateTopic(t, cfg.Config)
	test.Produce(t, cfg.Config, records)

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

	cfg := test.ParseConfigMap[Config](t, test.SourceConfigMap(t))
	cfg.ReadFromBeginning = false

	records := test.GenerateFranzRecords(1, 6)
	test.CreateTopic(t, cfg.Config)
	test.Produce(t, cfg.Config, records)

	c, err := NewFranzConsumer(ctx, cfg)
	is.NoErr(err)
	defer func() {
		err := c.Close(ctx)
		is.NoErr(err)
	}()

	ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	got, err := c.Consume(ctxTimeout)
	is.Equal(err, context.DeadlineExceeded)
	is.Equal(got, nil)

	records = test.GenerateFranzRecords(7, 9)
	test.Produce(t, cfg.Config, records)

	for i := 0; i < len(records); i++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		got, err := c.Consume(ctx)
		is.NoErr(err)
		is.Equal(got.Key, records[i].Key)
	}
}
