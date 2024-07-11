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

//go:generate mockgen -destination franz_mock.go -package source -mock_names=Client=MockClient . Client

package source

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

type FranzConsumer struct {
	client Client
	acker  *batchAcker

	iter *kgo.FetchesRecordIter
}

// Client is a franz-go kafka client.
type Client interface {
	Close()
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	OptValue(opt any) any
	PollFetches(ctx context.Context) kgo.Fetches
	AllowRebalance()
}

var _ Consumer = (*FranzConsumer)(nil)

func NewFranzConsumer(ctx context.Context, cfg Config) (*FranzConsumer, error) {
	c := &FranzConsumer{
		iter: &kgo.FetchesRecordIter{}, // empty iterator is done
	}

	opts := cfg.FranzClientOpts(sdk.Logger(ctx))
	opts = append(opts, []kgo.Opt{
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.BlockRebalanceOnPoll(),
	}...)

	if !cfg.ReadFromBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	if cfg.GroupID != "" {
		opts = append(opts, []kgo.Opt{
			kgo.DisableAutoCommit(),
			kgo.OnPartitionsRevoked(c.lost),
			kgo.OnPartitionsLost(c.lost),
		}...)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	c.client = cl
	c.acker = newBatchAcker(cl, 1000)

	return c, nil
}

func (c *FranzConsumer) lost(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
	// TODO: put a proper retry backoff loop here.
	for c.acker.curBatchIndex != 0 {
		sdk.Logger(ctx).Warn().Msgf("partitions revoked or lost, waiting until current batch of records are acked and committed...")
		time.Sleep(50 * time.Millisecond)
	}
}

func (c *FranzConsumer) Consume(ctx context.Context) (*Record, error) {
	for c.iter.Done() {
		fetches := c.client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return nil, err
		}
		if fetches.Empty() {
			continue // no records, keep polling
		}
		c.iter = fetches.RecordIter()
		c.acker.Records(fetches.Records()...)
		break
	}
	c.client.AllowRebalance()
	return (*Record)(c.iter.Next()), nil
}

func (c *FranzConsumer) Ack(ctx context.Context) error {
	return c.acker.Ack(ctx)
}

func (c *FranzConsumer) Close(ctx context.Context) error {
	var multierr []error

	if err := c.acker.Flush(ctx); err != nil {
		multierr = append(multierr, err)
	}
	c.client.Close()

	return errors.Join(multierr...)
}

// batchAcker commits acks in batches.
type batchAcker struct {
	client Client

	batchSize     int
	curBatchIndex int

	records []*kgo.Record
	m       sync.Mutex
}

func newBatchAcker(client Client, batchSize int) *batchAcker {
	return &batchAcker{
		client:        client,
		batchSize:     batchSize,
		curBatchIndex: 0,
		records:       make([]*kgo.Record, 0, 10000), // prepare generous capacity
	}
}

func (a *batchAcker) Records(recs ...*kgo.Record) {
	a.m.Lock()
	a.records = append(a.records, recs...)
	a.m.Unlock()
}

func (a *batchAcker) Ack(ctx context.Context) error {
	a.curBatchIndex++
	if a.curBatchIndex < a.batchSize {
		return nil
	}
	// TODO flush on timeout
	sdk.Logger(ctx).Debug().Msgf("acked on batch index: %d", a.curBatchIndex)
	return a.Flush(ctx)
}

func (a *batchAcker) Flush(ctx context.Context) error {
	if a.curBatchIndex == 0 {
		return nil // nothing to flush
	}

	a.m.Lock()
	defer a.m.Unlock()

	sdk.Logger(ctx).Debug().Msgf("flushing from beginning to batch index %d.", a.curBatchIndex)
	err := a.client.CommitRecords(ctx, a.records[:a.curBatchIndex]...)
	if err != nil {
		return fmt.Errorf("failed to commit records: %w", err)
	}

	a.records = a.records[a.curBatchIndex:]
	a.curBatchIndex = 0
	return nil
}
