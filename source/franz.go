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

//go:generate mockgen -typed -destination franz_mock.go -package source -mock_names=Client=MockClient . Client

package source

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ackBatchSize = 1000
	ackBatchTime = 5 * time.Second
)

type FranzConsumer struct {
	client Client
	acker  *batchAcker

	iter *kgo.FetchesRecordIter

	retryGroupJoinErrors bool
}

// Client is a franz-go kafka client.
type Client interface {
	Close()
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	OptValue(opt any) any
	PollFetches(ctx context.Context) kgo.Fetches
}

var _ Consumer = (*FranzConsumer)(nil)

func NewFranzConsumer(ctx context.Context, cfg Config) (*FranzConsumer, error) {
	opts := cfg.FranzClientOpts(sdk.Logger(ctx))
	opts = append(opts, []kgo.Opt{
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
	}...)

	if !cfg.ReadFromBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	if cfg.GroupID != "" {
		opts = append(opts, kgo.DisableAutoCommit()) // TODO research if we need to add OnPartitionsRevoked (see DisableAutoCommit doc)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	consumer := &FranzConsumer{
		client:               cl,
		acker:                newBatchAcker(cl, ackBatchSize),
		iter:                 &kgo.FetchesRecordIter{}, // empty iterator is done
		retryGroupJoinErrors: cfg.RetryGroupJoinErrors,
	}

	go consumer.scheduleFlushing(ctx)

	return consumer, nil
}

func (c *FranzConsumer) scheduleFlushing(ctx context.Context) {
	ticker := time.Tick(ackBatchTime)

	for {
		select {
		case <-ctx.Done():
			sdk.Logger(ctx).Debug().Err(ctx.Err()).
				Msg("FranzConsumer context done, exiting scheduleFlushing goroutine")
			return
		case <-ticker:
			err := c.acker.Flush(ctx)
			if err != nil {
				sdk.Logger(ctx).Warn().Err(err).Msg("failed to flush acks")
			}
		}
	}
}

func (c *FranzConsumer) Consume(ctx context.Context) (*Record, error) {
	for c.iter.Done() {
		fetches := c.client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			var errGroupSession *kgo.ErrGroupSession
			if c.retryGroupJoinErrors &&
				(errors.As(err, &errGroupSession) || strings.Contains(err.Error(), "unable to join group session")) {
				sdk.Logger(ctx).Warn().Err(err).Msgf("group session error, retrying")
				return nil, sdk.ErrBackoffRetry
			}

			return nil, err
		}
		if fetches.Empty() {
			continue // no records, keep polling
		}
		c.iter = fetches.RecordIter()
		c.acker.Records(fetches.Records()...)
		break
	}
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
	// m is used to synchronize access to records and curBatchIndex
	m sync.Mutex
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
	a.m.Lock()
	a.curBatchIndex++
	curBatchIndex := a.curBatchIndex
	a.m.Unlock()

	if curBatchIndex < a.batchSize {
		return nil
	}
	return a.Flush(ctx)
}

func (a *batchAcker) Flush(ctx context.Context) error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.curBatchIndex == 0 {
		return nil // nothing to flush
	}

	err := a.client.CommitRecords(ctx, a.records[:a.curBatchIndex]...)
	if err != nil {
		return fmt.Errorf("failed to commit records: %w", err)
	}

	a.records = a.records[a.curBatchIndex:]
	a.curBatchIndex = 0
	return nil
}
