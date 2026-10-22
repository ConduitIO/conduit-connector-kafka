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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/mirror"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/kafkaconnect"
	"github.com/goccy/go-json"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type FranzProducer struct {
	client     *kgo.Client
	keyEncoder dataEncoder

	// getTopic is a function that returns the topic for a record. If nil, the
	// producer will use the default topic. This function is not safe for
	// concurrent use.
	getTopic func(opencdc.Record) (string, error)
	// defaultTopic is the static topic from the config, used when getTopic is
	// nil (a non-templated topic). Mirror mode needs it explicitly to check
	// the partition count and set Record.Topic, because it cannot rely on the
	// client's DefaultProduceTopic alone.
	defaultTopic string

	// Mirror mode options (see the kafka mirror passthrough design in
	// ConduitIO/conduit).
	mirror            bool
	allowLossyHeaders bool
	produceTimeout    time.Duration

	// partitions caches the destination partition count per topic in mirror
	// mode (see checkMirrorPartition). Guarded by partitionsMu.
	partitionsMu sync.Mutex
	partitions   map[string]int32

	// closeOnce, poisoned and closed implement the bounded produce wait and
	// make Close idempotent and safe against a concurrent Produce (see
	// produceMirror). closeOnce runs Close exactly once; poisoned stops new
	// produces; closed is closed inside the same Once so a waiter can stop
	// waiting the moment the client is closed.
	closeOnce sync.Once
	poisoned  atomic.Bool
	closed    chan struct{}

	// postEnqueueErrors counts produce errors reported after enqueue (see the
	// mirror design, Ordering). It is a structured log field, not a Prometheus
	// metric: the SDK has no connector metrics API yet.
	postEnqueueErrors atomic.Uint64
}

var _ Producer = (*FranzProducer)(nil)

// settleTimeout bounds how long produceMirror waits for callbacks after it has
// closed the client. The close resolves every enqueued promise at once; only a
// Produce that lost the race with Close never calls back, so the wait is short.
const settleTimeout = 5 * time.Second

// errNotResolved marks a produce slot whose callback has not run. It must not
// be nil, so a slot that was never confirmed is nacked, never acked.
var errNotResolved = errors.New("produce result not resolved")

var (
	errProduceTimeout   = errors.New("produceTimeout exceeded")
	errClientClosed     = errors.New("producer closed")
	errProducerPoisoned = errors.New("producer poisoned")
)

func NewFranzProducer(ctx context.Context, cfg Config) (*FranzProducer, error) {
	topic, topicFn, err := cfg.ParseTopic()
	if err != nil {
		// Unlikely to happen, as the topic is validated in the config.
		return nil, fmt.Errorf("failed to parse topic: %w", err)
	}

	// Host dialer (if any) is read from ctx and installed on the client.
	opts := cfg.FranzClientOpts(ctx, sdk.Logger(ctx))
	opts = append(opts, []kgo.Opt{
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.RequiredAcks(cfg.RequiredAcks()),
		kgo.ProducerBatchCompression(cfg.CompressionCodecs()...),
		kgo.ProducerBatchMaxBytes(cfg.BatchBytes),
		kgo.DefaultProduceTopic(topic),
	}...)

	if cfg.Mirror {
		// Mirror mode writes to the partition named by kafka.partition, so it
		// uses the manual partitioner, and never auto-creates a topic: a
		// broker-default topic usually has the wrong partition count.
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	} else {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if cfg.RequiredAcks() != kgo.AllISRAcks() {
		sdk.Logger(ctx).Warn().Msgf("disabling idempotent writes because \"acks\" is set to %v", cfg.Acks)
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	var keyEncoder dataEncoder = bytesEncoder{}
	if cfg.useKafkaConnectKeyFormat {
		keyEncoder = kafkaConnectEncoder{}
	}

	return &FranzProducer{
		client:            cl,
		keyEncoder:        keyEncoder,
		getTopic:          topicFn,
		defaultTopic:      topic,
		mirror:            cfg.Mirror,
		allowLossyHeaders: cfg.AllowLossyHeaders,
		produceTimeout:    cfg.ProduceTimeout,
		partitions:        make(map[string]int32),
		closed:            make(chan struct{}),
	}, nil
}

func (p *FranzProducer) Produce(ctx context.Context, records []opencdc.Record) (int, error) {
	if p.mirror {
		return p.produceMirror(ctx, records)
	}

	if len(records) == 1 {
		// Fast path for a single record.
		rec, err := p.prepareRecord(ctx, records[0])
		if err != nil {
			return 0, fmt.Errorf("failed to prepare record: %w", err)
		}
		_, err = p.client.ProduceSync(ctx, rec).First()
		if err != nil {
			return 0, fmt.Errorf("failed to produce record: %w", err)
		}
		return 1, nil
	}

	var (
		wg csync.WaitGroup
		// results[i] is the delivery outcome of records[i]. Each promise
		// writes only its own slot, so there is no shared append, and the
		// slice stays in record order regardless of the order in which the
		// broker completes the promises.
		results         = make([]error, len(records))
		produced        int // number of records handed to the client
		rec             *kgo.Record
		prepareErr      error
		prepareErrIndex = -1
	)

	for i, r := range records {
		rec, prepareErr = p.prepareRecord(ctx, r)
		if prepareErr != nil {
			prepareErrIndex = i
			prepareErr = fmt.Errorf("failed to prepare record: %w", prepareErr)
			break
		}

		wg.Add(1)
		produced++
		p.client.Produce(
			ctx,
			rec,
			func(_ *kgo.Record, err error) {
				results[i] = err
				wg.Done()
			},
		)
	}

	// Everything is enqueued: stop lingering so the batch is sent now instead
	// of after ProducerLinger (see unlinger).
	p.unlinger(ctx)

	err := wg.Wait(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to wait for all records to be produced: %w", err)
	}

	// Invariant 1: the returned count n means "records[:n] are durably
	// written" and the SDK acks exactly that prefix. n must therefore be the
	// index of the first failed record in record order, never a position in
	// completion order.
	for i, err := range results[:produced] {
		if err != nil {
			return i, fmt.Errorf("failed to produce record %v: %w", i, err)
		}
	}

	if prepareErr != nil {
		// We failed to prepare a record, return the error and the index of the
		// record that failed.
		return prepareErrIndex, prepareErr
	}

	return produced, nil
}

// produceMirror writes records in mirror mode: it preserves each record's
// partition, headers, timestamp, key and value, and bounds the produce wait by
// produceTimeout (deliveryTimeout must stay unset in mirror mode, so there is
// no other bound). A timeout, a context cancellation or a concurrent Close all
// end the wait; the producer is then poisoned and closed, and every record
// whose promise did not resolve with nil is nacked, so a record is never acked
// unless the broker confirmed it (Invariant 1).
func (p *FranzProducer) produceMirror(ctx context.Context, records []opencdc.Record) (int, error) {
	if p.poisoned.Load() {
		return 0, fmt.Errorf("%w: the producer is closed after a produce timeout", errProducerPoisoned)
	}

	prepared := make([]*kgo.Record, len(records))
	for i, r := range records {
		rec, err := p.prepareRecord(ctx, r)
		if err != nil {
			return i, fmt.Errorf("failed to prepare record: %w", err)
		}
		prepared[i] = rec
	}

	results := make([]error, len(records))
	for i := range results {
		results[i] = errNotResolved
	}

	var wg csync.WaitGroup
	for i, rec := range prepared {
		wg.Add(1)
		p.client.Produce(ctx, rec, func(r *kgo.Record, err error) {
			results[i] = err
			if err != nil {
				p.recordPostEnqueueError(ctx, r, err)
			}
			wg.Done()
		})
	}

	// Stop lingering so the batch is sent now instead of after ProducerLinger.
	// Flush returns once every buffered record is resolved or flushCtx ends;
	// the wait below stays the sole judge of success, timeout and close, so
	// the produceTimeout and poison semantics are unchanged.
	flushCtx, stopFlush := context.WithCancel(ctx)
	defer stopFlush()
	go p.unlinger(flushCtx)

	waitErr := p.wait(ctx, &wg)
	if waitErr != nil {
		// The wait ended without every promise resolving. Poison and close so
		// the remaining promises resolve at once, then nack from the first
		// record that did not confirm. A record whose Produce lost the race
		// with Close never calls back, so the settle wait is bounded.
		p.poisoned.Store(true)
		p.closeOnce.Do(func() {
			close(p.closed)
			p.client.Close()
		})
		// The close fails every enqueued promise with "client closed" and
		// fires its callback, so wg reaches zero as soon as all enqueued
		// callbacks have run. Waiting establishes a happens-before edge that
		// makes reading results race-free. Orphans (a Produce that lost the
		// race with Close) never call back, so the wait is bounded and, if it
		// times out, nothing is read and every record is nacked.
		if err := wg.WaitTimeout(context.Background(), settleTimeout); err == nil {
			n := firstNotConfirmed(results)
			return n, p.produceWaitError(waitErr)
		}
		return 0, p.produceWaitError(waitErr)
	}

	n := firstNotConfirmed(results)
	if n < len(results) {
		return n, fmt.Errorf("failed to produce record %d: %w", n, results[n])
	}
	return len(results), nil
}

// unlinger makes the client send everything enqueued so far without waiting
// out ProducerLinger (franz-go defaults it to 10ms since v1.20). Without it a
// batched Write costs a full linger however small the batch, which caps
// throughput at batch/linger. Flush, unlike ProduceSync, also covers a topic
// whose metadata is not loaded yet (the first Write) and does not stop
// ctx-aware waiting in the caller. It only changes when records are sent, not
// which records, in what order, or how they are acked: the per-record
// promises remain the only source of delivery outcome (Invariant 1), so its
// error is deliberately ignored; ctx ending is reported by the caller's wait.
func (p *FranzProducer) unlinger(ctx context.Context) {
	_ = p.client.Flush(ctx)
}

// wait blocks until every callback has run, produceTimeout expires, the caller
// context is done, or the producer is closed.
func (p *FranzProducer) wait(ctx context.Context, wg *csync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		(*sync.WaitGroup)(wg).Wait()
		close(done)
	}()

	timer := time.NewTimer(p.produceTimeout)
	defer timer.Stop()

	select {
	case <-done:
		return nil
	case <-timer.C:
		return errProduceTimeout
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return errClientClosed
	}
}

func (p *FranzProducer) produceWaitError(waitErr error) error {
	if errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
		return fmt.Errorf("produce wait canceled: %w", waitErr)
	}
	return fmt.Errorf("%w: %v", ErrCodeProduceTimeout, waitErr)
}

// firstNotConfirmed returns the index of the first record whose result is not
// nil. A nil result is the broker's confirmation; anything else (a produce
// error, "client closed", or the errNotResolved marker for a record whose
// promise never ran) is not confirmed and must be nacked.
func firstNotConfirmed(results []error) int {
	for i, err := range results {
		if err != nil {
			return i
		}
	}
	return len(results)
}

// recordPostEnqueueError reports a produce error that the broker or client
// returned after the record was enqueued. Per the mirror design, this voids the
// per-partition ordering guarantee for that destination partition (later
// records of the batch may already be stored), so it is a WARN with the loud
// metric name, not a debug log.
func (p *FranzProducer) recordPostEnqueueError(ctx context.Context, r *kgo.Record, err error) {
	total := p.postEnqueueErrors.Add(1)
	topic, partition := "", int32(0)
	if r != nil {
		topic, partition = r.Topic, r.Partition
	}
	sdk.Logger(ctx).Warn().
		Err(err).
		Str("topic", topic).
		Int32("partition", partition).
		Uint64(MetricPostEnqueueErrors, total).
		Msg("a produce error was reported after enqueue; per-partition order is no longer guaranteed " +
			"for this destination partition, re-seed it")
}

func (p *FranzProducer) prepareRecord(ctx context.Context, r opencdc.Record) (*kgo.Record, error) {
	if p.mirror {
		return p.prepareMirrorRecord(ctx, r)
	}

	encodedKey, err := p.keyEncoder.Encode(r.Key)
	if err != nil {
		return nil, fmt.Errorf("could not encode key: %w", err)
	}

	var topic string
	if p.getTopic != nil {
		topic, err = p.getTopic(r)
		if err != nil {
			return nil, fmt.Errorf("could not get topic: %w", err)
		}
	}
	return &kgo.Record{
		Key:   encodedKey,
		Value: r.Bytes(),
		Topic: topic,
	}, nil
}

// prepareMirrorRecord builds the Kafka record from the mirror metadata and
// checks the destination partition count.
func (p *FranzProducer) prepareMirrorRecord(ctx context.Context, r opencdc.Record) (*kgo.Record, error) {
	rec, err := buildMirrorRecord(r, p.allowLossyHeaders)
	if err != nil {
		return nil, err
	}

	if p.getTopic != nil {
		rec.Topic, err = p.getTopic(r)
		if err != nil {
			return nil, fmt.Errorf("could not get topic: %w", err)
		}
	} else {
		rec.Topic = p.defaultTopic
	}

	if err := p.checkMirrorPartition(ctx, r, rec.Topic, rec.Partition); err != nil {
		return nil, err
	}
	return rec, nil
}

// checkMirrorPartition enforces the partition-count rules from the mirror
// design: the destination must have at least as many partitions as the source
// (fail-fast on the first record, KAFKA_PARTITION_COUNT_MISMATCH), and the
// record's partition must exist (KAFKA_PARTITION_OUT_OF_RANGE, after one
// metadata refresh in case the topic was grown).
func (p *FranzProducer) checkMirrorPartition(ctx context.Context, r opencdc.Record, topic string, partition int32) error {
	count, err := p.destinationPartitionCount(ctx, topic)
	if err != nil {
		return err
	}

	if s, ok := r.Metadata[mirror.Partitions]; ok {
		if src, err := mirror.ParsePartition(s); err == nil && src > count {
			return fmt.Errorf("%w: source topic has %d partitions but destination %q has %d",
				ErrCodePartitionCountMismatch, src, topic, count)
		}
	}

	if partition >= count {
		// Refresh once: the topic may have been grown while the pipeline runs.
		if fresh, err := p.refreshPartitionCount(ctx, topic); err == nil {
			count = fresh
		}
		if partition >= count {
			return fmt.Errorf("%w: partition %d is out of range for topic %q with %d partitions",
				ErrCodePartitionOutOfRange, partition, topic, count)
		}
	}
	return nil
}

func (p *FranzProducer) destinationPartitionCount(ctx context.Context, topic string) (int32, error) {
	p.partitionsMu.Lock()
	if c, ok := p.partitions[topic]; ok && c > 0 {
		p.partitionsMu.Unlock()
		return c, nil
	}
	p.partitionsMu.Unlock()
	return p.refreshPartitionCount(ctx, topic)
}

func (p *FranzProducer) refreshPartitionCount(ctx context.Context, topic string) (int32, error) {
	meta, err := kadm.NewClient(p.client).Metadata(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("failed to read metadata for topic %q: %w", topic, err)
	}
	td, ok := meta.Topics[topic]
	if !ok || td.Err != nil {
		if td.Err == nil {
			td.Err = kerr.UnknownTopicOrPartition
		}
		if errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
			return 0, fmt.Errorf("%w: topic %q does not exist", ErrCodeDestinationTopicMissing, topic)
		}
		return 0, fmt.Errorf("failed to read metadata for topic %q: %w", topic, td.Err)
	}

	// Kafka partition IDs are int32, so a topic cannot have more partitions
	// than fit in an int32.
	count := int32(len(td.Partitions)) //nolint:gosec // partition count fits int32 by protocol
	p.partitionsMu.Lock()
	p.partitions[topic] = count
	p.partitionsMu.Unlock()
	return count, nil
}

func (p *FranzProducer) Close(_ context.Context) error {
	p.poisoned.Store(true)
	p.closeOnce.Do(func() {
		close(p.closed)
		if p.client != nil {
			p.client.Close()
		}
	})
	return nil
}

// dataEncoder is similar to a sdk.Encoder, which takes data and encodes it in
// a certain format. The producer uses this to encode the key of the kafka
// message.
type dataEncoder interface {
	Encode(opencdc.Data) ([]byte, error)
}

// bytesEncoder is a dataEncoder that simply calls data.Bytes().
type bytesEncoder struct{}

func (bytesEncoder) Encode(data opencdc.Data) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	return data.Bytes(), nil
}

// kafkaConnectEncoder encodes the data into a kafka connect JSON with schema
// (NB: this is not the same as JSONSchema).
type kafkaConnectEncoder struct{}

func (e kafkaConnectEncoder) Encode(data opencdc.Data) ([]byte, error) {
	sd := e.toStructuredData(data)
	schema := kafkaconnect.Reflect(sd)
	if schema == nil {
		// s is nil, let's write an empty struct in the schema
		schema = &kafkaconnect.Schema{
			Type:     kafkaconnect.TypeStruct,
			Optional: true,
		}
	}

	env := kafkaconnect.Envelope{
		Schema:  *schema,
		Payload: sd,
	}
	// TODO add support for other encodings than JSON
	return json.Marshal(env)
}

// toStructuredData tries its best to return StructuredData.
func (kafkaConnectEncoder) toStructuredData(d opencdc.Data) opencdc.Data {
	switch d := d.(type) {
	case nil:
		return nil
	case opencdc.StructuredData:
		return d
	case opencdc.RawData:
		// try parsing the raw data as json
		var sd opencdc.StructuredData
		err := json.Unmarshal(d, &sd)
		if err != nil {
			// it's not JSON, nothing more we can do
			return d
		}
		return sd
	default:
		// should not be possible
		panic(fmt.Errorf("unknown data type: %T", d))
	}
}
