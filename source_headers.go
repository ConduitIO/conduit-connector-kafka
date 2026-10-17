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
	"encoding/base64"
	"sync"
	"unicode/utf8"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// CodeLegacyHeaderNotUTF8 is the stable code of the warning logged when a
	// Kafka header's key or value is not valid UTF-8, so its
	// kafka.header.<key> metadata entry is not valid UTF-8 either. Warning
	// only; the record is emitted unchanged.
	CodeLegacyHeaderNotUTF8 = "KAFKA_LEGACY_HEADER_NOT_UTF8"

	// MetricLegacyHeaderNotUTF8 names the count of such entries. The SDK has no
	// connector metrics API yet, so it is reported as a structured log field
	// with this name (see the kafka mirror passthrough design in ConduitIO/conduit).
	MetricLegacyHeaderNotUTF8 = "kafka_source_legacy_header_not_utf8_total"

	// CodeMirrorLegacyHeaderOmitted is the stable code of the warning logged
	// when emitMirrorMetadata omits a kafka.header.<k> entry because the header
	// key or value is not valid UTF-8. The raw bytes stay in kafka.headers.
	CodeMirrorLegacyHeaderOmitted = "KAFKA_LEGACY_HEADER_OMITTED"

	// MetricMirrorLegacyHeaderOmitted names the count of entries omitted when
	// emitMirrorMetadata is on, reported as a structured log field.
	MetricMirrorLegacyHeaderOmitted = "kafka_source_legacy_header_omitted_total"
)

// maxWarnedHeaderKeys bounds the keys warned about individually, so a topic
// whose binary header keys differ in every record can neither flood the log nor
// grow the set without limit. Keys past it are still counted.
const maxWarnedHeaderKeys = 100

// Values of the "invalid" log field: which part of the header is not UTF-8.
const (
	partKey   = "key"
	partValue = "value"
)

// nonUTF8HeaderTracker reports kafka.header.<key> metadata entries whose key or
// value is not valid UTF-8 (issue #319).
//
// OpenCDC metadata is a map[string]string, and a protobuf string must be valid
// UTF-8, so such a record cannot be marshalled as opencdc.v1.Record: it fails
// on every path that does (a standalone connector's gRPC hop, a standalone
// processor, the API). Encoders that put metadata through JSON replace the
// invalid bytes with U+FFFD instead, silently.
//
// The entries are not changed. Escaping or replacing the value would change
// the data a consumer sees without marking it (invariant 6), and omitting the
// entry changes the default output, which needs the deprecation policy
// (announce, warn, remove over two minor versions). This is the warning step:
// a WARN once per header key per source run, and a running count.
//
// A header key that repeats in one record is judged on the value that won (the
// last), which is the one in the metadata. The key is logged as base64 of its
// bytes in header_key_b64, never as a raw string, since a JSON log encoder
// would mangle a key that is not valid UTF-8; the value is never logged.
//
// Safe for concurrent use: Read and Teardown may run on different goroutines.
type nonUTF8HeaderTracker struct {
	mu     sync.Mutex
	warned map[string]bool // by header key
	total  uint64
}

func (t *nonUTF8HeaderTracker) observe(ctx context.Context, rec *source.Record, metadata opencdc.Metadata) {
	if len(rec.Headers) == 0 {
		return
	}
	var seen map[string]bool
	for _, h := range rec.Headers {
		if seen[h.Key] {
			continue // judged once, on the value that won
		}
		value := metadata[MetadataKafkaHeaderPrefix+h.Key]
		keyOK, valueOK := utf8.ValidString(h.Key), utf8.ValidString(value)
		if keyOK && valueOK {
			continue
		}
		if seen == nil {
			seen = make(map[string]bool, len(rec.Headers))
		}
		seen[h.Key] = true
		t.record(ctx, rec, h.Key, keyOK)
	}
}

func (t *nonUTF8HeaderTracker) record(ctx context.Context, rec *source.Record, key string, keyOK bool) {
	t.mu.Lock()
	t.total++
	total := t.total
	first := !t.warned[key] && len(t.warned) < maxWarnedHeaderKeys
	if first {
		if t.warned == nil {
			t.warned = make(map[string]bool)
		}
		t.warned[key] = true
	}
	t.mu.Unlock()

	if !first {
		return
	}
	part := partValue
	if !keyOK {
		part = partKey
	}
	sdk.Logger(ctx).Warn().
		Str("code", CodeLegacyHeaderNotUTF8).
		Str("topic", rec.Topic).
		Int32("partition", rec.Partition).
		Int64("offset", rec.Offset).
		Str("header_key_b64", base64.StdEncoding.EncodeToString([]byte(key))).
		Str("invalid", part).
		Uint64(MetricLegacyHeaderNotUTF8, total).
		Msg("a Kafka header's " + part + " is not valid UTF-8, and is emitted unchanged in its " +
			"kafka.header.<key> metadata entry. A record carrying it cannot be marshalled as an OpenCDC " +
			"protobuf record (it fails in standalone connectors and processors and in the API), and JSON encoders " +
			"replace the invalid bytes with U+FFFD. A future version will omit such entries by default " +
			"(conduit-connector-kafka#319). Logged once per header key; later ones are counted")
}

// logTotal logs the count once, at teardown, if any entry was seen.
func (t *nonUTF8HeaderTracker) logTotal(ctx context.Context) {
	t.mu.Lock()
	total, keys := t.total, len(t.warned) // keys: at most maxWarnedHeaderKeys
	t.mu.Unlock()
	if total == 0 {
		return
	}
	sdk.Logger(ctx).Warn().
		Str("code", CodeLegacyHeaderNotUTF8).
		Uint64(MetricLegacyHeaderNotUTF8, total).
		Int("header_keys_warned", keys).
		Msg("kafka.header.<key> metadata entries that are not valid UTF-8 were emitted during this run")
}

// mirrorHeaderOmittedTracker counts kafka.header.<k> entries the source omits
// when emitMirrorMetadata is on, and warns once per header key. It is a
// separate type from nonUTF8HeaderTracker because the metric and the message
// differ: here the entry is omitted (the raw bytes stay in kafka.headers), not
// emitted unchanged.
type mirrorHeaderOmittedTracker struct {
	mu     sync.Mutex
	warned map[string]bool // by header key
	total  uint64
}

func (t *mirrorHeaderOmittedTracker) record(ctx context.Context, rec *source.Record, key string, keyOK bool) {
	t.mu.Lock()
	t.total++
	total := t.total
	first := !t.warned[key] && len(t.warned) < maxWarnedHeaderKeys
	if first {
		if t.warned == nil {
			t.warned = make(map[string]bool)
		}
		t.warned[key] = true
	}
	t.mu.Unlock()

	if !first {
		return
	}
	part := partValue
	if !keyOK {
		part = partKey
	}
	sdk.Logger(ctx).Warn().
		Str("code", CodeMirrorLegacyHeaderOmitted).
		Str("topic", rec.Topic).
		Int32("partition", rec.Partition).
		Int64("offset", rec.Offset).
		Str("header_key_b64", base64.StdEncoding.EncodeToString([]byte(key))).
		Str("invalid", part).
		Uint64(MetricMirrorLegacyHeaderOmitted, total).
		Msg("a Kafka header's " + part + " is not valid UTF-8, so its kafka.header.<key> entry is omitted " +
			"while emitMirrorMetadata is on; the raw bytes stay in kafka.headers")
}
