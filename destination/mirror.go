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

package destination

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/mirror"
	"github.com/twmb/franz-go/pkg/kgo"
)

// buildMirrorRecord builds the Kafka record to write from the mirror metadata,
// preserving key, value, headers, timestamp and partition verbatim. It fails
// with a stable code rather than coercing, so nothing lossy happens silently.
func buildMirrorRecord(r opencdc.Record, allowLossyHeaders bool) (*kgo.Record, error) {
	partition, err := mirrorPartition(r.Metadata)
	if err != nil {
		return nil, err
	}
	headers, err := mirrorHeaders(r, allowLossyHeaders)
	if err != nil {
		return nil, err
	}
	timestamp, err := mirrorTimestamp(r.Metadata)
	if err != nil {
		return nil, err
	}
	key, err := mirrorKey(r)
	if err != nil {
		return nil, err
	}
	value, err := mirrorValue(r)
	if err != nil {
		return nil, err
	}

	return &kgo.Record{
		Key:       key,
		Value:     value,
		Headers:   headers,
		Timestamp: timestamp,
		Partition: partition,
	}, nil
}

func mirrorPartition(metadata opencdc.Metadata) (int32, error) {
	s, ok := metadata[mirror.Partition]
	if !ok || s == "" {
		return 0, fmt.Errorf("%w: %s is missing; set emitMirrorMetadata on the source",
			ErrCodePartitionInvalid, mirror.Partition)
	}
	p, err := mirror.ParsePartition(s)
	if err != nil {
		return 0, fmt.Errorf("%w: %s=%q: %v", ErrCodePartitionInvalid, mirror.Partition, s, err)
	}
	return p, nil
}

func mirrorHeaders(r opencdc.Record, allowLossy bool) ([]kgo.RecordHeader, error) {
	s, ok := r.Metadata[mirror.Headers]
	if !ok {
		if allowLossy {
			return lossyHeaders(r.Metadata), nil
		}
		return nil, fmt.Errorf("%w: %s is missing; set emitMirrorMetadata on the source or allowLossyHeaders",
			ErrCodeHeadersMissing, mirror.Headers)
	}
	h, err := mirror.DecodeHeaders(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCodeHeadersInvalid, err)
	}
	return h, nil
}

// lossyHeaders rebuilds headers from the kafka.header.<k> metadata entries.
// Repeated keys are collapsed and order is not preserved; this path is used
// only under the explicit allowLossyHeaders escape hatch.
func lossyHeaders(metadata opencdc.Metadata) []kgo.RecordHeader {
	var keys []string
	for k := range metadata {
		if strings.HasPrefix(k, mirror.HeaderPrefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	out := make([]kgo.RecordHeader, 0, len(keys))
	for _, k := range keys {
		out = append(out, kgo.RecordHeader{
			Key:   strings.TrimPrefix(k, mirror.HeaderPrefix),
			Value: []byte(metadata[k]),
		})
	}
	return out
}

func mirrorTimestamp(metadata opencdc.Metadata) (time.Time, error) {
	s, ok := metadata[mirror.Timestamp]
	if !ok || s == "" {
		return time.Time{}, fmt.Errorf("%w: %s is missing; set emitMirrorMetadata on the source",
			ErrCodeTimestampInvalid, mirror.Timestamp)
	}
	t, err := mirror.ParseTimestamp(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s=%q: %v", ErrCodeTimestampInvalid, mirror.Timestamp, s, err)
	}
	return t, nil
}

func mirrorKey(r opencdc.Record) ([]byte, error) {
	raw, err := rawBytes(r.Key)
	if err != nil {
		return nil, fmt.Errorf("%w: key: %v", ErrCodeMirrorPayloadNotRaw, err)
	}

	marker, present := r.Metadata[mirror.KeyNull]
	switch {
	case !present:
		if len(raw) == 0 {
			return nil, fmt.Errorf("%w: %s is absent and the key is empty",
				ErrCodeMirrorKeyNullnessUnknown, mirror.KeyNull)
		}
		return raw, nil
	default:
		isNull, err := mirror.ParseNullMarker(marker)
		if err != nil {
			return nil, fmt.Errorf("%w: %s=%q: %v", ErrCodeMirrorKeyConflict, mirror.KeyNull, marker, err)
		}
		if isNull {
			if len(raw) != 0 {
				return nil, fmt.Errorf("%w: %s is true but the key is non-empty",
					ErrCodeMirrorKeyConflict, mirror.KeyNull)
			}
			return nil, nil
		}
		// Non-null: an empty key is written as an empty non-null key, never as
		// a null key (which would change its meaning on a compacted topic).
		if len(raw) == 0 {
			return []byte{}, nil
		}
		return raw, nil
	}
}

func mirrorValue(r opencdc.Record) ([]byte, error) {
	raw, err := rawBytes(r.Payload.After)
	if err != nil {
		return nil, fmt.Errorf("%w: payload: %v", ErrCodeMirrorPayloadNotRaw, err)
	}

	marker, present := r.Metadata[mirror.ValueNull]
	switch {
	case !present:
		if len(raw) == 0 {
			return nil, fmt.Errorf("%w: %s is absent and the payload is empty",
				ErrCodeMirrorValueNullnessUnknwn, mirror.ValueNull)
		}
		return raw, nil
	default:
		isNull, err := mirror.ParseNullMarker(marker)
		if err != nil {
			return nil, fmt.Errorf("%w: %s=%q: %v", ErrCodeMirrorValueConflict, mirror.ValueNull, marker, err)
		}
		if isNull {
			if len(raw) != 0 {
				return nil, fmt.Errorf("%w: %s is true but the payload is non-empty",
					ErrCodeMirrorValueConflict, mirror.ValueNull)
			}
			return nil, nil // tombstone
		}
		// Non-null: an empty value is written as an empty non-null value, never
		// as a tombstone (which would delete the key on a compacted topic).
		if len(raw) == 0 {
			return []byte{}, nil
		}
		return raw, nil
	}
}

// rawBytes returns the bytes of data, which must be opencdc.RawData (or nil).
// Structured data is rejected: a mirror must not re-serialize.
func rawBytes(d opencdc.Data) ([]byte, error) {
	if d == nil {
		return nil, nil
	}
	raw, ok := d.(opencdc.RawData)
	if !ok {
		return nil, fmt.Errorf("data is %T, not raw; disable sdk.schema.extract.*", d)
	}
	return []byte(raw), nil
}
