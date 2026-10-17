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

// Package mirror holds the shared lossless Kafka-to-Kafka mirror metadata
// format: the metadata key names, the strict codec for kafka.headers, and the
// parsers for the other keys. The source writes the keys and the destination
// reads them, so the format is defined once here. See the kafka mirror
// passthrough design in ConduitIO/conduit.
package mirror

import (
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Metadata keys. The source emits these only when emitMirrorMetadata is on,
// and the destination reads them only when mirror is on.
const (
	Partition     = "kafka.partition"
	Partitions    = "kafka.partitions"
	Offset        = "kafka.offset"
	Timestamp     = "kafka.timestamp"
	TimestampType = "kafka.timestamp.type"
	Headers       = "kafka.headers"
	KeyNull       = "kafka.key.null"
	ValueNull     = "kafka.value.null"

	// HeaderPrefix is the prefix of the lossy kafka.header.<key> metadata
	// entries, which the destination reads only under allowLossyHeaders.
	HeaderPrefix = "kafka.header."
)

// header is one element of the kafka.headers JSON array. K is base64 of the
// header key bytes; V is base64 of the header value bytes, or nil for a null
// header value. Pointers distinguish a missing field (nil) from a field whose
// base64 encoding is the empty string (an empty key or value).
type header struct {
	K *string `json:"k"`
	V *string `json:"v"`
}

// EncodeHeaders encodes headers into the kafka.headers JSON value. The order is
// preserved and repeated keys are separate elements. A null header value
// encodes as JSON null; an empty one as the empty string. It panics only on
// conditions that cannot happen (base64 of bytes and JSON marshaling of strings
// cannot fail).
func EncodeHeaders(headers []kgo.RecordHeader) string {
	out := make([]header, len(headers))
	for i, h := range headers {
		k := base64.StdEncoding.EncodeToString([]byte(h.Key))
		out[i].K = &k
		if h.Value != nil {
			v := base64.StdEncoding.EncodeToString(h.Value)
			out[i].V = &v
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		panic(fmt.Errorf("error marshaling kafka.headers: %w", err))
	}
	return string(b)
}

// DecodeHeaders decodes the kafka.headers JSON value strictly. Malformed input
// fails: invalid JSON, unknown fields, a missing "k", a non-string "k"/"v"
// (other than a null "v"), base64 that is not standard padded, and any trailing
// content after the array. A null "v" decodes to a nil header value; the empty
// string decodes to an empty non-nil value.
func DecodeHeaders(s string) ([]kgo.RecordHeader, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.DisallowUnknownFields()

	var hs []header
	if err := dec.Decode(&hs); err != nil {
		return nil, fmt.Errorf("invalid kafka.headers: %w", err)
	}
	// The second Decode must find nothing: it rejects trailing data after the
	// array, which Decode alone would leave unread.
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("invalid kafka.headers: trailing data after the array")
	}

	out := make([]kgo.RecordHeader, len(hs))
	for i, h := range hs {
		if h.K == nil {
			return nil, fmt.Errorf("invalid kafka.headers: element %d has no \"k\"", i)
		}
		key, err := base64.StdEncoding.Strict().DecodeString(*h.K)
		if err != nil {
			return nil, fmt.Errorf("invalid kafka.headers: element %d key is not standard padded base64: %w", i, err)
		}
		out[i].Key = string(key)
		if h.V != nil {
			value, err := base64.StdEncoding.Strict().DecodeString(*h.V)
			if err != nil {
				return nil, fmt.Errorf("invalid kafka.headers: element %d value is not standard padded base64: %w", i, err)
			}
			out[i].Value = value
		}
	}
	return out, nil
}

// ParsePartition parses a decimal Kafka partition number.
func ParsePartition(s string) (int32, error) {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("not a decimal partition number: %w", err)
	}
	if n < 0 {
		return 0, fmt.Errorf("negative partition number")
	}
	return int32(n), nil
}

// ParseTimestamp parses kafka.timestamp, milliseconds since epoch.
func ParseTimestamp(s string) (time.Time, error) {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("not a decimal millisecond timestamp: %w", err)
	}
	return time.UnixMilli(ms), nil
}

// ParseNullMarker parses a kafka.key.null / kafka.value.null value.
func ParseNullMarker(s string) (bool, error) {
	switch s {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("not a boolean null marker")
	}
}
