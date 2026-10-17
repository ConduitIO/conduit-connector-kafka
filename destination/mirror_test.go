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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/conduitio/conduit-connector-kafka/mirror"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
)

func validMirrorMetadata() opencdc.Metadata {
	return opencdc.Metadata{
		mirror.Partition:     "2",
		mirror.Partitions:    "3",
		mirror.Offset:        "10",
		mirror.Timestamp:     "1760000000000",
		mirror.TimestampType: "create",
		mirror.Headers:       "[]",
		mirror.KeyNull:       "false",
		mirror.ValueNull:     "false",
	}
}

func TestBuildMirrorRecord(t *testing.T) {
	is := is.New(t)

	rec := opencdc.Record{
		Key:      opencdc.RawData("key"),
		Payload:  opencdc.Change{After: opencdc.RawData("value")},
		Metadata: validMirrorMetadata(),
	}

	got, err := buildMirrorRecord(rec, false)
	is.NoErr(err)
	is.Equal(got.Partition, int32(2))
	is.Equal(got.Key, []byte("key"))
	is.Equal(got.Value, []byte("value"))
	is.Equal(got.Timestamp, time.UnixMilli(1760000000000))
	is.Equal(got.Headers, []kgo.RecordHeader{})
}

func TestFranzProducer_MirrorOpts(t *testing.T) {
	is := is.New(t)

	cfg := Config{
		Config:         common.Config{Servers: []string{"test-host:9092"}},
		Topic:          "test-topic",
		Acks:           "all",
		BatchBytes:     512,
		Mirror:         true,
		ProduceTimeout: time.Minute,
	}
	p, err := NewFranzProducer(context.Background(), cfg)
	is.NoErr(err)

	// Mirror mode uses the manual partitioner and never auto-creates topics.
	is.True(p.client.OptValue(kgo.RecordPartitioner) != nil)
	is.Equal(p.client.OptValue(kgo.AllowAutoTopicCreation), false)
}

func TestBuildMirrorRecord_Headers(t *testing.T) {
	is := is.New(t)
	md := validMirrorMetadata()
	md[mirror.Headers] = mirror.EncodeHeaders([]kgo.RecordHeader{
		{Key: "a", Value: []byte("b")},
		{Key: "n", Value: nil},
	})

	rec := opencdc.Record{
		Key:      opencdc.RawData("key"),
		Payload:  opencdc.Change{After: opencdc.RawData("value")},
		Metadata: md,
	}
	got, err := buildMirrorRecord(rec, false)
	is.NoErr(err)
	is.Equal(got.Headers, []kgo.RecordHeader{
		{Key: "a", Value: []byte("b")},
		{Key: "n", Value: nil},
	})
}

func TestBuildMirrorRecord_KeyValueNullRules(t *testing.T) {
	cases := []struct {
		name      string
		key       opencdc.Data
		value     opencdc.Data
		keyNull   string
		valueNull string
		wantKey   []byte
		wantVal   []byte
		wantErr   error
	}{
		{
			name:      "null key and value",
			key:       opencdc.RawData(nil),
			value:     opencdc.RawData(nil),
			keyNull:   "true",
			valueNull: "true",
			wantKey:   nil,
			wantVal:   nil,
		},
		{
			name:      "empty non-null key and value",
			key:       opencdc.RawData(""),
			value:     opencdc.RawData(""),
			keyNull:   "false",
			valueNull: "false",
			wantKey:   []byte{},
			wantVal:   []byte{},
		},
		{
			name:    "absent markers with non-empty data",
			key:     opencdc.RawData("k"),
			value:   opencdc.RawData("v"),
			wantKey: []byte("k"),
			wantVal: []byte("v"),
		},
		{
			name:    "absent markers with empty data",
			key:     opencdc.RawData(""),
			value:   opencdc.RawData(""),
			wantErr: ErrCodeMirrorKeyNullnessUnknown, // the key is checked first
		},
		{
			name:      "value null but non-empty",
			key:       opencdc.RawData("k"),
			value:     opencdc.RawData("v"),
			keyNull:   "false",
			valueNull: "true",
			wantErr:   ErrCodeMirrorValueConflict,
		},
		{
			name:      "structured payload rejected",
			key:       opencdc.RawData("k"),
			value:     opencdc.StructuredData{"a": "b"},
			keyNull:   "false",
			valueNull: "false",
			wantErr:   ErrCodeMirrorPayloadNotRaw,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			md := validMirrorMetadata()
			if tc.keyNull == "" {
				delete(md, mirror.KeyNull)
			} else {
				md[mirror.KeyNull] = tc.keyNull
			}
			if tc.valueNull == "" {
				delete(md, mirror.ValueNull)
			} else {
				md[mirror.ValueNull] = tc.valueNull
			}

			rec := opencdc.Record{
				Key:      tc.key,
				Payload:  opencdc.Change{After: tc.value},
				Metadata: md,
			}
			got, err := buildMirrorRecord(rec, false)
			if tc.wantErr != nil {
				is.True(err != nil)
				is.True(errors.Is(err, tc.wantErr))
				return
			}
			is.NoErr(err)
			is.Equal(got.Key, tc.wantKey)
			is.Equal(got.Value, tc.wantVal)
		})
	}
}

func TestBuildMirrorRecord_MissingFields(t *testing.T) {
	cases := []struct {
		name    string
		missing string
		wantErr error
	}{
		{"partition", mirror.Partition, ErrCodePartitionInvalid},
		{"headers", mirror.Headers, ErrCodeHeadersMissing},
		{"timestamp", mirror.Timestamp, ErrCodeTimestampInvalid},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			md := validMirrorMetadata()
			delete(md, tc.missing)
			rec := opencdc.Record{
				Key:      opencdc.RawData("k"),
				Payload:  opencdc.Change{After: opencdc.RawData("v")},
				Metadata: md,
			}
			_, err := buildMirrorRecord(rec, false)
			is.True(errors.Is(err, tc.wantErr))
		})
	}
}

func TestBuildMirrorRecord_LossyHeaders(t *testing.T) {
	is := is.New(t)
	md := validMirrorMetadata()
	delete(md, mirror.Headers)
	md["kafka.header.b"] = "2"
	md["kafka.header.a"] = "1"

	rec := opencdc.Record{
		Key:      opencdc.RawData("k"),
		Payload:  opencdc.Change{After: opencdc.RawData("v")},
		Metadata: md,
	}
	got, err := buildMirrorRecord(rec, true)
	is.NoErr(err)
	// Sorted by key for determinism; order is not preserved.
	is.Equal(got.Headers, []kgo.RecordHeader{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	})
}
