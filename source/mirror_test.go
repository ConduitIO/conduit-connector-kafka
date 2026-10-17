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

package source

import (
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/mirror"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBuildMirrorMetadata(t *testing.T) {
	is := is.New(t)

	rec := &Record{
		Topic:     "t",
		Partition: 2,
		Offset:    10,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Timestamp: time.UnixMilli(1760000000000),
		Headers: []kgo.RecordHeader{
			{Key: "a", Value: []byte("b")},
		},
	}
	rec.Attrs = kgo.NewRecordAttrs(kgo.RecordAttrsOpts{})

	md := opencdc.Metadata{}
	BuildMirrorMetadata(rec, 3, md)

	is.Equal(md[mirror.Partition], "2")
	is.Equal(md[mirror.Partitions], "3")
	is.Equal(md[mirror.Offset], "10")
	is.Equal(md[mirror.Timestamp], "1760000000000")
	is.Equal(md[mirror.TimestampType], "create")
	is.Equal(md[mirror.KeyNull], "false")
	is.Equal(md[mirror.ValueNull], "false")
	is.Equal(md[mirror.Headers], mirror.EncodeHeaders(rec.Headers))
}

func TestBuildMirrorMetadata_NullMarkersAndUnknownPartitions(t *testing.T) {
	is := is.New(t)

	rec := &Record{
		Topic:     "t",
		Partition: 0,
		Offset:    0,
		Key:       nil,
		Value:     nil,
		Timestamp: time.UnixMilli(0),
	}
	rec.Attrs = kgo.NewRecordAttrs(kgo.RecordAttrsOpts{TimestampType: 1})

	md := opencdc.Metadata{}
	BuildMirrorMetadata(rec, 0, md) // partitions unknown -> omitted

	is.Equal(md[mirror.KeyNull], "true")
	is.Equal(md[mirror.ValueNull], "true")
	is.Equal(md[mirror.TimestampType], "logappend")
	_, ok := md[mirror.Partitions]
	is.True(!ok)
}
