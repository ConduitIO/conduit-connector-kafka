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
	"strconv"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/mirror"
)

// Values of kafka.timestamp.type. kgo.Record.Attrs.TimestampType returns 0 for
// CreateTime, 1 for LogAppendTime and -1 when the record has no timestamp; the
// mirror metadata only ever carries "create" or "logappend" because both
// brokers assign a timestamp to records produced today.
const (
	timestampTypeCreate    = "create"
	timestampTypeLogAppend = "logappend"
)

// BuildMirrorMetadata writes the mirror metadata keys for rec into metadata.
// partitions is the partition count of rec's topic, or 0 when it is unknown
// (the key is then omitted so the destination cannot mistake "unknown" for a
// real count). It is called only when the source's emitMirrorMetadata option is
// enabled.
func BuildMirrorMetadata(rec *Record, partitions int32, metadata opencdc.Metadata) {
	metadata[mirror.Partition] = strconv.FormatInt(int64(rec.Partition), 10)
	if partitions > 0 {
		metadata[mirror.Partitions] = strconv.FormatInt(int64(partitions), 10)
	}
	metadata[mirror.Offset] = strconv.FormatInt(rec.Offset, 10)
	metadata[mirror.Timestamp] = strconv.FormatInt(rec.Timestamp.UnixMilli(), 10)
	if rec.Attrs.TimestampType() == 1 {
		metadata[mirror.TimestampType] = timestampTypeLogAppend
	} else {
		metadata[mirror.TimestampType] = timestampTypeCreate
	}
	metadata[mirror.Headers] = mirror.EncodeHeaders(rec.Headers)
	metadata[mirror.KeyNull] = strconv.FormatBool(rec.Key == nil)
	metadata[mirror.ValueNull] = strconv.FormatBool(rec.Value == nil)
}
