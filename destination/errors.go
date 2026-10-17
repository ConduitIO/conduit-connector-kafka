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

import "errors"

// Stable error codes for mirror mode. Each is a sentinel error that operators
// and agents can match with errors.Is; the code is the sentinel's message. The
// names follow the kafka mirror passthrough design in ConduitIO/conduit.
var (
	// Config validation.
	ErrCodeMirrorAcks               = errors.New("KAFKA_MIRROR_ACKS")
	ErrCodeDeliveryTimeoutWithTS    = errors.New("KAFKA_DELIVERY_TIMEOUT_WITH_TIMESTAMP")
	ErrCodeMirrorFormatSet          = errors.New("KAFKA_MIRROR_FORMAT_SET")
	ErrCodeProduceTimeoutOutOfRange = errors.New("KAFKA_PRODUCE_TIMEOUT_OUT_OF_RANGE")

	// Record failures in mirror mode.
	ErrCodePartitionInvalid          = errors.New("KAFKA_PARTITION_INVALID")
	ErrCodePartitionOutOfRange       = errors.New("KAFKA_PARTITION_OUT_OF_RANGE")
	ErrCodePartitionCountMismatch    = errors.New("KAFKA_PARTITION_COUNT_MISMATCH")
	ErrCodeDestinationTopicMissing   = errors.New("KAFKA_DESTINATION_TOPIC_MISSING")
	ErrCodeHeadersInvalid            = errors.New("KAFKA_HEADERS_INVALID")
	ErrCodeHeadersMissing            = errors.New("KAFKA_HEADERS_MISSING")
	ErrCodeTimestampInvalid          = errors.New("KAFKA_TIMESTAMP_INVALID")
	ErrCodeMirrorPayloadNotRaw       = errors.New("KAFKA_MIRROR_PAYLOAD_NOT_RAW")
	ErrCodeMirrorValueConflict       = errors.New("KAFKA_MIRROR_VALUE_CONFLICT")
	ErrCodeMirrorValueNullnessUnknwn = errors.New("KAFKA_MIRROR_VALUE_NULLNESS_UNKNOWN")
	ErrCodeMirrorKeyConflict         = errors.New("KAFKA_MIRROR_KEY_CONFLICT")
	ErrCodeMirrorKeyNullnessUnknown  = errors.New("KAFKA_MIRROR_KEY_NULLNESS_UNKNOWN")
	ErrCodeProduceTimeout            = errors.New("KAFKA_PRODUCE_TIMEOUT")

	// Metric names. The SDK has no connector metrics API yet, so they are
	// reported as structured log fields with these names.
	MetricPostEnqueueErrors = "kafka_destination_post_enqueue_errors_total"
)
