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

// TODO do not push this to main, this is only an experiment to see if micro-batches improve performance

package kafka

import (
	"bytes"
	"strconv"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	separator      = '⌘'
	microBatchSize = 50
)

type MicroBatch []sdk.Record

// -- MERGE --

func (b MicroBatch) ToRecord() sdk.Record {
	lastRec := b[len(b)-1]
	return sdk.Record{
		Position:  b.mergePositions(),
		Operation: lastRec.Operation, // operation is the same for all right now
		Metadata:  b.mergeMetadata(),
		Key:       b.mergeKeys(),
		Payload: sdk.Change{
			After: b.mergePayloadAfter(),
		},
	}
}

func (b MicroBatch) mergeMetadata() sdk.Metadata {
	m := make(sdk.Metadata, len(b[0].Metadata)*(len(b)+1))
	for i, rec := range b {
		for k, v := range rec.Metadata {
			m[strconv.Itoa(i)+"."+k] = v
		}
	}
	return m
}

func (b MicroBatch) mergePositions() sdk.Position {
	buf := bytes.NewBuffer(make([]byte, 0, len(b[0].Position)*(len(b)+1)))
	for _, rec := range b {
		buf.Write(rec.Position)
		buf.WriteRune(separator)
	}
	return buf.Bytes()
}

func (b MicroBatch) mergeKeys() sdk.Data {
	buf := bytes.NewBuffer(make([]byte, 0, len(b[0].Key.Bytes())*(len(b)+1)))
	for _, rec := range b {
		buf.Write(rec.Key.Bytes())
		buf.WriteRune(separator)
	}
	return sdk.RawData(buf.Bytes())
}

func (b MicroBatch) mergePayloadAfter() sdk.Data {
	buf := bytes.NewBuffer(make([]byte, 0, len(b[0].Payload.After.Bytes())*(len(b)+1)))
	for _, rec := range b {
		buf.Write(rec.Payload.After.Bytes())
		buf.WriteRune(separator)
	}
	return sdk.RawData(buf.Bytes())
}

// -- SPLIT --

func (b *MicroBatch) FromRecord(rec sdk.Record) {
	positions := b.splitBytes(rec.Position)
	keys := b.splitBytes(rec.Key.Bytes())
	payloadAfters := b.splitBytes(rec.Payload.After.Bytes())
	metadata := b.splitMetadata(rec.Metadata, len(positions))

	for i := 0; i < len(positions); i++ {
		*b = append(*b, sdk.Record{
			Position:  positions[i],
			Operation: rec.Operation,
			Metadata:  metadata[i],
			Key:       sdk.RawData(keys[i]),
			Payload: sdk.Change{
				After: sdk.RawData(payloadAfters[i]),
			},
		})
	}
}

func (MicroBatch) splitBytes(bb []byte) [][]byte {
	split := bytes.Split(bb, []byte(string([]rune{separator})))
	return split[:len(split)-1] // data ends with a separator, remove last element
}

func (MicroBatch) splitMetadata(metadata sdk.Metadata, count int) []sdk.Metadata {
	m := make([]sdk.Metadata, count)
	genericMetadata := make(sdk.Metadata)
	for k, v := range metadata {
		prefix, key, found := strings.Cut(k, ".")
		if !found {
			genericMetadata[k] = v
			continue
		}
		i, err := strconv.Atoi(prefix)
		if err != nil {
			genericMetadata[k] = v
			continue
		}
		md := m[i]
		if md == nil {
			md = sdk.Metadata{}
			m[i] = md
		}
		md[key] = v
	}
	for _, md := range m {
		for k, v := range genericMetadata {
			md[k] = v
		}
	}
	return m
}
