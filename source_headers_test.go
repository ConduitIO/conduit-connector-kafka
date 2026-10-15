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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	opencdcv1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
	"github.com/conduitio/conduit-connector-kafka/source"
	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

// Header fixtures for issue #319. binValue and binKey are not valid UTF-8.
var (
	binValue = []byte{0xff, 0xfe, 0x00}
	binKey   = string([]byte{0xff, 0xfe})
)

// nonUTF8Headers has a binary value, a binary key, a plain header, a repeated
// key whose winning (last) value is binary, and a repeated key whose earlier
// value is binary but whose winning value is plain.
func nonUTF8Headers() []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "bin", Value: binValue},
		{Key: binKey, Value: []byte("plain")},
		{Key: "ok", Value: []byte("plain")},
		{Key: "rep", Value: []byte("first")},
		{Key: "rep", Value: binValue},
		{Key: "fixed", Value: binValue},
		{Key: "fixed", Value: []byte("last")},
	}
}

// wantNonUTF8Metadata is what the source emitted for nonUTF8Headers before
// #319 and must keep emitting: every entry byte for byte, last value wins.
func wantNonUTF8Metadata() map[string]string {
	return map[string]string{
		"kafka.header.bin":       string(binValue),
		"kafka.header." + binKey: "plain",
		"kafka.header.ok":        "plain",
		"kafka.header.rep":       string(binValue),
		"kafka.header.fixed":     "last",
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p) //nolint:wrapcheck // test helper
}

func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, l := range bytes.Split(bytes.TrimSpace(b.buf.Bytes()), []byte("\n")) {
		if len(l) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(l, &m); err != nil {
			t.Fatalf("log line is not JSON: %q", l)
		}
		out = append(out, m)
	}
	return out
}

func (b *syncBuffer) raw() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buf.Bytes())
}

func capturedLogs() (context.Context, *syncBuffer) {
	buf := &syncBuffer{}
	return zerolog.New(buf).WithContext(context.Background()), buf
}

func warningsWithCode(lines []map[string]any, code string) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["level"] == "warn" && l["code"] == code {
			out = append(out, l)
		}
	}
	return out
}

// assertNonUTF8Reported checks what #319 asks for, for `records` reads of a
// record carrying nonUTF8Headers: one WARN per offending header key (bin, the
// binary key, rep; not ok, not fixed), naming the key as base64 and never the
// value, and a teardown total counting every offending entry.
func assertNonUTF8Reported(t *testing.T, logs *syncBuffer, records int) {
	t.Helper()
	is := is.New(t)

	lines := logs.lines(t)
	warns := warningsWithCode(lines, CodeLegacyHeaderNotUTF8)
	var perKey []map[string]any
	var total map[string]any
	for _, w := range warns {
		if _, ok := w["header_key_b64"]; ok {
			perKey = append(perKey, w)
		} else {
			total = w
		}
	}

	got := map[string]string{}
	for _, w := range perKey {
		k, err := base64.StdEncoding.DecodeString(w["header_key_b64"].(string))
		is.NoErr(err)
		got[string(k)] = w["invalid"].(string)
	}
	is.Equal(len(perKey), 3) // once per key, not once per record
	is.Equal(got, map[string]string{"bin": "value", binKey: "key", "rep": "value"})

	is.True(total != nil) // teardown summary
	is.Equal(total[MetricLegacyHeaderNotUTF8], float64(3*records))

	// The value is never logged, in any form.
	is.True(!bytes.Contains(logs.raw(), binValue[:2]))
	is.True(!strings.Contains(string(logs.raw()), base64.StdEncoding.EncodeToString(binValue)))
}

// protoMarshal is the path a standalone connector's records take.
func protoMarshal(rec opencdc.Record) error {
	var pb opencdcv1.Record
	if err := rec.ToProto(&pb); err != nil {
		return err //nolint:wrapcheck // test helper
	}
	_, err := proto.Marshal(&pb)
	return err //nolint:wrapcheck // test helper
}

func TestSource_Read_NonUTF8Headers_EmittedUnchangedAndReported(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)

	recs := test.GenerateFranzRecords(0, 1, "foo")
	for _, r := range recs {
		r.Headers = nonUTF8Headers()
	}

	consumerMock := source.NewMockConsumer(ctrl)
	for _, r := range recs {
		consumerMock.EXPECT().Consume(gomock.Any()).Return((*source.Record)(r), nil)
	}
	consumerMock.EXPECT().Close(gomock.Any()).Return(nil)

	ctx, logs := capturedLogs()
	cfg := test.ParseConfigMap[source.Config](t, test.SourceConfigMap(t, false, false))
	underTest := Source{consumer: consumerMock, config: cfg}

	for range recs {
		got, err := underTest.Read(ctx)
		is.NoErr(err)
		for k, v := range wantNonUTF8Metadata() {
			is.Equal(got.Metadata[k], v) // emitted unchanged: invariant 6, and the default output contract
		}
		// The hazard this reports is real: the record cannot cross a
		// protobuf hop.
		err = protoMarshal(got)
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), "invalid UTF-8"))
	}
	is.NoErr(underTest.Teardown(ctx))

	assertNonUTF8Reported(t, logs, len(recs))
}

func TestSource_Read_ValidHeaders_NotReported(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)

	rec := test.GenerateFranzRecords(0, 0, "foo")[0]
	rec.Headers = []kgo.RecordHeader{
		{Key: "a", Value: []byte("value")},
		{Key: "ctrl", Value: []byte{0, 1, 2}}, // control bytes are valid UTF-8
		{Key: "nil", Value: nil},
		{Key: "ünï", Value: []byte("çødé")},
	}
	consumerMock := source.NewMockConsumer(ctrl)
	consumerMock.EXPECT().Consume(gomock.Any()).Return((*source.Record)(rec), nil)
	consumerMock.EXPECT().Close(gomock.Any()).Return(nil)

	ctx, logs := capturedLogs()
	cfg := test.ParseConfigMap[source.Config](t, test.SourceConfigMap(t, false, false))
	underTest := Source{consumer: consumerMock, config: cfg}
	got, err := underTest.Read(ctx)
	is.NoErr(err)
	is.NoErr(protoMarshal(got))
	is.NoErr(underTest.Teardown(ctx))
	is.Equal(len(warningsWithCode(logs.lines(t), CodeLegacyHeaderNotUTF8)), 0)
}

// A topic whose binary header keys differ in every record must not flood the
// log or grow memory without bound; every entry is still counted.
func TestNonUTF8HeaderTracker_BoundsWarnedKeys(t *testing.T) {
	is := is.New(t)
	ctx, logs := capturedLogs()
	var tr nonUTF8HeaderTracker

	const n = maxWarnedHeaderKeys + 50
	for i := range n {
		key := string([]byte{0xff, byte(i >> 8), byte(i)})
		rec := &source.Record{Topic: "t", Headers: []kgo.RecordHeader{{Key: key, Value: []byte("v")}}}
		tr.observe(ctx, rec, opencdc.Metadata{MetadataKafkaHeaderPrefix + key: "v"})
	}
	tr.logTotal(ctx)

	warns := warningsWithCode(logs.lines(t), CodeLegacyHeaderNotUTF8)
	is.Equal(len(warns), maxWarnedHeaderKeys+1) // per key up to the bound, plus the total
	is.Equal(len(tr.warned), maxWarnedHeaderKeys)
	is.Equal(warns[len(warns)-1][MetricLegacyHeaderNotUTF8], float64(n))
}
