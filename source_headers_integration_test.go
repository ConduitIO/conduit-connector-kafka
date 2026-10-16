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
	"testing"

	"github.com/conduitio/conduit-connector-kafka/source"
	"github.com/conduitio/conduit-connector-kafka/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// Issue #319, against a real broker: headers that are not valid UTF-8 survive
// the broker byte for byte, reach the source's metadata unchanged, and are
// reported once per key with a count.
func TestSource_Integration_NonUTF8Headers(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	ctx, logs := capturedLogs()

	cfgMap := test.SourceConfigMap(t, false, false)
	cfg := test.ParseConfigMap[source.Config](t, cfgMap)

	recs := test.GenerateFranzRecords(1, 2)
	for _, r := range recs {
		r.Headers = nonUTF8Headers()
	}
	test.Produce(t, cfg.Servers, cfg.Topics[0], recs)

	underTest := NewSource()
	is.NoErr(sdk.Util.ParseConfig(ctx, cfgMap, underTest.Config(), Connector.NewSpecification().SourceParams))
	is.NoErr(underTest.Open(ctx, nil))

	for _, want := range recs {
		got, err := underTest.Read(ctx)
		is.NoErr(err)
		is.Equal(got.Key.Bytes(), want.Key)
		for k, v := range wantNonUTF8Metadata() {
			is.Equal(got.Metadata[k], v)
		}
		is.True(protoMarshal(got) != nil)
		is.NoErr(underTest.Ack(ctx, got.Position))
	}
	is.NoErr(underTest.Teardown(ctx))

	assertNonUTF8Reported(t, logs, len(recs))
}
