// Copyright © 2024 Meroxa, Inc.
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

package redpanda

import (
	"context"
	"strconv"
	"testing"

	"github.com/conduitio-labs/conduit-connector-redpanda/source"
	"github.com/conduitio-labs/conduit-connector-redpanda/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"
)

func TestSource_Teardown_Success(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)

	consumerMock := source.NewMockConsumer(ctrl)
	consumerMock.
		EXPECT().
		Close(context.Background()).
		Return(nil)

	cfg := test.ParseConfigMap[source.Config](t, test.SourceConfigMap(t, true))

	underTest := Source{consumer: consumerMock, config: cfg}
	is.NoErr(underTest.Teardown(context.Background()))
}

func TestSource_Teardown_NoOpen(t *testing.T) {
	is := is.New(t)
	underTest := NewSource()
	is.NoErr(underTest.Teardown(context.Background()))
}

func TestSource_Read(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)

	rec := test.GenerateFranzRecords(0, 0, "foo")[0]
	rec.Headers = []kgo.RecordHeader{
		{Key: "header-a", Value: []byte("value-a")},
		{Key: "header-b", Value: []byte{0, 1, 2}},
	}
	want := sdk.Record{
		Position: source.Position{
			GroupID:   "",
			Topic:     rec.Topic,
			Partition: rec.Partition,
			Offset:    rec.Offset,
		}.ToSDKPosition(),
		Operation: sdk.OperationCreate,
		Metadata: map[string]string{
			sdk.MetadataCollection:  rec.Topic,
			sdk.MetadataCreatedAt:   strconv.FormatInt(rec.Timestamp.UnixNano(), 10),
			"kafka.header.header-a": "value-a",
			"kafka.header.header-b": string([]byte{0, 1, 2}),
		},
		Key: sdk.RawData(rec.Key),
		Payload: sdk.Change{
			After: sdk.RawData(rec.Value),
		},
	}

	consumerMock := source.NewMockConsumer(ctrl)
	consumerMock.
		EXPECT().
		Consume(gomock.Any()).
		Return((*source.Record)(rec), nil)

	cfg := test.ParseConfigMap[source.Config](t, test.SourceConfigMap(t, false))
	underTest := Source{consumer: consumerMock, config: cfg}
	got, err := underTest.Read(context.Background())
	is.NoErr(err)
	is.True(got.Metadata[sdk.MetadataReadAt] != "")
	want.Metadata[sdk.MetadataReadAt] = got.Metadata[sdk.MetadataReadAt]
	is.Equal(cmp.Diff(want, got, cmpopts.IgnoreUnexported(sdk.Record{})), "")
}
