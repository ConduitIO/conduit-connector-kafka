// Copyright © 2022 Meroxa, Inc.
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
	"context"
	"strconv"
	"testing"

	"github.com/conduitio/conduit-connector-kafka/test"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-kafka/source"
	"github.com/matryer/is"
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

	underTest := Source{consumer: consumerMock, config: test.SourceConfig(t)}
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

	rec := test.GenerateRecords(0, 0)[0]
	want := sdk.Record{
		Position: source.Position{
			GroupID:   "",
			Topic:     rec.Topic,
			Partition: rec.Partition,
			Offset:    rec.Offset,
		}.ToSDKPosition(),
		Operation: sdk.OperationCreate,
		Metadata: map[string]string{
			MetadataKafkaTopic:    rec.Topic,
			sdk.MetadataCreatedAt: strconv.FormatInt(rec.Timestamp.UnixNano(), 10),
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

	underTest := Source{consumer: consumerMock, config: test.SourceConfig(t)}
	got, err := underTest.Read(context.Background())
	is.NoErr(err)
	is.True(got.Metadata[sdk.MetadataReadAt] != "")
	want.Metadata[sdk.MetadataReadAt] = got.Metadata[sdk.MetadataReadAt]
	is.Equal(want, got)
}
