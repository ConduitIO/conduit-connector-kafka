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
	"testing"

	"github.com/conduitio-labs/conduit-connector-redpanda/destination"
	"github.com/conduitio-labs/conduit-connector-redpanda/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestDestination_Teardown_Success(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)

	producerMock := destination.NewMockProducer(ctrl)
	producerMock.
		EXPECT().
		Close(ctx).
		Return(nil)

	cfg := test.ParseConfigMap[destination.Config](t, test.DestinationConfigMap(t))

	underTest := Destination{producer: producerMock, config: cfg}
	is.NoErr(underTest.Teardown(ctx))
}

func TestDestination_Teardown_NoOpen(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	underTest := NewDestination()
	is.NoErr(underTest.Teardown(ctx))
}

func TestDestination_Write_Produce(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	recs := []sdk.Record{{}}
	producerMock := destination.NewMockProducer(ctrl)
	producerMock.EXPECT().Produce(ctx, recs).Return(1, nil)

	cfg := test.ParseConfigMap[destination.Config](t, test.DestinationConfigMap(t))
	underTest := Destination{producer: producerMock, config: cfg}

	count, err := underTest.Write(ctx, recs)
	is.NoErr(err)
	is.Equal(count, 1)
}
