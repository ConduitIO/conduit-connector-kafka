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
	"testing"

	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	underTest := NewDestination()
	is.NoErr(underTest.Teardown(context.Background()))
}

// func TestConfigureDestination_KafkaProducerCreated(t *testing.T) {
// 	is := is.New(t)
// 	underTest := Destination{}
// 	err := underTest.Configure(context.Background(), configMap())
// 	is.NoErr(err)
//
// 	err = underTest.Open(context.Background())
// 	is.NoErr(err)
// 	is.True(underTest.Producer != nil)
// 	defer underTest.Producer.Close()
// }

// func TestTeardown_ClosesClient(t *testing.T) {
// 	is := is.New(t)
// 	ctrl := gomock.NewController(t)
//
// 	clientMock := mock.NewProducer(ctrl)
// 	clientMock.
// 		EXPECT().
// 		Close().
// 		Return(nil)
//
// 	underTest := Destination{Producer: clientMock, Config: connectorCfg()}
// 	is.NoErr(underTest.Teardown(context.Background()))
// }

// func TestWrite_ClientSendsMessage(t *testing.T) {
// 	is := is.New(t)
// 	ctrl := gomock.NewController(t)
// 	ctx := context.Background()
//
// 	rec := testRec()
// 	producerMock := mock.NewProducer(ctrl)
// 	producerMock.EXPECT().Send(ctx, []sdk.Record{rec}).Return(1, nil)
//
// 	underTest := Destination{Producer: producerMock, Config: connectorCfg()}
//
// 	count, err := underTest.Write(ctx, []sdk.Record{rec})
// 	is.NoErr(err)
// 	is.Equal(count, 1)
// }
