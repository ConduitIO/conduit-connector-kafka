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

package kafka_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	kafka "github.com/conduitio/conduit-connector-kafka"
	"github.com/conduitio/conduit-connector-kafka/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func TestConfigureDestination_FailsWhenConfigEmpty(t *testing.T) {
	is := is.New(t)
	underTest := kafka.Destination{}
	err := underTest.Configure(context.Background(), make(map[string]string))
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), "config is invalid:"))
}

func TestConfigureDestination_FailsWhenConfigInvalid(t *testing.T) {
	is := is.New(t)
	underTest := kafka.Destination{}
	err := underTest.Configure(context.Background(), map[string]string{"foobar": "foobar"})
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), "config is invalid:"))
}

func TestConfigureDestination_KafkaProducerCreated(t *testing.T) {
	is := is.New(t)
	underTest := kafka.Destination{}
	err := underTest.Configure(context.Background(), configMap())
	is.NoErr(err)

	err = underTest.Open(context.Background())
	is.NoErr(err)
	is.True(underTest.Producer != nil)
	defer underTest.Producer.Close()
}

func TestTeardown_ClosesClient(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)

	clientMock := mock.NewProducer(ctrl)
	clientMock.
		EXPECT().
		Close().
		Return(nil)

	underTest := kafka.Destination{Producer: clientMock, Config: connectorCfg()}
	is.NoErr(underTest.Teardown(context.Background()))
}

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	underTest := kafka.NewDestination()
	is.NoErr(underTest.Teardown(context.Background()))
}

func TestWrite_ClientSendsMessage(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	rec := testRec()
	producerMock := mock.NewProducer(ctrl)
	producerMock.
		EXPECT().
		Send(
			gomock.Eq(ctx),
			gomock.Eq(rec.Key.Bytes()),
			gomock.Eq(rec.Payload.Bytes()),
			gomock.Eq(rec.Position),
			gomock.Any(),
		).
		Return(nil)

	underTest := kafka.Destination{Producer: producerMock, Config: connectorCfg()}

	err := underTest.WriteAsync(
		ctx,
		rec,
		func(err error) error { return nil }, // an sdk.AckFunc
	)
	is.NoErr(err)
}

func connectorCfg() kafka.Config {
	cfg, _ := kafka.Parse(configMap())
	return cfg
}

func configMap() map[string]string {
	return map[string]string{kafka.Servers: "localhost:9092", kafka.Topic: "test"}
}

func testRec() sdk.Record {
	return sdk.Record{
		Position:  []byte(uuid.NewString()),
		Metadata:  nil,
		CreatedAt: time.Time{},
		Key:       sdk.RawData(uuid.NewString()),
		Payload:   sdk.RawData(fmt.Sprintf("test message %s", time.Now())),
	}
}
