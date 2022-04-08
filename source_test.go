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
	"strings"
	"testing"
	"time"

	kafka "github.com/conduitio/conduit-connector-kafka"
	"github.com/conduitio/conduit-connector-kafka/mock"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matryer/is"
	skafka "github.com/segmentio/kafka-go"
)

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	is := is.New(t)
	underTest := kafka.Source{}
	err := underTest.Configure(context.Background(), make(map[string]string))
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), "config is invalid:"))
}

func TestConfigureSource_FailsWhenConfigInvalid(t *testing.T) {
	is := is.New(t)
	underTest := kafka.Source{}
	err := underTest.Configure(context.Background(), map[string]string{"foobar": "foobar"})
	is.True(err != nil)
	is.True(strings.HasPrefix(err.Error(), "config is invalid:"))
}

func TestTeardownSource_ClosesClient(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)

	consumerMock := mock.NewConsumer(ctrl)
	consumerMock.
		EXPECT().
		Close().
		Return(nil)

	underTest := kafka.Source{Consumer: consumerMock, Config: connectorCfg()}
	is.NoErr(underTest.Teardown(context.Background()))
}

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	underTest := kafka.NewSource()
	is.NoErr(underTest.Teardown(context.Background()))
}

func TestReadPosition(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)

	kafkaMsg := testKafkaMsg()
	cfg := connectorCfg()
	pos := []byte(uuid.NewString())

	consumerMock := mock.NewConsumer(ctrl)
	consumerMock.
		EXPECT().
		Get(gomock.Any()).
		Return(kafkaMsg, pos, nil)

	underTest := kafka.Source{Consumer: consumerMock, Config: cfg}
	rec, err := underTest.Read(context.Background())
	is.NoErr(err)
	is.Equal(rec.Key.Bytes(), kafkaMsg.Key)
	is.Equal(rec.Payload.Bytes(), kafkaMsg.Value)

	is.Equal(pos, []byte(rec.Position))
}

func TestRead(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)

	kafkaMsg := testKafkaMsg()
	cfg := connectorCfg()
	pos := []byte(uuid.NewString())

	consumerMock := mock.NewConsumer(ctrl)
	consumerMock.
		EXPECT().
		Get(gomock.Any()).
		Return(kafkaMsg, pos, nil)

	underTest := kafka.Source{Consumer: consumerMock, Config: cfg}
	rec, err := underTest.Read(context.Background())
	is.NoErr(err)
	is.Equal(rec.Key.Bytes(), kafkaMsg.Key)
	is.Equal(rec.Payload.Bytes(), kafkaMsg.Value)
	is.Equal(pos, []byte(rec.Position))
}

func testKafkaMsg() *skafka.Message {
	return &skafka.Message{
		Topic:         "test",
		Partition:     0,
		Offset:        123,
		HighWaterMark: 234,
		Key:           []byte("test-key"),
		Value:         []byte("test-value"),
		Time:          time.Time{},
	}
}
