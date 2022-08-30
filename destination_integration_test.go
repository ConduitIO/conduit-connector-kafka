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
	"fmt"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	skafka "github.com/segmentio/kafka-go"
)

// todo try optimizing, the test takes 15 seconds to run!
func TestDestination_Write_Simple(t *testing.T) {
	is := is.New(t)
	// prepare test data
	cfg := newTestConfig(t)
	createTopic(t, cfg[Topic])
	testWriteSimple(cfg, is)
}

func TestDestination_Write_Simple_AutoCreate(t *testing.T) {
	is := is.New(t)
	// prepare test data
	cfg := newTestConfig(t)
	testWriteSimple(cfg, is)
}

func testWriteSimple(cfg map[string]string, is *is.I) {
	record := testRecord()

	// prepare SUT
	underTest := Destination{}
	err := underTest.Configure(context.Background(), cfg)
	is.NoErr(err)

	err = underTest.Open(context.Background())
	defer func(underTest *Destination, ctx context.Context) {
		err := underTest.Teardown(ctx)
		is.NoErr(err)
	}(&underTest, context.Background())
	is.NoErr(err)

	// act and assert
	count, err := underTest.Write(
		context.Background(),
		[]sdk.Record{record},
	)
	is.NoErr(err)
	is.Equal(count, 1)

	message, err := waitForReaderMessage(cfg[Topic], 15*time.Second)
	is.NoErr(err)
	is.Equal(record.Bytes(), message.Value)
}

func waitForReaderMessage(topic string, timeout time.Duration) (skafka.Message, error) {
	// Kafka is started in Docker
	reader := newKafkaReader(topic)
	defer reader.Close()

	withTimeout, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return reader.ReadMessage(withTimeout)
}

func newTestConfig(t *testing.T) map[string]string {
	return map[string]string{
		Servers: "localhost:9092",
		Topic:   t.Name() + uuid.NewString(),
	}
}

func testRecord() sdk.Record {
	return sdk.Record{
		Operation: sdk.OperationUpdate,
		Position:  []byte(uuid.NewString()),
		Metadata: map[string]string{
			"foo": "bar",
		},
		Key: sdk.RawData(uuid.NewString()),
		Payload: sdk.Change{
			Before: sdk.RawData(fmt.Sprintf("test before %s", time.Now())),
			After:  sdk.RawData(fmt.Sprintf("test after %s", time.Now())),
		},
	}
}

func newKafkaReader(topic string) (reader *skafka.Reader) {
	return skafka.NewReader(skafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       topic,
		StartOffset: skafka.FirstOffset,
		GroupID:     uuid.NewString(),
	})
}
