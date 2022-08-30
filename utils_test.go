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
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matryer/is"
	skafka "github.com/segmentio/kafka-go"
)

func readFile(filename string, t *testing.T) string {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	return string(bytes)
}

// createTopic creates a topic and waits until its actually created.
// Having topics auto-created is not an option since the writer
// again doesn't wait for the topic to be actually created.
// Also see: https://github.com/segmentio/kafka-go/issues/219
func createTopic(t *testing.T, topic string) {
	is := is.New(t)

	client := skafka.Client{Addr: skafka.TCP("localhost:9092")}
	_, err := client.CreateTopics(
		context.Background(),
		&skafka.CreateTopicsRequest{Topics: []skafka.TopicConfig{
			{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
		}},
	)
	is.NoErr(err)
}

func sendTestMessages(t *testing.T, cfg Config, from int, to int) {
	is := is.New(t)
	writer := skafka.Writer{
		Addr:         skafka.TCP(cfg.Servers...),
		Topic:        cfg.Topic,
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
		MaxAttempts:  2,
	}
	defer writer.Close()

	for i := from; i <= to; i++ {
		err := sendTestMessage(
			&writer,
			fmt.Sprintf("test-key-%d", i),
			fmt.Sprintf("test-payload-%d", i),
		)
		is.NoErr(err)
	}
}

func sendTestMessage(writer *skafka.Writer, key string, payload string) error {
	return writer.WriteMessages(
		context.Background(),
		skafka.Message{
			Key:   []byte(key),
			Value: []byte(payload),
		},
	)
}

func testConfig() Config {
	cfg, _ := Parse(testConfigMap())
	return cfg
}

func testConfigMap() map[string]string {
	return map[string]string{
		Servers:           "localhost:9092",
		Topic:             "test-topic-" + uuid.NewString(),
		ReadFromBeginning: "true",
	}
}
