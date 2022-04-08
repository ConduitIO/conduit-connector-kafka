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
	"encoding/json"
	"testing"

	"github.com/matryer/is"
	"github.com/segmentio/kafka-go"
)

func TestMessagePosition(t *testing.T) {
	is := is.New(t)
	msg := &kafka.Message{
		Topic:     "test-topic",
		Partition: 12,
		Offset:    987,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}
	c := segmentConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"localhost"}, Topic: "test-topic", GroupID: "abc"}),
	}
	pos, err := c.positionOf(msg)
	is.NoErr(err)

	positionParsed := &position{}
	err = json.Unmarshal(pos, positionParsed)
	is.NoErr(err)

	is.Equal("abc", positionParsed.GroupID)
	is.Equal("test-topic", positionParsed.Topic)
	is.Equal(12, positionParsed.Partition)
	is.Equal(int64(987), positionParsed.Offset)
}
