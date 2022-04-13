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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matryer/is"
	skafka "github.com/segmentio/kafka-go"
)

func TestConsumer_Get_FromBeginning(t *testing.T) {
	is := is.New(t)
	t.Parallel()

	cfg := Config{
		Topic:             "TestConsumer_Get_FromBeginning_" + uuid.NewString(),
		Servers:           []string{"localhost:9092"},
		ReadFromBeginning: true,
	}
	createTopic(t, cfg.Topic)
	sendTestMessages(t, cfg, 1, 6)

	consumer, err := NewConsumer()
	is.NoErr(err)
	defer consumer.Close()

	err = consumer.StartFrom(cfg, nil)
	is.NoErr(err)
	time.Sleep(5 * time.Second)

	messagesUnseen := map[string]bool{
		"test-key-1": true,
		"test-key-2": true,
		"test-key-3": true,
		"test-key-4": true,
		"test-key-5": true,
		"test-key-6": true,
	}
	for i := 1; i <= 6; i++ {
		message, _, err := waitForMessage(consumer, 200*time.Millisecond)
		is.True(message != nil)
		is.NoErr(err)
		delete(messagesUnseen, string(message.Key))
	}
	is.Equal(0, len(messagesUnseen))
}

func TestConsumer_Get_OnlyNew(t *testing.T) {
	is := is.New(t)
	t.Parallel()

	cfg := Config{
		Topic:             "TestConsumer_Get_OnlyNew_" + uuid.NewString(),
		Servers:           []string{"localhost:9092"},
		ReadFromBeginning: false,
	}
	createTopic(t, cfg.Topic)
	sendTestMessages(t, cfg, 1, 6)

	consumer, err := NewConsumer()
	is.NoErr(err)
	defer consumer.Close()

	err = consumer.StartFrom(cfg, nil)
	is.NoErr(err)
	time.Sleep(4 * time.Second)

	sendTestMessages(t, cfg, 7, 9)

	messagesUnseen := map[string]bool{
		"test-key-7": true,
		"test-key-8": true,
		"test-key-9": true,
	}
	for i := 1; i <= 3; i++ {
		message, _, err := waitForMessage(consumer, 200*time.Millisecond)
		is.True(message != nil)
		is.NoErr(err)
		delete(messagesUnseen, string(message.Key))
	}
	is.Equal(0, len(messagesUnseen))
}

func waitForMessage(consumer Consumer, timeout time.Duration) (*skafka.Message, []byte, error) {
	c := make(chan struct {
		msg *skafka.Message
		pos []byte
		err error
	})

	go func() {
		msg, pos, err := consumer.Get(context.Background())
		c <- struct {
			msg *skafka.Message
			pos []byte
			err error
		}{msg: msg, pos: pos, err: err}
	}()

	select {
	case r := <-c:
		return r.msg, r.pos, r.err // completed normally
	case <-time.After(timeout):
		return nil, nil, errors.New("timed out while waiting for message") // timed out
	}
}

func TestGet_KafkaDown(t *testing.T) {
	is := is.New(t)
	t.Parallel()

	cfg := Config{Topic: "client_integration_test_topic", Servers: []string{"localhost:12345"}}
	consumer, err := NewConsumer()
	is.NoErr(err)

	err = consumer.StartFrom(cfg, nil)
	is.NoErr(err)

	msg, _, err := consumer.Get(context.Background())
	is.True(msg == nil)
	var cause *net.OpError
	is.True(errors.As(err, &cause))
	is.Equal("dial", cause.Op)
	is.Equal("tcp", cause.Net)
}
