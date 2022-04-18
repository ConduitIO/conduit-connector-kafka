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

//go:generate mockgen -destination mock/consumer.go -package mock -mock_names=Consumer=Consumer . Consumer

package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Consumer represents a Kafka consumer in a simplified form,
// with just the functionality which is needed for this plugin.
// A Consumer's offset is being managed by the broker.
type Consumer interface {
	// StartFrom instructs the consumer to connect to a broker and a topic, using the provided consumer group ID.
	// The group ID is significant for this consumer's offsets.
	// By using the same group ID after a restart, we make sure that the consumer continues from where it left off.
	// Returns: An error, if the consumer could not be set to read from the given position, nil otherwise.
	StartFrom(config Config, position []byte) error

	// Get returns a message from the configured topic. Waits until a messages is available
	// or until it errors out.
	// Returns: a message (if available), the message's position and an error (if there was one).
	Get(ctx context.Context) (*kafka.Message, []byte, error)

	Ack(position sdk.Position) error

	// Close this consumer and the associated resources (e.g. connections to the broker)
	Close() error
}

type position struct {
	GroupID   string `json:"groupID"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

func (p position) json() ([]byte, error) {
	bytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("couldn't transform position into json: %w", err)
	}
	return bytes, nil
}

func parsePosition(bytes []byte) (position, error) {
	pos := position{}
	if len(bytes) == 0 {
		return pos, nil
	}
	err := json.Unmarshal(bytes, &pos)
	if err != nil {
		return position{}, err
	}
	return pos, nil
}

type segmentConsumer struct {
	reader        *kafka.Reader
	unackMessages []*kafka.Message
}

// NewConsumer creates a new Kafka consumer. The consumer needs to be started
// (using the StartFrom method) before actually being used.
func NewConsumer() (Consumer, error) {
	return &segmentConsumer{}, nil
}

func (c *segmentConsumer) StartFrom(config Config, positionBytes []byte) error {
	// todo if we can assume that a new Config instance will always be created by calling Parse(),
	// and that the instance will not be mutated, then we can leave it out these checks.
	if len(config.Servers) == 0 {
		return ErrServersMissing
	}
	if config.Topic == "" {
		return ErrTopicMissing
	}
	position, err := parsePosition(positionBytes)
	if err != nil {
		return fmt.Errorf("couldn't parse position: %w", err)
	}

	err = c.newReader(config, position.GroupID)
	if err != nil {
		return fmt.Errorf("couldn't create reader: %w", err)
	}
	return nil
}

func (c *segmentConsumer) newReader(cfg Config, groupID string) error {
	readerCfg := kafka.ReaderConfig{
		Brokers:               cfg.Servers,
		Topic:                 cfg.Topic,
		WatchPartitionChanges: true,
	}
	// Group ID
	if groupID == "" {
		readerCfg.GroupID = uuid.NewString()
	} else {
		readerCfg.GroupID = groupID
	}
	// StartOffset
	if cfg.ReadFromBeginning {
		readerCfg.StartOffset = kafka.FirstOffset
	} else {
		readerCfg.StartOffset = kafka.LastOffset
	}
	// TLS config
	if cfg.useTLS() {
		err := c.withTLS(&readerCfg, cfg)
		if err != nil {
			return fmt.Errorf("failed to set up TLS: %w", err)
		}
	}
	// SASL
	if cfg.saslEnabled() {
		err := c.withSASL(&readerCfg, cfg)
		if err != nil {
			return fmt.Errorf("couldn't configure SASL: %w", err)
		}
	}
	c.reader = kafka.NewReader(readerCfg)
	return nil
}

func (c *segmentConsumer) withTLS(readerCfg *kafka.ReaderConfig, cfg Config) error {
	tlsCfg, err := newTLSConfig(cfg.ClientCert, cfg.ClientKey, cfg.CACert, cfg.InsecureSkipVerify)
	if err != nil {
		return fmt.Errorf("invalid TLS config: %w", err)
	}
	if readerCfg.Dialer == nil {
		readerCfg.Dialer = &kafka.Dialer{}
	}
	readerCfg.Dialer.DualStack = true
	readerCfg.Dialer.TLS = tlsCfg
	return nil
}

func (c *segmentConsumer) withSASL(readerCfg *kafka.ReaderConfig, cfg Config) error {
	if readerCfg.Dialer == nil {
		readerCfg.Dialer = &kafka.Dialer{}
	}
	if !cfg.saslEnabled() {
		return errors.New("input config has no SASL parameters")
	}
	mechanism, err := newSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
	if err != nil {
		return fmt.Errorf("couldn't configure SASL mechanism: %w", err)
	}
	readerCfg.Dialer.SASLMechanism = mechanism
	return nil
}

func (c *segmentConsumer) Get(ctx context.Context) (*kafka.Message, []byte, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't read message: %w", err)
	}

	position, err := c.positionOf(&msg)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get message's position: %w", err)
	}

	c.unackMessages = append(c.unackMessages, &msg)
	return &msg, position, nil
}

func (c *segmentConsumer) positionOf(m *kafka.Message) ([]byte, error) {
	p := position{
		GroupID:   c.readerID(),
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
	}
	return p.json()
}

func (c *segmentConsumer) Ack(position sdk.Position) error {
	err := c.canAck(position)
	if err != nil {
		return fmt.Errorf("ack not possible: %w", err)
	}
	err = c.reader.CommitMessages(context.Background(), *c.unackMessages[0])
	if err != nil {
		return fmt.Errorf("couldn't commit messages: %w", err)
	}
	c.unackMessages = c.unackMessages[1:]
	return nil
}

func (c *segmentConsumer) canAck(position sdk.Position) error {
	if len(c.unackMessages) == 0 {
		return errors.New("no unacknowledged messages found")
	}
	pos, err := c.positionOf(c.unackMessages[0])
	if err != nil {
		return fmt.Errorf("failed to get position of first unacknowledged message: %w", err)
	}
	// We're going to yell at Conduit for not keeping its promise:
	// acks should be requested in the same order reads were done.
	if bytes.Compare(pos, position) != 0 {
		return fmt.Errorf("ack is out-of-order, requested ack for %q, but first unack. message is %q", position, pos)
	}
	return err
}

func (c *segmentConsumer) Close() error {
	if c.reader == nil {
		return nil
	}
	// this will also make the loops in the reader goroutines stop
	err := c.reader.Close()
	if err != nil {
		return fmt.Errorf("couldn't close reader: %w", err)
	}

	return nil
}

func (c *segmentConsumer) readerID() string {
	return c.reader.Config().GroupID
}
