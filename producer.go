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

//go:generate mockgen -destination mock/producer.go -package mock -mock_names=Producer=Producer . Producer

package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	// Send synchronously delivers a message.
	// Returns an error, if the message could not be delivered.
	Send(key []byte, payload []byte) error

	// Close this producer and the associated resources (e.g. connections to the broker)
	Close() error
}

type segmentProducer struct {
	writer *kafka.Writer
}

// NewProducer creates a new Kafka producer.
// The current implementation uses Segment's kafka-go client.
func NewProducer(cfg Config) (Producer, error) {
	if len(cfg.Servers) == 0 {
		return nil, ErrServersMissing
	}
	if cfg.Topic == "" {
		return nil, ErrTopicMissing
	}

	p := &segmentProducer{}
	err := p.newWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't create writer: %w", err)
	}
	return p, nil
}

func (p *segmentProducer) newWriter(cfg Config) error {
	p.writer = &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Servers...),
		Topic:                  cfg.Topic,
		BatchSize:              1,
		WriteTimeout:           cfg.DeliveryTimeout,
		RequiredAcks:           cfg.Acks,
		MaxAttempts:            3,
		AllowAutoTopicCreation: true,
	}
	err := p.configureSecurity(cfg)
	if err != nil {
		return fmt.Errorf("couldn't configure security: %w", err)
	}
	return nil
}

func (p *segmentProducer) configureSecurity(cfg Config) error {
	// Nothing to do
	if cfg.ClientCert == "" && !cfg.saslEnabled() {
		return nil
	}
	transport := &kafka.Transport{}
	// TLS settings
	if cfg.ClientCert != "" {
		tlsCfg, err := newTLSConfig(cfg.ClientCert, cfg.ClientKey, cfg.CACert, cfg.InsecureSkipVerify)
		if err != nil {
			return fmt.Errorf("invalid TLS config: %w", err)
		}
		transport.TLS = tlsCfg
	}

	// SASL
	if cfg.saslEnabled() {
		mechanism, err := newSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return fmt.Errorf("couldn't configure SASL: %w", err)
		}
		transport.SASL = mechanism
	}
	p.writer.Transport = transport

	return nil
}

func (p *segmentProducer) Send(key []byte, payload []byte) error {
	err := p.writer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   key,
			Value: payload,
		},
	)

	if err != nil {
		return fmt.Errorf("message not delivered: %w", err)
	}
	return nil
}

func (p *segmentProducer) Close() error {
	if p.writer == nil {
		return nil
	}
	// this will also make the loops in the reader goroutines stop
	err := p.writer.Close()
	if err != nil {
		return fmt.Errorf("couldn't close writer: %w", err)
	}

	return nil
}
