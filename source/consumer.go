// Copyright © 2023 Meroxa, Inc.
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

//go:generate mockgen -destination consumer_mock.go -package source -mock_names=Consumer=MockConsumer . Consumer

package source

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer is a kafka consumer.
type Consumer interface {
	// Consume returns the next message from the configured topic. Waits until a
	// message is available or until the context is canceled.
	Consume(context.Context) (*Record, error)
	// Ack commits the offset to Kafka.
	Ack(context.Context) error
	// Close this consumer and the associated resources.
	Close(context.Context) error
}

type Record kgo.Record
