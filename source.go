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
	"fmt"
	"strings"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	MetadataKafkaHeaderPrefix = "kafka.header."
)

type Source struct {
	sdk.UnimplementedSource

	consumer source.Consumer
	config   source.Config
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() config.Parameters {
	return source.Config{}.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg config.Config) error {
	err := sdk.Util.ParseConfig(ctx, cfg, &s.config, NewSource().Parameters())
	if err != nil {
		return err
	}
	err = s.config.Validate(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *Source) Open(ctx context.Context, sdkPos opencdc.Position) error {
	err := s.config.TryDial(ctx)
	if err != nil {
		return fmt.Errorf("failed to dial broker: %w", err)
	}

	if sdkPos != nil {
		// update group ID in the config
		p, err := source.ParseSDKPosition(sdkPos)
		if err != nil {
			return err
		}
		if s.config.GroupID != "" && s.config.GroupID != p.GroupID {
			return fmt.Errorf("the old position contains a different consumer group ID than the connector configuration (%q vs %q), please check if the configured group ID changed since the last run", p.GroupID, s.config.GroupID)
		}
		s.config.GroupID = p.GroupID
	}
	if s.config.GroupID == "" {
		// this must be the first run of the connector, create a new group ID
		s.config.GroupID = uuid.NewString()
		sdk.Logger(ctx).Info().Str("groupId", s.config.GroupID).Msg("assigning source to new consumer group")
	}

	s.consumer, err = source.NewFranzConsumer(ctx, s.config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	rec, err := s.consumer.Consume(ctx)
	if err != nil {
		var errGroupSession *kgo.ErrGroupSession
		if s.config.RetryGroupJoinErrors &&
			(errors.As(err, &errGroupSession) || strings.Contains(err.Error(), "unable to join group session")) {
			sdk.Logger(ctx).Error().Msgf("group session error, retrying: %s", err.Error())
			return opencdc.Record{}, sdk.ErrBackoffRetry
		}
		if s.config.RetryLeaderErrors {
			switch err {
			case kerr.LeaderNotAvailable,
				kerr.EligibleLeadersNotAvailable,
				kerr.PreferredLeaderNotAvailable,
				kerr.BrokerNotAvailable:
				sdk.Logger(ctx).Error().Msgf("ephemeral error connecting to broker or leader, retrying: %s", err.Error())
				return opencdc.Record{}, sdk.ErrBackoffRetry
			}
		}
		return opencdc.Record{}, fmt.Errorf("failed getting a record: %w", err)
	}

	metadata := opencdc.Metadata{}
	metadata.SetCollection(rec.Topic)
	metadata.SetCreatedAt(rec.Timestamp)
	for _, h := range rec.Headers {
		metadata[MetadataKafkaHeaderPrefix+h.Key] = string(h.Value)
	}

	return sdk.Util.Source.NewRecordCreate(
		source.Position{
			GroupID:   s.config.GroupID,
			Topic:     rec.Topic,
			Partition: rec.Partition,
			Offset:    rec.Offset,
		}.ToSDKPosition(),
		metadata,
		opencdc.RawData(rec.Key),
		opencdc.RawData(rec.Value),
	), nil
}

func (s *Source) Ack(ctx context.Context, _ opencdc.Position) error {
	return s.consumer.Ack(ctx)
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.consumer != nil {
		err := s.consumer.Close(ctx)
		if err != nil {
			return fmt.Errorf("failed closing Kafka consumer: %w", err)
		}
	}
	return nil
}
