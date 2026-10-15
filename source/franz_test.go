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

package source

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/conduitio/conduit-connector-kafka/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"go.uber.org/mock/gomock"
)

func TestFranzConsumer_Opts(t *testing.T) {
	is := is.New(t)

	clientCert, clientKey, caCert := test.Certificates(t)

	cfg := Config{
		Config: common.Config{
			Servers:  []string{"test-host:9092"},
			ClientID: "test-client-id",

			ConfigSASL: common.ConfigSASL{
				Mechanism: "PLAIN",
				Username:  "user",
				Password:  "pass",
			},
			ConfigTLS: common.ConfigTLS{
				ClientCert: clientCert,
				ClientKey:  clientKey,
				CACert:     caCert,
			},
		},
		Topics:  []string{"test-topic"},
		GroupID: "test-group-id",
	}

	c, err := NewFranzConsumer(context.Background(), cfg)
	is.NoErr(err)

	is.Equal(c.client.OptValue(kgo.ConsumeTopics), map[string]*regexp.Regexp{cfg.Topics[0]: nil})
	is.Equal(c.client.OptValue(kgo.ConsumerGroup), cfg.GroupID)

	is.Equal(c.client.OptValue(kgo.ClientID), cfg.ClientID)
	is.Equal(cmp.Diff(c.client.OptValue(kgo.DialTLSConfig), cfg.TLS(), cmpopts.IgnoreUnexported(tls.Config{})), "")
	is.Equal(c.client.OptValue(kgo.SASL).([]sasl.Mechanism)[0].Name(), cfg.SASL().Name())
}

func Test_FranzConsumer_Consume_Success(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	cl := NewMockClient(gomock.NewController(t))
	cl.EXPECT().
		PollFetches(gomock.Any()).
		Return([]kgo.Fetch{{
			Topics: []kgo.FetchTopic{{
				Topic: "test",
				Partitions: []kgo.FetchPartition{{
					Partition: 0,
					Err:       nil,
					Records: []*kgo.Record{
						{
							Key:       []byte("hi"),
							Value:     []byte("hello"),
							Partition: 0,
							Offset:    1,
							Timestamp: time.Now(),
							Topic:     "test",
						},
					},
				}},
			}},
		}}).
		Times(1)

	c := &FranzConsumer{
		client:               cl,
		acker:                newBatchAcker(ctx, cl, 1000, time.Second),
		iter:                 &kgo.FetchesRecordIter{},
		retryGroupJoinErrors: true,
	}

	r, err := c.Consume(ctx)
	is.NoErr(err)
	is.Equal(r.Key, []byte("hi"))
	is.Equal(r.Value, []byte("hello"))
}

func Test_FranzConsumer_Consume_RetryGroupJoinError(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	cl := NewMockClient(gomock.NewController(t))
	cl.EXPECT().
		PollFetches(gomock.Any()).
		Return([]kgo.Fetch{{
			Topics: []kgo.FetchTopic{{
				Topic: "",
				Partitions: []kgo.FetchPartition{{
					Partition: 0,
					Err:       errors.New("unable to join group session: unable to dial: dial tcp 127.0.0.1:9092: connect: connection refused"),
				}},
			}},
		}}).
		Times(1)

	c := &FranzConsumer{
		client:               cl,
		acker:                newBatchAcker(ctx, cl, 1000, time.Second),
		iter:                 &kgo.FetchesRecordIter{},
		retryGroupJoinErrors: true,
	}

	_, err := c.Consume(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))
}

func Test_FranzConsumer_Consume_LogsEveryFetchError(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())

	cl := NewMockClient(gomock.NewController(t))
	cl.EXPECT().PollFetches(gomock.Any()).Return(kgo.Fetches{
		{Topics: []kgo.FetchTopic{{Topic: "", Partitions: []kgo.FetchPartition{{Partition: -1, Err: errors.New("unable to join group session: boom")}}}}},
		{Topics: []kgo.FetchTopic{{Topic: "orders", Partitions: []kgo.FetchPartition{{Partition: 3, Err: errors.New("authorization failed: second")}}}}},
	})

	c := &FranzConsumer{
		client:               cl,
		acker:                newBatchAcker(ctx, cl, 1000, time.Second),
		iter:                 &kgo.FetchesRecordIter{},
		retryGroupJoinErrors: true,
	}

	_, err := c.Consume(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry)) // group-error path unchanged
	is.True(strings.Contains(buf.String(), "boom"))
	is.True(strings.Contains(buf.String(), "authorization failed: second")) // not swallowed
	is.True(strings.Contains(buf.String(), `"partition":3`))
}

func Test_FranzConsumer_Consume_ShutdownErrorsNotLogged(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"context canceled", context.Canceled},
		{"deadline exceeded", context.DeadlineExceeded},
		{"client closed", kgo.ErrClientClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			var buf bytes.Buffer
			ctx := zerolog.New(&buf).WithContext(context.Background())

			cl := NewMockClient(gomock.NewController(t))
			// the shape franz-go injects: no topic, one partition
			cl.EXPECT().PollFetches(gomock.Any()).Return(kgo.Fetches{
				{Topics: []kgo.FetchTopic{{Topic: "", Partitions: []kgo.FetchPartition{{Partition: 0, Err: tc.err}}}}},
			})

			c := &FranzConsumer{
				client:               cl,
				acker:                newBatchAcker(ctx, cl, 1000, time.Second),
				iter:                 &kgo.FetchesRecordIter{},
				retryGroupJoinErrors: true,
			}

			_, err := c.Consume(ctx)
			is.True(errors.Is(err, tc.err)) // returned unchanged
			is.True(!strings.Contains(buf.String(), "fetch error"))
		})
	}
}
