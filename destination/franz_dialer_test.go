// Copyright © 2026 Meroxa, Inc.
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

package destination

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/dialer"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kfake"
)

var errDialRefused = errors.New("refused by test dialer")

func dialerTestConfig(servers []string) Config {
	return Config{
		Config:          common.Config{Servers: servers},
		Topic:           "t",
		Acks:            "all",
		DeliveryTimeout: 2 * time.Second,
		Compression:     "gzip",
		BatchBytes:      1 << 20,
	}
}

func TestFranzProducer_HostDialerRefuses(t *testing.T) {
	is := is.New(t)
	addr, accepted := test.CountingListener(t)
	rd := &test.RecordingDialer{Err: errDialRefused}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = dialer.WithDialContext(ctx, rd.DialContext)

	p, err := NewFranzProducer(ctx, dialerTestConfig([]string{addr}))
	is.NoErr(err)
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	// Produce path.
	_, err = p.Produce(ctx, []opencdc.Record{{Payload: opencdc.Change{After: opencdc.RawData("v")}}})
	is.True(errors.Is(err, errDialRefused))

	// Admin (metadata) path.
	_, err = p.refreshPartitionCount(ctx, "t")
	is.True(errors.Is(err, errDialRefused))

	is.True(len(rd.Addrs()) > 0)
	for _, a := range rd.Addrs() {
		is.Equal(a, addr)
	}
	is.Equal(accepted(), 0) // the real listener was never reached
}

func TestFranzProducer_HostDialerUsed(t *testing.T) {
	is := is.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "t"))
	is.NoErr(err)
	t.Cleanup(c.Close)
	rd := &test.RecordingDialer{}
	ctx := dialer.WithDialContext(context.Background(), rd.DialContext)

	p, err := NewFranzProducer(ctx, dialerTestConfig(c.ListenAddrs()))
	is.NoErr(err)
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	n, err := p.Produce(ctx, []opencdc.Record{{Payload: opencdc.Change{After: opencdc.RawData("v")}}})
	is.NoErr(err)
	is.Equal(n, 1)
	count, err := p.refreshPartitionCount(ctx, "t")
	is.NoErr(err)
	is.Equal(count, int32(2))
	is.Equal(rd.Addrs()[0], c.ListenAddrs()[0])
}

func TestFranzProducer_NoHostDialer(t *testing.T) {
	is := is.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "t"))
	is.NoErr(err)
	t.Cleanup(c.Close)

	p, err := NewFranzProducer(context.Background(), dialerTestConfig(c.ListenAddrs()))
	is.NoErr(err)
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	n, err := p.Produce(context.Background(), []opencdc.Record{{Payload: opencdc.Change{After: opencdc.RawData("v")}}})
	is.NoErr(err)
	is.Equal(n, 1) // default dialer, behavior unchanged
}
