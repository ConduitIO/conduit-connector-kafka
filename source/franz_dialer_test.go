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

package source

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/dialer"
	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kfake"
)

var errDialRefused = errors.New("refused by test dialer")

func TestFranzConsumer_HostDialerRefuses(t *testing.T) {
	is := is.New(t)
	addr, accepted := test.CountingListener(t)
	rd := &test.RecordingDialer{Err: errDialRefused}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = dialer.WithDialContext(ctx, rd.DialContext)

	cfg := Config{Config: common.Config{Servers: []string{addr}}, Topics: []string{"t"}, GroupID: "g"}
	c, err := NewFranzConsumer(ctx, cfg)
	is.NoErr(err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	// Admin (metadata) path.
	_, err = c.PartitionCount(ctx, "t")
	is.True(errors.Is(err, errDialRefused))

	// Consumer-group / fetch path.
	_, _ = c.Consume(ctx)

	is.True(len(rd.Addrs()) > 0)
	for _, a := range rd.Addrs() {
		is.Equal(a, addr)
	}
	is.Equal(accepted(), 0) // the real listener was never reached
}

func TestFranzConsumer_HostDialerUsed(t *testing.T) {
	is := is.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "t"))
	is.NoErr(err)
	t.Cleanup(c.Close)
	rd := &test.RecordingDialer{}
	ctx := dialer.WithDialContext(context.Background(), rd.DialContext)

	cfg := Config{Config: common.Config{Servers: c.ListenAddrs()}, Topics: []string{"t"}, GroupID: "g"}
	cons, err := NewFranzConsumer(ctx, cfg)
	is.NoErr(err)
	t.Cleanup(func() { _ = cons.Close(context.Background()) })

	n, err := cons.PartitionCount(ctx, "t")
	is.NoErr(err)
	is.Equal(n, int32(2))
	is.Equal(rd.Addrs()[0], c.ListenAddrs()[0])
}

func TestFranzConsumer_NoHostDialer(t *testing.T) {
	is := is.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "t"))
	is.NoErr(err)
	t.Cleanup(c.Close)

	cfg := Config{Config: common.Config{Servers: c.ListenAddrs()}, Topics: []string{"t"}, GroupID: "g"}
	cons, err := NewFranzConsumer(context.Background(), cfg)
	is.NoErr(err)
	t.Cleanup(func() { _ = cons.Close(context.Background()) })

	n, err := cons.PartitionCount(context.Background(), "t")
	is.NoErr(err)
	is.Equal(n, int32(2)) // default dialer, behavior unchanged
}
