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

package test

import (
	"context"
	"net"
	"sync"
	"testing"
)

// RecordingDialer is a dial function for tests. It records every address it is
// asked to dial and then either refuses with Err or, if Err is nil, connects to
// the address with a plain TCP dial and remembers the connection.
type RecordingDialer struct {
	// Err, when set, is returned from every dial and no connection is made.
	Err error

	mu    sync.Mutex
	addrs []string
	conns []net.Conn
}

// DialContext implements dialer.DialContextFunc.
func (d *RecordingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	d.addrs = append(d.addrs, address)
	d.mu.Unlock()
	if d.Err != nil {
		return nil, d.Err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()
	return conn, nil
}

// Addrs returns the addresses dialed so far, in order.
func (d *RecordingDialer) Addrs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addrs...)
}

// DropConns closes every connection the dialer made so far, which simulates
// the connections being lost so that clients have to reconnect.
func (d *RecordingDialer) DropConns() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.conns {
		_ = c.Close()
	}
	d.conns = nil
}

// CountingListener listens on a local port and counts accepted connections. It
// stands in for a broker that must not be reached. The returned func reports
// how many connections were accepted so far.
func CountingListener(t testing.TB) (addr string, accepted func() int) {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu sync.Mutex
		n  int
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			n++
			mu.Unlock()
			_ = c.Close()
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		<-done
	})
	return lis.Addr().String(), func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}
}
