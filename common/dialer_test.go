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

package common_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/dialer"
	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

var nop = zerolog.Nop()

var errDialRefused = errors.New("refused by test dialer")

func TestTryDial_HostDialerRefuses(t *testing.T) {
	is := is.New(t)
	addr, accepted := test.CountingListener(t)
	rd := &test.RecordingDialer{Err: errDialRefused}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	ctx = dialer.WithDialContext(ctx, rd.DialContext)

	err := common.Config{Servers: []string{addr}}.TryDial(ctx)

	is.True(errors.Is(err, errDialRefused))
	is.Equal(rd.Addrs()[0], addr)
	is.Equal(accepted(), 0) // the real listener was never reached
}

func TestTryDial_HostDialerUsed(t *testing.T) {
	is := is.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	is.NoErr(err)
	t.Cleanup(c.Close)
	rd := &test.RecordingDialer{}
	ctx := dialer.WithDialContext(context.Background(), rd.DialContext)

	is.NoErr(common.Config{Servers: c.ListenAddrs()}.TryDial(ctx))
	is.Equal(rd.Addrs()[0], c.ListenAddrs()[0])
}

func TestTryDial_NoHostDialer(t *testing.T) {
	is := is.New(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	is.NoErr(err)
	t.Cleanup(c.Close)

	// No dialer on the context: the franz-go default dialer connects directly.
	is.NoErr(common.Config{Servers: c.ListenAddrs()}.TryDial(context.Background()))
}

// TestFranzClientOpts_DiscoveredBrokersAndReconnect seeds the client with one
// broker and produces to partitions led by all of them. The dialer must see the
// brokers that were only learned from metadata, and must keep being used for
// reconnects after the context it was read from has ended.
func TestFranzClientOpts_DiscoveredBrokersAndReconnect(t *testing.T) {
	is := is.New(t)
	// Partition leaders are spread over the brokers at random; with this many
	// partitions a broker other than the seed always leads at least one.
	const topic, partitions = "dial-topic", 30
	c, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(partitions, topic))
	is.NoErr(err)
	t.Cleanup(c.Close)
	addrs := c.ListenAddrs()

	rd := &test.RecordingDialer{}
	ctx, cancel := context.WithCancel(dialer.WithDialContext(context.Background(), rd.DialContext))
	opts := common.Config{Servers: addrs[:1]}.FranzClientOpts(ctx, &nop)
	cancel() // the client must not depend on the build context staying alive
	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.DefaultProduceTopic(topic))
	cl, err := kgo.NewClient(opts...)
	is.NoErr(err)
	t.Cleanup(cl.Close)

	produceAll := func() {
		for p := int32(0); p < partitions; p++ {
			res := cl.ProduceSync(context.Background(), &kgo.Record{Partition: p, Value: []byte("v")})
			is.NoErr(res.FirstErr())
		}
	}
	produceAll()
	cluster := map[string]bool{}
	for _, a := range addrs {
		cluster[a] = true
	}
	discovered := false
	for _, a := range rd.Addrs() {
		is.True(cluster[a]) // only cluster brokers, and always via the host dialer
		discovered = discovered || a != addrs[0]
	}
	is.True(discovered) // a broker learned from metadata was dialed through the host dialer

	before := len(rd.Addrs())
	rd.DropConns()
	produceAll()
	is.True(len(rd.Addrs()) > before) // reconnects went through the host dialer too
}

// TestFranzClientOpts_HostDialerWithTLS checks that TLS is still layered over
// the host's raw connection (franz-go refuses Dialer together with
// DialTLSConfig, so the connector does the handshake itself).
func TestFranzClientOpts_HostDialerWithTLS(t *testing.T) {
	is := is.New(t)
	serverCert, certPEM := selfSignedCert(t)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.TLS(&tls.Config{Certificates: []tls.Certificate{serverCert}}))
	is.NoErr(err)
	t.Cleanup(c.Close)

	cfg := common.Config{Servers: c.ListenAddrs()}
	cfg.TLSEnabled = true
	cfg.CACert = certPEM

	rd := &test.RecordingDialer{}
	ctx := dialer.WithDialContext(context.Background(), rd.DialContext)
	is.NoErr(cfg.TryDial(ctx))
	is.Equal(rd.Addrs()[0], c.ListenAddrs()[0])

	// Without the CA the handshake must fail: the dialer path verifies certs.
	bad := common.Config{Servers: c.ListenAddrs()}
	bad.TLSEnabled = true
	ctx2, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	var certErr x509.UnknownAuthorityError
	is.True(errors.As(bad.TryDial(ctx2), &certErr))
}

func selfSignedCert(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	is := is.New(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	is.NoErr(err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "broker.example.test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	is.NoErr(err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key},
		string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
