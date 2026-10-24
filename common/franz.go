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

package common

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/conduitio/conduit-commons/dialer"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// WithFranzClientOpts lets you specify custom kafka client options (meant for
// test purposes).
func (c Config) WithFranzClientOpts(opts ...kgo.Opt) Config {
	c.franzClientOpts = append(c.franzClientOpts, opts...)
	return c
}

// dialTimeout is franz-go's default dial timeout. It is applied to the host
// dialer path so a host-supplied dialer sees the same bound as the default one.
const dialTimeout = 10 * time.Second

// FranzClientOpts returns the kafka client options derived from the common config.
//
// If the host put a dialer on ctx (see [dialer.WithDialContext]), every broker
// connection the returned client opens uses it: the seed brokers, brokers
// discovered later through metadata, and reconnects after a connection is lost.
// The dialer is captured here, when the client options are built, so it stays in
// effect after ctx ends. Without a host dialer the franz-go default is kept.
func (c Config) FranzClientOpts(ctx context.Context, logger *zerolog.Logger) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Servers...),
		kgo.ClientID(c.ClientID),
	}
	opts = append(opts, c.franzClientOpts...)
	if logger.GetLevel() != zerolog.Disabled {
		opts = append(opts, kgo.WithLogger(franzLogger{logger: logger}))
	}
	if sasl := c.SASL(); sasl != nil {
		opts = append(opts, kgo.SASL(sasl))
	}
	tlsCfg := c.TLS()
	if dial, ok := dialer.FromContext(ctx); ok {
		// Host dialer: installed on every client built from these options.
		// kgo rejects Dialer combined with DialTLSConfig, so TLS is layered
		// over the host's raw connection here instead.
		opts = append(opts, kgo.Dialer(tlsDialer(dial, tlsCfg)))
	} else if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	return opts
}

// tlsDialer returns a kgo dial function that opens the raw connection with
// dial and, if tlsCfg is not nil, runs a TLS handshake over it. It mirrors what
// kgo does for DialTLSConfig: the config is cloned per dial, ServerName
// defaults to the host part of the broker address, and the dial is bounded by
// kgo's default 10s timeout.
func tlsDialer(dial dialer.DialContextFunc, tlsCfg *tls.Config) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()

		var cfg *tls.Config
		if tlsCfg != nil {
			cfg = tlsCfg.Clone()
			if cfg.ServerName == "" {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return nil, fmt.Errorf("unable to split host:port for dialing: %w", err)
				}
				cfg.ServerName = host
			}
		}

		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if cfg == nil {
			return conn, nil
		}
		tlsConn := tls.Client(conn, cfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
}

type franzLogger struct {
	logger *zerolog.Logger
}

func (f franzLogger) Level() kgo.LogLevel {
	return kgo.LogLevelWarn // only log warnings and errors
}

func (f franzLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	lvl := f.toZerologLevel(level)

	e := f.logger.WithLevel(lvl)
	if !e.Enabled() {
		return
	}
	for i := 0; i < len(keyvals); i += 2 {
		key := keyvals[0]
		var val any
		if len(keyvals) > i+1 {
			val = keyvals[i]
		}

		keyStr, ok := key.(string)
		if !ok {
			keyStr = fmt.Sprint(key)
		}
		e.Any(keyStr, val)
	}
	e.Msg(msg)
}

func (f franzLogger) toZerologLevel(level kgo.LogLevel) zerolog.Level {
	switch level {
	case kgo.LogLevelNone:
		return zerolog.Disabled
	case kgo.LogLevelError:
		return zerolog.ErrorLevel
	case kgo.LogLevelWarn:
		return zerolog.WarnLevel
	case kgo.LogLevelInfo:
		return zerolog.InfoLevel
	case kgo.LogLevelDebug:
		return zerolog.DebugLevel
	default:
		return zerolog.NoLevel
	}
}
