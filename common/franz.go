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
	"fmt"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// WithFranzClientOpts lets you specify custom kafka client options (meant for
// test purposes).
func (c Config) WithFranzClientOpts(opts ...kgo.Opt) Config {
	c.franzClientOpts = append(c.franzClientOpts, opts...)
	return c
}

// FranzClientOpts returns the kafka client options derived from the common config.
func (c Config) FranzClientOpts(logger *zerolog.Logger) []kgo.Opt {
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
	if tls := c.TLS(); tls != nil {
		opts = append(opts, kgo.DialTLSConfig(tls))
	}
	return opts
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
