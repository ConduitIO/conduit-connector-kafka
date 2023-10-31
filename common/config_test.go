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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestConfig_TryDial(t *testing.T) {
	is := is.New(t)
	t.Parallel()

	cfg := Config{
		Servers: []string{"localhost:12345"}, // Kafka is not running on this port
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := cfg.TryDial(ctx)

	var opErr *net.OpError
	is.True(errors.As(err, &opErr))

	is.Equal(opErr.Op, "dial")
	is.Equal(opErr.Net, "tcp")
}
