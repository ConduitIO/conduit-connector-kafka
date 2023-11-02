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

package destination

import (
	"context"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-kafka/test"
	"github.com/matryer/is"
)

func TestFranzProducer_Produce(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	ctx := context.Background()

	cfg := test.ParseConfigMap[Config](t, test.DestinationConfigMap(t))
	cfg.Config = test.ConfigWithIntegrationTestOptions(cfg.Config)

	p, err := NewFranzProducer(ctx, cfg)
	is.NoErr(err)
	defer func() {
		err := p.Close(ctx)
		is.NoErr(err)
	}()

	wantRecords := test.GenerateSDKRecords(1, 6)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	count, err := p.Produce(ctx, wantRecords)
	is.NoErr(err)
	is.Equal(count, len(wantRecords))

	gotRecords := test.Consume(t, cfg.Config, len(wantRecords))
	is.Equal(len(wantRecords), len(gotRecords))
	for i, got := range gotRecords {
		is.Equal(got.Value, wantRecords[i].Bytes())
	}
}
