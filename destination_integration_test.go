// Copyright © 2024 Meroxa, Inc.
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

package redpanda

import (
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-redpanda/destination"
	"github.com/conduitio-labs/conduit-connector-redpanda/source"
	"github.com/conduitio-labs/conduit-connector-redpanda/test"
	"github.com/matryer/is"
)

func TestDestination_Integration_WriteExistingTopic(t *testing.T) {
	cfgMap := test.DestinationConfigMap(t)
	cfg := test.ParseConfigMap[destination.Config](t, cfgMap)

	test.CreateTopics(t, cfg.Servers, []string{cfg.Topic})
	testDestinationIntegrationWrite(t, cfgMap)
}

func TestDestination_Integration_WriteCreateTopic(t *testing.T) {
	cfgMap := test.DestinationConfigMap(t)
	testDestinationIntegrationWrite(t, cfgMap)
}

func testDestinationIntegrationWrite(t *testing.T, cfg map[string]string) {
	is := is.New(t)
	ctx := context.Background()

	wantRecords := test.GenerateSDKRecords(1, 6)

	underTest := NewDestination()
	defer func() {
		err := underTest.Teardown(ctx)
		is.NoErr(err)
	}()

	err := underTest.Configure(ctx, cfg)
	is.NoErr(err)

	err = underTest.Open(ctx)
	is.NoErr(err)

	count, err := underTest.Write(ctx, wantRecords)
	is.NoErr(err)
	is.Equal(count, len(wantRecords))

	// source config needs "topics" param
	cfg["topics"] = cfg["topic"]
	cfg["topic"] = ""

	srcCfg := test.ParseConfigMap[source.Config](t, cfg)
	gotRecords := test.Consume(t, srcCfg.Servers, srcCfg.Topics[0], len(wantRecords))
	is.Equal(len(wantRecords), len(gotRecords))
	for i, got := range gotRecords {
		is.Equal(got.Value, wantRecords[i].Bytes())
	}
}
