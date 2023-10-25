// Copyright © 2022 Meroxa, Inc.
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

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-kafka/config"
	"github.com/conduitio/conduit-connector-kafka/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func TestDestination_Integration_WriteExistingTopic(t *testing.T) {
	cfgMap := test.DestinationConfigMap(t)
	cfg := test.ParseConfigMap[config.Config](t, cfgMap)

	test.CreateTopic(t, cfg)
	testDestinationIntegrationWrite(t, cfgMap)
}

func TestDestination_Integration_WriteCreateTopic(t *testing.T) {
	cfgMap := test.DestinationConfigMap(t)
	testDestinationIntegrationWrite(t, cfgMap)
}

func testDestinationIntegrationWrite(t *testing.T, cfg map[string]string) {
	is := is.New(t)
	ctx := context.Background()

	record := sdk.Record{
		Operation: sdk.OperationUpdate,
		Position:  []byte(uuid.NewString()),
		Metadata: map[string]string{
			"foo": "bar",
		},
		Key: sdk.RawData(uuid.NewString()),
		Payload: sdk.Change{
			Before: sdk.RawData(fmt.Sprintf("test before %s", time.Now())),
			After:  sdk.RawData(fmt.Sprintf("test after %s", time.Now())),
		},
	}

	underTest := NewDestination()
	defer func() {
		err := underTest.Teardown(ctx)
		is.NoErr(err)
	}()

	err := underTest.Configure(ctx, cfg)
	is.NoErr(err)

	err = underTest.Open(ctx)
	is.NoErr(err)

	count, err := underTest.Write(ctx, []sdk.Record{record})
	is.NoErr(err)
	is.Equal(count, 1)

	recs := test.Consume(t, test.ParseConfigMap[config.Config](t, cfg), 1)
	is.Equal(record.Bytes(), recs[0].Value)
}
