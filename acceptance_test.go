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
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/conduitio/conduit-connector-kafka/source"
	"github.com/conduitio/conduit-connector-kafka/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
)

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		"servers": "localhost:9092",
		// source params
		"readFromBeginning": "true",
		// destination params
		"batchBytes":  "1000012",
		"acks":        "all",
		"compression": "snappy",
	}

	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,

				BeforeTest: func(t *testing.T) {
					lastSlash := strings.LastIndex(t.Name(), "/")
					cfg["topic"] = t.Name()[lastSlash+1:] + uuid.NewString()
				},

				Skip: []string{
					// Configure tests are faulty since we rely on paramgen to validate required parameters.
					"TestSource_Configure_RequiredParams",
					"TestDestination_Configure_RequiredParams",
				},

				WriteTimeout: time.Second * 10,
				ReadTimeout:  time.Second * 10,
			},
		},
	})
}

type AcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

// ReadFromDestination is overwritten because the source connector uses a consumer
// group which results in slow reads. This speeds up the destination tests.
func (d AcceptanceTestDriver) ReadFromDestination(t *testing.T, records []sdk.Record) []sdk.Record {
	cfg := test.ParseConfigMap[common.Config](t, d.SourceConfig(t))
	kgoRecs := test.Consume(t, cfg, len(records))

	recs := make([]sdk.Record, len(kgoRecs))
	for i, rec := range kgoRecs {
		metadata := sdk.Metadata{MetadataKafkaTopic: rec.Topic}
		metadata.SetCreatedAt(rec.Timestamp)

		recs[i] = sdk.Util.Source.NewRecordCreate(
			source.Position{
				GroupID:   "",
				Topic:     rec.Topic,
				Partition: rec.Partition,
				Offset:    rec.Offset,
			}.ToSDKPosition(),
			metadata,
			sdk.RawData(rec.Key),
			sdk.RawData(rec.Value),
		)
	}
	return recs
}
