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
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
)

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		"servers":           "localhost:9092",
		"readFromBeginning": "true",
	}

	sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector:         Connector,
			SourceConfig:      cfg,
			DestinationConfig: cfg,

			BeforeTest: func(t *testing.T) {
				cfg["topic"] = "TestAcceptance-" + uuid.NewString()
			},

			Skip: []string{
				// Configure tests are faulty since we rely on paramgen to validate required parameters.
				"TestSource_Configure_RequiredParams",
				"TestDestination_Configure_RequiredParams",
			},

			ReadTimeout: 10 * time.Second,
		},
	})
}
