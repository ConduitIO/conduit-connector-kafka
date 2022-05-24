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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"go.uber.org/goleak"

	"testing"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		Servers:           "localhost:9092",
		ReadFromBeginning: "true",
	}

	sdk.AcceptanceTest(t, driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: Specification,
					NewSource:        NewSource,
					NewDestination:   NewDestination,
				},
				SourceConfig:      cfg,
				DestinationConfig: cfg,

				BeforeTest: func(t *testing.T) {
					cfg[Topic] = "TestAcceptance-" + uuid.NewString()
				},
				AfterTest: func(t *testing.T) {
				},
				GoleakOptions: []goleak.Option{
					// kafka.DefaultTransport starts some goroutines: https://github.com/segmentio/kafka-go/issues/599
					goleak.IgnoreTopFunction("github.com/segmentio/kafka-go.(*connPool).discover"),
					goleak.IgnoreTopFunction("github.com/segmentio/kafka-go.(*conn).run"),
				},
			},
		},
	})
}
