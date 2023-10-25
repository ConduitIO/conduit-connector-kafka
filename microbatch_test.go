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

package kafka

import (
	"fmt"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func TestMicroBatch(t *testing.T) {
	is := is.New(t)

	have := make(MicroBatch, 10)
	for i := range have {
		have[i] = sdk.Record{
			Position:  sdk.Position(fmt.Sprintf("pos-%d", i)),
			Operation: sdk.OperationCreate,
			Metadata: sdk.Metadata{
				"foo":                    uuid.NewString(),
				fmt.Sprintf("foo-%d", i): fmt.Sprintf("special-metadata-for-%d", i),
			},
			Key: sdk.RawData(fmt.Sprintf("key-%d", i)),
			Payload: sdk.Change{
				After: sdk.RawData(fmt.Sprintf("after-%d", i)),
			},
		}
	}

	// merge into one record
	r := have.ToRecord()

	// split batch back into multiple records
	got := make(MicroBatch, 0, len(have))
	got.FromRecord(r)

	is.Equal(cmp.Diff(got, have, cmpopts.IgnoreUnexported(sdk.Record{})), "")
}
