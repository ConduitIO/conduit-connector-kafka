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

package source

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-connector-kafka/common"
	"github.com/matryer/is"
)

func TestConfig_BaseValidationsDone(t *testing.T) {
	// Required fields and simple validations are already executed
	// by the SDK via parameter specifications.
	// Here we are making sure that the base config's validation is done.
	is := is.New(t)
	underTest := Config{
		Config: common.Config{
			ConfigSASL: common.ConfigSASL{
				Password: "password",
			},
		},
	}

	err := underTest.Validate(context.Background())
	is.True(errors.Is(err, common.ErrSASLInvalidAuth))
}
