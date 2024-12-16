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
	"fmt"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestConfig_ValidateTopics(t *testing.T) {
	// Note that we are testing custom validations. Required fields and simple
	// validations are already executed by the SDK via parameter specifications.
	testCases := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "valid",
			cfg: Config{
				Topics: []string{"topic1"},
			},
			wantErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			err := tc.cfg.Validate(context.Background())
			fmt.Println(err)
			if tc.wantErr != "" {
				is.True(err != nil)
				is.True(strings.Contains(err.Error(), tc.wantErr))
			} else {
				is.NoErr(err)
				is.Equal(tc.cfg.Topics, []string{"topic1"})
			}
		})
	}
}
