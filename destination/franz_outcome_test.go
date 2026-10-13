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
	"errors"
	"testing"

	"github.com/matryer/is"
)

func TestProduceOutcome(t *testing.T) {
	bad := errors.New("bad")
	prep := errors.New("prepare")

	testCases := []struct {
		name            string
		results         []error
		prepareErrIndex int
		prepareErr      error
		wantN           int
		wantErr         bool
		wantIs          error
	}{
		{name: "none bad", results: []error{nil, nil, nil}, prepareErrIndex: -1, wantN: 3},
		{name: "empty", results: nil, prepareErrIndex: -1, wantN: 0},
		{name: "bad at 0", results: []error{bad, nil, nil}, prepareErrIndex: -1, wantN: 0, wantErr: true, wantIs: bad},
		{name: "bad in the middle", results: []error{nil, nil, bad, nil, nil}, prepareErrIndex: -1, wantN: 2, wantErr: true, wantIs: bad},
		{name: "bad at last", results: []error{nil, nil, bad}, prepareErrIndex: -1, wantN: 2, wantErr: true, wantIs: bad},
		{name: "multiple bad: first wins", results: []error{nil, bad, nil, bad}, prepareErrIndex: -1, wantN: 1, wantErr: true, wantIs: bad},
		{name: "all bad", results: []error{bad, bad, bad}, prepareErrIndex: -1, wantN: 0, wantErr: true, wantIs: bad},
		{name: "prepare error only", results: []error{nil, nil}, prepareErrIndex: 2, prepareErr: prep, wantN: 2, wantErr: true, wantIs: prep},
		{name: "prepare error at 0", results: nil, prepareErrIndex: 0, prepareErr: prep, wantN: 0, wantErr: true, wantIs: prep},
		{name: "delivery failure before prepare error wins", results: []error{nil, bad}, prepareErrIndex: 2, prepareErr: prep, wantN: 1, wantErr: true, wantIs: bad},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			n, err := produceOutcome(tc.results, tc.prepareErrIndex, tc.prepareErr)
			is.Equal(n, tc.wantN)
			is.Equal(err != nil, tc.wantErr)
			if tc.wantIs != nil {
				is.True(errors.Is(err, tc.wantIs))
			}
		})
	}
}
