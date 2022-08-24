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
	"errors"
	"fmt"
	"testing"

	"github.com/matryer/is"
	"github.com/segmentio/kafka-go"
)

func TestNewProducer_MissingRequired(t *testing.T) {
	is := is.New(t)
	testCases := []struct {
		name   string
		config Config
		exp    error
	}{
		{
			name:   "servers missing",
			config: Config{Topic: "topic"},
			exp:    ErrServersMissing,
		},
		{
			name:   "topic missing",
			config: Config{Servers: []string{"irrelevant servers"}},
			exp:    ErrTopicMissing,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			producer, err := NewProducer(tc.config)
			is.True(producer == nil)
			is.True(err != nil)
			is.True(errors.Is(err, tc.exp))
		})
	}
}

func TestBatchSizeAdjustingBalancer_Balance(t *testing.T) {
	w := &kafka.Writer{}
	balancer := &batchSizeAdjustingBalancer{writer: w}

	testCases := []struct {
		r int // record count
		p int // partition count
		b int // want batch size
	}{
		{r: 1, p: 1, b: 1},
		{r: 1, p: 2, b: 1},
		{r: 1, p: 4, b: 1},
		{r: 1, p: 8, b: 1},
		{r: 2, p: 1, b: 2},
		{r: 2, p: 2, b: 1},
		{r: 2, p: 4, b: 1},
		{r: 2, p: 8, b: 1},
		{r: 4, p: 1, b: 4},
		{r: 4, p: 2, b: 2},
		{r: 4, p: 4, b: 1},
		{r: 4, p: 8, b: 1},
		{r: 100, p: 1, b: 100},
		{r: 123, p: 123, b: 1},
		{r: 124, p: 123, b: 2},
		{r: 246, p: 123, b: 2},
		{r: 247, p: 123, b: 3},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("r-%d-p-%d-b-%d", tc.r, tc.p, tc.b), func(t *testing.T) {
			is := is.New(t)
			balancer.SetRecordCount(tc.r)
			balancer.Balance(kafka.Message{}, make([]int, tc.p)...)
			is.Equal(w.BatchSize, tc.b)
			balancer.Balance(kafka.Message{}, 1) // make sure that Balance calculated batch size only in first call
			is.Equal(w.BatchSize, tc.b)          // expected batch size to stay the same
		})
	}
}
