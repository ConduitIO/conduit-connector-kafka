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

package cpluginv1

const (
	OperationCreate Operation = iota + 1
	OperationUpdate
	OperationDelete
	OperationSnapshot
)

type Operation int

type Record struct {
	Position  []byte
	Operation Operation
	Metadata  map[string]string
	Key       Data
	Payload   Change
}

type Change struct {
	Before Data
	After  Data
}

type Data interface {
	isData()
}

type RawData []byte

func (RawData) isData() {}

type StructuredData map[string]interface{}

func (StructuredData) isData() {}
