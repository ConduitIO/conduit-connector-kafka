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

package fromproto

import (
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	opencdcv1 "go.buf.build/grpc/go/conduitio/conduit-connector-protocol/opencdc/v1"
)

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var cTypes [1]struct{}
	_ = cTypes[int(cpluginv1.OperationCreate)-int(opencdcv1.Operation_OPERATION_CREATE)]
	_ = cTypes[int(cpluginv1.OperationUpdate)-int(opencdcv1.Operation_OPERATION_UPDATE)]
	_ = cTypes[int(cpluginv1.OperationDelete)-int(opencdcv1.Operation_OPERATION_DELETE)]
	_ = cTypes[int(cpluginv1.OperationSnapshot)-int(opencdcv1.Operation_OPERATION_SNAPSHOT)]
}

func Record(record *opencdcv1.Record) (cpluginv1.Record, error) {
	key, err := Data(record.Key)
	if err != nil {
		return cpluginv1.Record{}, fmt.Errorf("error converting key: %w", err)
	}

	payload, err := Change(record.Payload)
	if err != nil {
		return cpluginv1.Record{}, fmt.Errorf("error converting payload: %w", err)
	}

	out := cpluginv1.Record{
		Position:  record.Position,
		Operation: cpluginv1.Operation(record.Operation),
		Metadata:  record.Metadata,
		Key:       key,
		Payload:   payload,
	}
	return out, nil
}

func Change(in *opencdcv1.Change) (cpluginv1.Change, error) {
	before, err := Data(in.Before)
	if err != nil {
		return cpluginv1.Change{}, fmt.Errorf("error converting before: %w", err)
	}

	after, err := Data(in.After)
	if err != nil {
		return cpluginv1.Change{}, fmt.Errorf("error converting after: %w", err)
	}

	out := cpluginv1.Change{
		Before: before,
		After:  after,
	}
	return out, nil
}

func Data(in *opencdcv1.Data) (cpluginv1.Data, error) {
	d := in.GetData()
	if d == nil {
		return nil, nil
	}

	switch v := d.(type) {
	case *opencdcv1.Data_RawData:
		return cpluginv1.RawData(v.RawData), nil
	case *opencdcv1.Data_StructuredData:
		return cpluginv1.StructuredData(v.StructuredData.AsMap()), nil
	default:
		return nil, errors.New("invalid Data type")
	}
}
