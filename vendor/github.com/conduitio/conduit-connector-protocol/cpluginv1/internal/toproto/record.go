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

package toproto

import (
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	opencdcv1 "go.buf.build/grpc/go/conduitio/conduit-connector-protocol/opencdc/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var cTypes [1]struct{}
	_ = cTypes[int(cpluginv1.OperationCreate)-int(opencdcv1.Operation_OPERATION_CREATE)]
	_ = cTypes[int(cpluginv1.OperationUpdate)-int(opencdcv1.Operation_OPERATION_UPDATE)]
	_ = cTypes[int(cpluginv1.OperationDelete)-int(opencdcv1.Operation_OPERATION_DELETE)]
	_ = cTypes[int(cpluginv1.OperationSnapshot)-int(opencdcv1.Operation_OPERATION_SNAPSHOT)]
}

func Record(record cpluginv1.Record) (*opencdcv1.Record, error) {
	key, err := Data(record.Key)
	if err != nil {
		return nil, fmt.Errorf("error converting key: %w", err)
	}

	payload, err := Change(record.Payload)
	if err != nil {
		return nil, fmt.Errorf("error converting payload: %w", err)
	}

	out := opencdcv1.Record{
		Position:  record.Position,
		Operation: opencdcv1.Operation(record.Operation),
		Metadata:  record.Metadata,
		Key:       key,
		Payload:   payload,
	}
	return &out, nil
}

func Change(in cpluginv1.Change) (*opencdcv1.Change, error) {
	before, err := Data(in.Before)
	if err != nil {
		return nil, fmt.Errorf("error converting before: %w", err)
	}

	after, err := Data(in.After)
	if err != nil {
		return nil, fmt.Errorf("error converting after: %w", err)
	}

	out := opencdcv1.Change{
		Before: before,
		After:  after,
	}
	return &out, nil
}

func Data(in cpluginv1.Data) (*opencdcv1.Data, error) {
	if in == nil {
		return nil, nil
	}

	var out opencdcv1.Data

	switch v := in.(type) {
	case cpluginv1.RawData:
		out.Data = &opencdcv1.Data_RawData{
			RawData: v,
		}
	case cpluginv1.StructuredData:
		content, err := structpb.NewStruct(v)
		if err != nil {
			return nil, err
		}
		out.Data = &opencdcv1.Data_StructuredData{
			StructuredData: content,
		}
	default:
		return nil, errors.New("invalid Data type")
	}

	return &out, nil
}
