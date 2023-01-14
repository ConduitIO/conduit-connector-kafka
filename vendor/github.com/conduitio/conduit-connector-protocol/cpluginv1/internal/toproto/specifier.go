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
	"fmt"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	connectorv1 "go.buf.build/grpc/go/conduitio/conduit-connector-protocol/connector/v1"
)

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var vTypes [1]struct{}
	_ = vTypes[int(cpluginv1.ValidationTypeRequired)-int(connectorv1.Specifier_Parameter_Validation_TYPE_REQUIRED)]
	_ = vTypes[int(cpluginv1.ValidationTypeLessThan)-int(connectorv1.Specifier_Parameter_Validation_TYPE_LESS_THAN)]
	_ = vTypes[int(cpluginv1.ValidationTypeGreaterThan)-int(connectorv1.Specifier_Parameter_Validation_TYPE_GREATER_THAN)]
	_ = vTypes[int(cpluginv1.ValidationTypeInclusion)-int(connectorv1.Specifier_Parameter_Validation_TYPE_INCLUSION)]
	_ = vTypes[int(cpluginv1.ValidationTypeExclusion)-int(connectorv1.Specifier_Parameter_Validation_TYPE_EXCLUSION)]
	_ = vTypes[int(cpluginv1.ValidationTypeRegex)-int(connectorv1.Specifier_Parameter_Validation_TYPE_REGEX)]
	// parameter types
	_ = vTypes[int(cpluginv1.ParameterTypeString)-int(connectorv1.Specifier_Parameter_TYPE_STRING)]
	_ = vTypes[int(cpluginv1.ParameterTypeInt)-int(connectorv1.Specifier_Parameter_TYPE_INT)]
	_ = vTypes[int(cpluginv1.ParameterTypeFloat)-int(connectorv1.Specifier_Parameter_TYPE_FLOAT)]
	_ = vTypes[int(cpluginv1.ParameterTypeBool)-int(connectorv1.Specifier_Parameter_TYPE_BOOL)]
	_ = vTypes[int(cpluginv1.ParameterTypeFile)-int(connectorv1.Specifier_Parameter_TYPE_FILE)]
	_ = vTypes[int(cpluginv1.ParameterTypeDuration)-int(connectorv1.Specifier_Parameter_TYPE_DURATION)]
}

func SpecifierSpecifyRequest(in cpluginv1.SpecifierSpecifyRequest) (*connectorv1.Specifier_Specify_Request, error) {
	return &connectorv1.Specifier_Specify_Request{}, nil
}

func SpecifierSpecifyResponse(in cpluginv1.SpecifierSpecifyResponse) (*connectorv1.Specifier_Specify_Response, error) {
	specMap := func(in map[string]cpluginv1.SpecifierParameter) (map[string]*connectorv1.Specifier_Parameter, error) {
		out := make(map[string]*connectorv1.Specifier_Parameter, len(in))
		var err error
		for k, v := range in {
			out[k], err = SpecifierParameter(v)
			if err != nil {
				return nil, fmt.Errorf("error converting SpecifierParameter %q: %w", k, err)
			}
		}
		return out, nil
	}

	sourceParams, err := specMap(in.SourceParams)
	if err != nil {
		return nil, fmt.Errorf("error converting SourceSpec: %w", err)
	}

	destinationParams, err := specMap(in.DestinationParams)
	if err != nil {
		return nil, fmt.Errorf("error converting DestinationSpec: %w", err)
	}

	out := connectorv1.Specifier_Specify_Response{
		Name:              in.Name,
		Summary:           in.Summary,
		Description:       in.Description,
		Version:           in.Version,
		Author:            in.Author,
		DestinationParams: destinationParams,
		SourceParams:      sourceParams,
	}
	return &out, nil
}

func SpecifierParameter(in cpluginv1.SpecifierParameter) (*connectorv1.Specifier_Parameter, error) {
	out := connectorv1.Specifier_Parameter{
		Default:     in.Default,
		Required:    in.Required, //nolint: staticcheck // still supported for now
		Description: in.Description,
		Type:        connectorv1.Specifier_Parameter_Type(in.Type),
		Validations: SpecifierParameterValidations(in.Validations),
	}
	return &out, nil
}

func SpecifierParameterValidations(in []cpluginv1.ParameterValidation) []*connectorv1.Specifier_Parameter_Validation {
	out := make([]*connectorv1.Specifier_Parameter_Validation, len(in))
	for i, v := range in {
		out[i] = &connectorv1.Specifier_Parameter_Validation{
			Type:  connectorv1.Specifier_Parameter_Validation_Type(v.Type),
			Value: v.Value,
		}
	}
	return out
}
