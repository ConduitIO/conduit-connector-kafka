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

//go:generate mockgen -destination=mock/destination.go -package=mock -mock_names=DestinationPlugin=DestinationPlugin,DestinationRunStream=DestinationRunStream . DestinationPlugin,DestinationRunStream

package cpluginv1

import (
	"context"
)

type DestinationPlugin interface {
	Configure(context.Context, DestinationConfigureRequest) (DestinationConfigureResponse, error)
	Start(context.Context, DestinationStartRequest) (DestinationStartResponse, error)
	Run(context.Context, DestinationRunStream) error
	Stop(context.Context, DestinationStopRequest) (DestinationStopResponse, error)
	Teardown(context.Context, DestinationTeardownRequest) (DestinationTeardownResponse, error)
}

type DestinationConfigureRequest struct {
	Config map[string]string
}
type DestinationConfigureResponse struct{}

type DestinationStartRequest struct{}
type DestinationStartResponse struct{}

type DestinationRunStream interface {
	Send(DestinationRunResponse) error
	Recv() (DestinationRunRequest, error)
}
type DestinationRunRequest struct {
	Record Record
}
type DestinationRunResponse struct {
	AckPosition []byte
	Error       string
}

type DestinationStopRequest struct {
	LastPosition []byte
}
type DestinationStopResponse struct{}

type DestinationTeardownRequest struct{}
type DestinationTeardownResponse struct{}
