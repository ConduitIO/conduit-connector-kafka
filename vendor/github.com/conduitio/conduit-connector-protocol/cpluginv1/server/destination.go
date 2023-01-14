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

package server

import (
	"context"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/conduitio/conduit-connector-protocol/cpluginv1/internal/fromproto"
	"github.com/conduitio/conduit-connector-protocol/cpluginv1/internal/toproto"
	connectorv1 "go.buf.build/grpc/go/conduitio/conduit-connector-protocol/connector/v1"
)

func NewDestinationPluginServer(impl cpluginv1.DestinationPlugin) connectorv1.DestinationPluginServer {
	return &destinationPluginServer{impl: impl}
}

type destinationPluginServer struct {
	connectorv1.UnimplementedDestinationPluginServer
	impl cpluginv1.DestinationPlugin
}

func (s *destinationPluginServer) Configure(ctx context.Context, protoReq *connectorv1.Destination_Configure_Request) (*connectorv1.Destination_Configure_Response, error) {
	goReq, err := fromproto.DestinationConfigureRequest(protoReq)
	if err != nil {
		return nil, err
	}
	goResp, err := s.impl.Configure(ctx, goReq)
	if err != nil {
		return nil, err
	}
	protoResp, err := toproto.DestinationConfigureResponse(goResp)
	if err != nil {
		return nil, err
	}
	return protoResp, nil
}
func (s *destinationPluginServer) Start(ctx context.Context, protoReq *connectorv1.Destination_Start_Request) (*connectorv1.Destination_Start_Response, error) {
	goReq, err := fromproto.DestinationStartRequest(protoReq)
	if err != nil {
		return nil, err
	}
	goResp, err := s.impl.Start(ctx, goReq)
	if err != nil {
		return nil, err
	}
	protoResp, err := toproto.DestinationStartResponse(goResp)
	if err != nil {
		return nil, err
	}
	return protoResp, nil
}
func (s *destinationPluginServer) Stop(ctx context.Context, protoReq *connectorv1.Destination_Stop_Request) (*connectorv1.Destination_Stop_Response, error) {
	goReq, err := fromproto.DestinationStopRequest(protoReq)
	if err != nil {
		return nil, err
	}
	goResp, err := s.impl.Stop(ctx, goReq)
	if err != nil {
		return nil, err
	}
	protoResp, err := toproto.DestinationStopResponse(goResp)
	if err != nil {
		return nil, err
	}
	return protoResp, nil
}
func (s *destinationPluginServer) Teardown(ctx context.Context, protoReq *connectorv1.Destination_Teardown_Request) (*connectorv1.Destination_Teardown_Response, error) {
	goReq, err := fromproto.DestinationTeardownRequest(protoReq)
	if err != nil {
		return nil, err
	}
	goResp, err := s.impl.Teardown(ctx, goReq)
	if err != nil {
		return nil, err
	}
	protoResp, err := toproto.DestinationTeardownResponse(goResp)
	if err != nil {
		return nil, err
	}
	return protoResp, nil
}
func (s *destinationPluginServer) Run(stream connectorv1.DestinationPlugin_RunServer) error {
	err := s.impl.Run(stream.Context(), &destinationRunStream{impl: stream})
	if err != nil {
		return err
	}
	return nil
}

type destinationRunStream struct {
	impl connectorv1.DestinationPlugin_RunServer
}

func (s *destinationRunStream) Send(in cpluginv1.DestinationRunResponse) error {
	out, err := toproto.DestinationRunResponse(in)
	if err != nil {
		return err
	}
	return s.impl.Send(out)
}

func (s *destinationRunStream) Recv() (cpluginv1.DestinationRunRequest, error) {
	in, err := s.impl.Recv()
	if err != nil {
		return cpluginv1.DestinationRunRequest{}, err
	}
	out, err := fromproto.DestinationRunRequest(in)
	if err != nil {
		return cpluginv1.DestinationRunRequest{}, err
	}
	return out, nil
}
