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

func NewSpecifierPluginServer(impl cpluginv1.SpecifierPlugin) connectorv1.SpecifierPluginServer {
	return &specifierPluginServer{impl: impl}
}

type specifierPluginServer struct {
	connectorv1.UnimplementedSpecifierPluginServer
	impl cpluginv1.SpecifierPlugin
}

func (s specifierPluginServer) Specify(ctx context.Context, protoReq *connectorv1.Specifier_Specify_Request) (*connectorv1.Specifier_Specify_Response, error) {
	goReq, err := fromproto.SpecifierSpecifyRequest(protoReq)
	if err != nil {
		return nil, err
	}
	goResp, err := s.impl.Specify(ctx, goReq)
	if err != nil {
		return nil, err
	}
	protoResp, err := toproto.SpecifierSpecifyResponse(goResp)
	if err != nil {
		return nil, err
	}
	return protoResp, nil
}
