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
	"errors"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/hashicorp/go-plugin"
	connectorv1 "go.buf.build/grpc/go/conduitio/conduit-connector-protocol/connector/v1"
	"google.golang.org/grpc"
)

func Serve(
	specifierFactory func() cpluginv1.SpecifierPlugin,
	sourceFactory func() cpluginv1.SourcePlugin,
	destinationFactory func() cpluginv1.DestinationPlugin,
	opts ...ServeOption,
) error {
	serveConfig := &plugin.ServeConfig{
		HandshakeConfig: plugin.HandshakeConfig{
			ProtocolVersion:  1,
			MagicCookieKey:   "CONDUIT_PLUGIN_MAGIC_COOKIE",
			MagicCookieValue: "204e8e812c3a1bb73b838928c575b42a105dd2e9aa449be481bc4590486df53f",
		},
		Plugins: plugin.PluginSet{
			"specifier":   &GRPCSpecifierPlugin{Factory: specifierFactory},
			"source":      &GRPCSourcePlugin{Factory: sourceFactory},
			"destination": &GRPCDestinationPlugin{Factory: destinationFactory},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	}
	for _, opt := range opts {
		err := opt.ApplyServeOption(serveConfig)
		if err != nil {
			return err
		}
	}

	plugin.Serve(serveConfig)
	return nil
}

// ServeOption is an interface for defining options that can be passed to the
// Serve function. Each implementation modifies the ServeConfig being
// generated. A slice of ServeOptions then, cumulatively applied, render a full
// ServeConfig.
type ServeOption interface {
	ApplyServeOption(*plugin.ServeConfig) error
}

type serveConfigFunc func(*plugin.ServeConfig) error

func (s serveConfigFunc) ApplyServeOption(in *plugin.ServeConfig) error {
	return s(in)
}

// WithDebug returns a ServeOption that will set the server into debug mode, using
// the passed options to populate the go-plugin ServeTestConfig.
func WithDebug(ctx context.Context, config chan *plugin.ReattachConfig, closeCh chan struct{}) ServeOption {
	return serveConfigFunc(func(in *plugin.ServeConfig) error {
		in.Test = &plugin.ServeTestConfig{
			Context:          ctx,
			ReattachConfigCh: config,
			CloseCh:          closeCh,
		}
		return nil
	})
}

func WithGRPCServerOptions(opt ...grpc.ServerOption) ServeOption {
	return serveConfigFunc(func(in *plugin.ServeConfig) error {
		in.GRPCServer = func(opts []grpc.ServerOption) *grpc.Server {
			return plugin.DefaultGRPCServer(append(opts, opt...))
		}
		return nil
	})
}

// GRPCSourcePlugin is an implementation of the
// github.com/hashicorp/go-plugin#Plugin and
// github.com/hashicorp/go-plugin#GRPCPlugin interfaces, it's using
// SourcePlugin.
type GRPCSourcePlugin struct {
	plugin.NetRPCUnsupportedPlugin
	Factory func() cpluginv1.SourcePlugin
}

var _ plugin.Plugin = (*GRPCSourcePlugin)(nil)

// GRPCClient always returns an error; we're only implementing the server half
// of the interface.
func (p *GRPCSourcePlugin) GRPCClient(context.Context, *plugin.GRPCBroker, *grpc.ClientConn) (interface{}, error) {
	return nil, errors.New("this package only implements gRPC servers")
}

// GRPCServer registers the gRPC source plugin server with the gRPC server that
// go-plugin is standing up.
func (p *GRPCSourcePlugin) GRPCServer(_ *plugin.GRPCBroker, s *grpc.Server) error {
	connectorv1.RegisterSourcePluginServer(s, NewSourcePluginServer(p.Factory()))
	return nil
}

// GRPCDestinationPlugin is an implementation of the
// github.com/hashicorp/go-plugin#Plugin and
// github.com/hashicorp/go-plugin#GRPCPlugin interfaces, it's using
// cpluginv1.DestinationPlugin.
type GRPCDestinationPlugin struct {
	plugin.NetRPCUnsupportedPlugin
	Factory func() cpluginv1.DestinationPlugin
}

var _ plugin.Plugin = (*GRPCDestinationPlugin)(nil)

// GRPCClient always returns an error; we're only implementing the server half
// of the interface.
func (p *GRPCDestinationPlugin) GRPCClient(context.Context, *plugin.GRPCBroker, *grpc.ClientConn) (interface{}, error) {
	return nil, errors.New("this package only implements gRPC servers")
}

// GRPCServer registers the gRPC destination plugin server with the gRPC server
// that go-plugin is standing up.
func (p *GRPCDestinationPlugin) GRPCServer(_ *plugin.GRPCBroker, s *grpc.Server) error {
	connectorv1.RegisterDestinationPluginServer(s, NewDestinationPluginServer(p.Factory()))
	return nil
}

// GRPCSpecifierPlugin is an implementation of the
// github.com/hashicorp/go-plugin#Plugin and
// github.com/hashicorp/go-plugin#GRPCPlugin interfaces, it's using
// cpluginv1.SpecifierPlugin.
type GRPCSpecifierPlugin struct {
	plugin.NetRPCUnsupportedPlugin
	Factory func() cpluginv1.SpecifierPlugin
}

var _ plugin.Plugin = (*GRPCSpecifierPlugin)(nil)

// GRPCClient always returns an error; we're only implementing the server half
// of the interface.
func (p *GRPCSpecifierPlugin) GRPCClient(context.Context, *plugin.GRPCBroker, *grpc.ClientConn) (interface{}, error) {
	return nil, errors.New("this package only implements gRPC servers")
}

// GRPCServer registers the gRPC specifier plugin server with the gRPC server that
// go-plugin is standing up.
func (p *GRPCSpecifierPlugin) GRPCServer(_ *plugin.GRPCBroker, s *grpc.Server) error {
	connectorv1.RegisterSpecifierPluginServer(s, NewSpecifierPluginServer(p.Factory()))
	return nil
}
