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
	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	connectorv1 "go.buf.build/grpc/go/conduitio/conduit-connector-protocol/connector/v1"
)

func DestinationConfigureRequest(in *connectorv1.Destination_Configure_Request) (cpluginv1.DestinationConfigureRequest, error) {
	out := cpluginv1.DestinationConfigureRequest{
		Config: in.Config,
	}
	return out, nil
}

func DestinationStartRequest(in *connectorv1.Destination_Start_Request) (cpluginv1.DestinationStartRequest, error) {
	return cpluginv1.DestinationStartRequest{}, nil
}

func DestinationRunRequest(in *connectorv1.Destination_Run_Request) (cpluginv1.DestinationRunRequest, error) {
	rec, err := Record(in.Record)
	if err != nil {
		return cpluginv1.DestinationRunRequest{}, err
	}
	out := cpluginv1.DestinationRunRequest{
		Record: rec,
	}
	return out, nil
}

func DestinationStopRequest(in *connectorv1.Destination_Stop_Request) (cpluginv1.DestinationStopRequest, error) {
	out := cpluginv1.DestinationStopRequest{
		LastPosition: in.LastPosition,
	}
	return out, nil
}

func DestinationTeardownRequest(in *connectorv1.Destination_Teardown_Request) (cpluginv1.DestinationTeardownRequest, error) {
	return cpluginv1.DestinationTeardownRequest{}, nil
}
