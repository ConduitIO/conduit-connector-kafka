// Copyright © 2026 Meroxa, Inc.
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

package common

import (
	"errors"
	"fmt"
)

// CodeSASLRequiresTLS is the stable error code returned when SASL is
// configured without TLS and without the saslAllowPlaintext opt-out.
const CodeSASLRequiresTLS = "KAFKA_SASL_REQUIRES_TLS"

// ErrSASLRequiresTLS is wrapped by the error returned for CodeSASLRequiresTLS.
var ErrSASLRequiresTLS = errors.New("SASL requires TLS")

// CodedError is a configuration error with a stable, machine-readable code,
// the configuration key that needs to change and a suggested fix.
type CodedError struct {
	// Code is stable across releases and safe to match on.
	Code string
	// ConfigPath is the configuration key the error is about.
	ConfigPath string
	Message    string
	// Fix is a suggested remediation.
	Fix string

	err error
}

func (e *CodedError) Error() string {
	return fmt.Sprintf("%s: %s (config: %s). Fix: %s", e.Code, e.Message, e.ConfigPath, e.Fix)
}

// Unwrap returns the sentinel error this error was created for.
func (e *CodedError) Unwrap() error { return e.err }
