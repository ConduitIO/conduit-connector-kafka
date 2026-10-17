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

package mirror

import (
	"strings"
	"testing"

	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestEncodeDecodeHeaders_RoundTrip(t *testing.T) {
	cases := map[string]struct {
		in  []kgo.RecordHeader
		out []kgo.RecordHeader
	}{
		"empty":    {in: nil, out: []kgo.RecordHeader{}},
		"nil list": {in: []kgo.RecordHeader{}, out: []kgo.RecordHeader{}},
		"plain": {
			in:  []kgo.RecordHeader{{Key: "a", Value: []byte("b")}},
			out: []kgo.RecordHeader{{Key: "a", Value: []byte("b")}},
		},
		"repeated keys": {
			in: []kgo.RecordHeader{
				{Key: "a", Value: []byte("1")},
				{Key: "a", Value: []byte("2")},
			},
			out: []kgo.RecordHeader{
				{Key: "a", Value: []byte("1")},
				{Key: "a", Value: []byte("2")},
			},
		},
		"null vs empty value": {
			in: []kgo.RecordHeader{
				{Key: "n", Value: nil},
				{Key: "e", Value: []byte{}},
			},
			out: []kgo.RecordHeader{
				{Key: "n", Value: nil},
				{Key: "e", Value: []byte{}},
			},
		},
		"binary and non-utf8 key": {
			in: []kgo.RecordHeader{
				{Key: "\xff\xfe\x00", Value: []byte{0x00, 0xff}},
				{Key: "", Value: []byte("empty key")},
			},
			out: []kgo.RecordHeader{
				{Key: "\xff\xfe\x00", Value: []byte{0x00, 0xff}},
				{Key: "", Value: []byte("empty key")},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			enc := EncodeHeaders(tc.in)
			got, err := DecodeHeaders(enc)
			is.NoErr(err)
			is.Equal(got, tc.out)
		})
	}
}

func TestDecodeHeaders_RejectsMalformed(t *testing.T) {
	cases := map[string]string{
		"not json":          `not json`,
		"not an array":      `{"k":"a"}`,
		"unknown field":     `[{"k":"a","v":"b","x":"y"}]`,
		"missing k":         `[{"v":"b"}]`,
		"non-string k":      `[{"k":1,"v":"b"}]`,
		"non-string v":      `[{"k":"a","v":1}]`,
		"url base64 key":    `[{"k":"a_b","v":"b"}]`,
		"missing padding":   `[{"k":"a","v":"b"}]`, // "b" is not padded base64
		"trailing data":     `[{"k":"a","v":"b"}] x`,
		"whitespace in b64": `[{"k":"aG Vs","v":"b"}]`,
	}

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			_, err := DecodeHeaders(in)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "invalid kafka.headers"))
		})
	}
}

func TestParsePartition(t *testing.T) {
	is := is.New(t)
	for _, s := range []string{"0", "3", "2147483647"} {
		_, err := ParsePartition(s)
		is.NoErr(err)
	}
	for _, s := range []string{"", "abc", "-1", "1.5"} {
		_, err := ParsePartition(s)
		is.True(err != nil)
	}
}

func TestParseTimestamp(t *testing.T) {
	is := is.New(t)
	_, err := ParseTimestamp("0")
	is.NoErr(err)
	_, err = ParseTimestamp("1760000000000")
	is.NoErr(err)
	_, err = ParseTimestamp("abc")
	is.True(err != nil)
}

func TestParseNullMarker(t *testing.T) {
	is := is.New(t)
	b, err := ParseNullMarker("true")
	is.NoErr(err)
	is.True(b)
	b, err = ParseNullMarker("false")
	is.NoErr(err)
	is.True(!b)
	_, err = ParseNullMarker("")
	is.True(err != nil)
	_, err = ParseNullMarker("TRUE")
	is.True(err != nil)
}
