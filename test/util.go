// Copyright © 2023 Meroxa, Inc.
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

package test

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/google/uuid"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-kafka/config"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func ConfigMap(t *testing.T) map[string]string {
	return map[string]string{
		"servers": "localhost:9092",
		"topic":   t.Name() + uuid.NewString(),
	}
}

func SourceConfigMap(t *testing.T) map[string]string {
	m := ConfigMap(t)
	m["readFromBeginning"] = "true"
	return m
}

func DestinationConfigMap(t *testing.T) map[string]string {
	m := ConfigMap(t)
	// no special fields for now
	return m
}

func ParseConfigMap[T any](t *testing.T, cfg map[string]string) T {
	is := is.New(t)
	is.Helper()

	var out T
	err := sdk.Util.ParseConfig(cfg, &out)
	is.NoErr(err)

	return out
}

func Consume(t *testing.T, cfg config.Config, limit int) []*kgo.Record {
	is := is.New(t)
	is.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Servers...),
		kgo.ConsumeTopics(cfg.Topic),
	)
	is.NoErr(err)
	defer cl.Close()

	var records []*kgo.Record
	for len(records) < limit {
		fetches := cl.PollFetches(context.Background())
		is.NoErr(fetches.Err())

		records = append(records, fetches.Records()...)
	}
	return records[:limit]
}

func Produce(t *testing.T, cfg config.Config, records []*kgo.Record) {
	is := is.New(t)
	is.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Servers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.AllowAutoTopicCreation(),
	)
	is.NoErr(err)
	defer cl.Close()

	results := cl.ProduceSync(context.Background(), records...)
	is.NoErr(results.FirstErr())
}

func GenerateRecords(from, to int) []*kgo.Record {
	recs := make([]*kgo.Record, 0, to-from+1)
	for i := from; i <= to; i++ {
		recs = append(recs, &kgo.Record{
			Key:   []byte(fmt.Sprintf("test-key-%d", i)),
			Value: []byte(fmt.Sprintf("test-payload-%d", i)),
		})
	}
	return recs
}

func CreateTopic(t *testing.T, cfg config.Config) {
	is := is.New(t)
	is.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Servers...),
	)
	is.NoErr(err)
	defer cl.Close()

	resp, err := kadm.NewClient(cl).CreateTopic(
		context.Background(), 1, 1, nil, cfg.Topic)
	is.NoErr(err)
	is.NoErr(resp.Err)
}

func Certificates(t *testing.T) (clientCert, clientKey, caCert string) {
	is := is.New(t)
	is.Helper()

	// get test dir
	_, filename, _, _ := runtime.Caller(0)
	testDir := path.Dir(filename)

	readFile := func(file string) string {
		bytes, err := os.ReadFile(path.Join(testDir, file))
		is.NoErr(err)
		return string(bytes)
	}

	clientCert = readFile("client.cer.pem")
	clientKey = readFile("client.key.pem")
	caCert = readFile("server.cer.pem")
	return
}
