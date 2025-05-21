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
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-kafka/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// timeout is the default timeout used in tests when interacting with Kafka.
const timeout = 5 * time.Second

// T reports when failures occur.
// testing.T and testing.B implement this interface.
type T interface {
	// Fail indicates that the test has failed but
	// allowed execution to continue.
	Fail()
	// FailNow indicates that the test has failed and
	// aborts the test.
	FailNow()
	// Name returns the name of the running (sub-) test or benchmark.
	Name() string
	// Logf formats its arguments according to the format, analogous to Printf, and
	// records the text in the error log.
	Logf(string, ...any)
	// Cleanup registers a function to be called when the test (or subtest) and all its
	// subtests complete. Cleanup functions will be called in last added,
	// first called order.
	Cleanup(func())
}

func getRandomTopicName(t T) string {
	lastSlash := strings.LastIndex(t.Name(), "/")
	return t.Name()[lastSlash+1:] + uuid.NewString()
}

func ConfigMap() map[string]string {
	return map[string]string{
		"servers": "localhost:9092",
	}
}

func SourceConfigMap(t T, multipleTopics, retryGroupJoinErrors bool) map[string]string {
	m := ConfigMap()
	m["readFromBeginning"] = "true"
	m["retryGroupJoinErrors"] = fmt.Sprint(retryGroupJoinErrors)
	m["topics"] = getRandomTopicName(t)
	if multipleTopics {
		m["topics"] = m["topics"] + "," + getRandomTopicName(t)
	}
	t.Logf("using topics: %v", m["topics"])
	return m
}

func DestinationConfigMap(t T) map[string]string {
	m := ConfigMap()
	m["topic"] = getRandomTopicName(t)
	t.Logf("using topic: %v", m["topic"])

	m["batchBytes"] = "1000012"
	m["acks"] = "all"
	m["compression"] = "snappy"
	return m
}

func ParseConfigMap[C any](t T, cfg config.Config) C {
	is := is.New(t)
	is.Helper()

	var out C
	err := cfg.DecodeInto(&out)
	is.NoErr(err)

	return out
}

func Consume(t T, servers []string, topic string, limit int) []*kgo.Record {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	is := is.New(t)
	is.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(servers...),
		kgo.ConsumeTopics(topic),
		kgo.MetadataMinAge(time.Millisecond*100),
	)
	is.NoErr(err)
	defer cl.Close()

	var records []*kgo.Record
	for len(records) < limit {
		fetches := cl.PollFetches(ctx)
		is.NoErr(fetches.Err())

		records = append(records, fetches.Records()...)
	}
	return records[:limit]
}

func Produce(t T, servers []string, topic string, records []*kgo.Record, timeoutOpt ...time.Duration) {
	CreateTopics(t, servers, []string{topic})

	timeout := timeout // copy default timeout
	if len(timeoutOpt) > 0 {
		timeout = timeoutOpt[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	is := is.New(t)
	is.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(servers...),
		kgo.DefaultProduceTopic(topic),
		kgo.MetadataMinAge(time.Millisecond*100),
	)
	is.NoErr(err)
	defer cl.Close()

	results := cl.ProduceSync(ctx, records...)
	is.NoErr(results.FirstErr())
}

// ListCommittedOffsets returns the committed offsets for the given group ID and topic.
func ListCommittedOffsets(t T, servers []string, groupID, topic string) map[int32]kadm.OffsetResponse {
	is := is.New(t)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(servers...),
	)
	is.NoErr(err) // failed to create a Kafka client
	defer client.Close()

	admin := kadm.NewClient(client)
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	offsets, err := admin.FetchOffsetsForTopics(ctx, groupID, topic)
	is.NoErr(err) // failed to fetch consumer group offsets

	return offsets[topic]
}

func GenerateFranzRecords(from, to int, topicOpt ...string) []*kgo.Record {
	topic := ""
	if len(topicOpt) > 0 {
		topic = topicOpt[0]
	}
	recs := make([]*kgo.Record, 0, to-from+1)
	for i := from; i <= to; i++ {
		recs = append(recs, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("test-key-%d", i)),
			Value: []byte(fmt.Sprintf("test-payload-%d", i)),
		})
	}
	return recs
}

func GenerateSDKRecords(from, to int, topicOpt ...string) []opencdc.Record {
	recs := GenerateFranzRecords(from, to, topicOpt...)
	sdkRecs := make([]opencdc.Record, len(recs))
	for i, rec := range recs {
		metadata := opencdc.Metadata{}
		metadata.SetCollection(rec.Topic)
		metadata.SetCreatedAt(rec.Timestamp)

		sdkRecs[i] = sdk.Util.Source.NewRecordCreate(
			[]byte(uuid.NewString()),
			metadata,
			opencdc.RawData(rec.Key),
			opencdc.RawData(rec.Value),
		)
	}
	return sdkRecs
}

func CreateTopics(t T, servers []string, topics []string) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	is := is.New(t)
	is.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(servers...),
		kgo.MetadataMinAge(time.Millisecond*100),
	)
	is.NoErr(err)

	t.Cleanup(cl.Close)

	adminCl := kadm.NewClient(cl)
	resp, err := adminCl.CreateTopics(ctx, 1, 1, nil, topics...)
	var kafkaErr *kerr.Error
	if errors.As(resp.Error(), &kafkaErr) && kafkaErr.Code == kerr.TopicAlreadyExists.Code {
		// ignore topic if it already exists
		cl.Close()
		return
	}
	is.NoErr(err)
	is.NoErr(resp.Error())

	// we created the topic, so we should clean up after the test
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		responses, err := adminCl.DeleteTopics(ctx, topics...)
		is.NoErr(err)
		for _, resp := range responses {
			is.Equal(resp.ErrMessage, "")
			is.NoErr(resp.Err)
		}
	})
}

func Certificates(t T) (clientCert, clientKey, caCert string) {
	is := is.New(t)
	is.Helper()

	// get test dir
	_, filename, _, _ := runtime.Caller(0) //nolint:dogsled // we don't need other values
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

func ConfigWithIntegrationTestOptions(cfg common.Config) common.Config {
	return cfg.WithFranzClientOpts(
		// by default metadata is fetched every 5 seconds, for integration tests
		// we set this to a lower value so the tests finish faster
		kgo.MetadataMinAge(time.Millisecond * 100),
	)
}
