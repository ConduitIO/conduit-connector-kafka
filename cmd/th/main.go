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

package main

import (
	"context"
	"log"
	"sync"
	"time"

	kafka "github.com/conduitio/conduit-connector-kafka"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	recordCount     = 1_000_000
	checkpointCount = 10
)

func main() {
	ctx := context.Background()

	s := kafka.Connector.NewSource()
	cfg := map[string]string{
		"servers":           "localhost:9092",
		"topic":             "source-test", // ensure there is enough records in the topic
		"readFromBeginning": "true",
	}

	dur := measure(func() {
		measureSource(ctx, s, cfg)
	})
	log.Println("---")
	log.Printf("Total: %v", dur)
}

func measureSource(ctx context.Context, s sdk.Source, cfg map[string]string) {
	ctx, cancel := context.WithCancel(ctx)

	dur := measure(func() {
		err := s.Configure(ctx, cfg)
		if err != nil {
			log.Fatal(err)
		}
	})
	log.Printf("Configure: %v", dur)

	dur = measure(func() {
		err := s.Open(ctx, nil)
		if err != nil {
			log.Fatal(err)
		}
	})
	log.Printf("Open: %v", dur)

	acks := make(chan sdk.Record, recordCount) // huge buffer so we don't delay reads
	var wg sync.WaitGroup
	wg.Add(1)
	go acker(s, acks, &wg)

	recCountPerCheckpoint := recordCount / checkpointCount

	// measure first record read manually, it might be slower
	dur = measure(func() {
		rec, err := s.Read(ctx)
		if err != nil {
			log.Fatal("Read: ", err)
		}
		acks <- rec
	})
	log.Printf("[read] First read: %v", dur)

	var cumDurRead time.Duration
	for cp := 0; cp < checkpointCount; cp++ {
		dur = measure(func() {
			for i := 0; i < recCountPerCheckpoint; i++ {
				rec, err := s.Read(ctx)
				if err != nil {
					log.Fatal("Read: ", err)
				}
				acks <- rec
			}
		})
		log.Printf("[read] Throughput (%v%%): %.0f msg/s", (cp+1)*100/checkpointCount, float64(recCountPerCheckpoint)/dur.Seconds())
		cumDurRead += dur
	}
	log.Printf("[read] Average: %.0f msg/s", float64(recordCount)/cumDurRead.Seconds())

	// stop
	dur = measure(func() {
		cancel()
		wg.Wait()
	})
	log.Printf("Stop: %v", dur)

	dur = measure(func() {
		err := s.Teardown(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	})
	log.Printf("Teardown: %v", dur)
}

func acker(source sdk.Source, c <-chan sdk.Record, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	recCountPerCheckpoint := recordCount / checkpointCount

	rec := <-c // read first ack manually
	dur := measure(func() {
		err := source.Ack(ctx, rec.Position)
		if err != nil {
			log.Fatal("Ack: ", err)
		}
	})
	log.Printf("[acks] First ack: %v", dur)

	var cumDurRead time.Duration
	for cp := 0; cp < checkpointCount; cp++ {
		dur = measure(func() {
			var count int
			for rec := range c {
				count++
				err := source.Ack(context.Background(), rec.Position)
				if err != nil {
					log.Fatal("Ack: ", err)
				}
				if count == recCountPerCheckpoint {
					return
				}
			}
		})
		log.Printf("[acks] Throughput (%v%%): %.0f msg/s", (cp+1)*100/checkpointCount, float64(recCountPerCheckpoint)/dur.Seconds())
		cumDurRead += dur
	}
	log.Printf("[acks] Average: %.0f msg/s", float64(recordCount)/cumDurRead.Seconds())
}

func measure(f func()) time.Duration {
	start := time.Now()
	f()
	return time.Since(start)
}
