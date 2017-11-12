// prom-migrate
// Copyright (C) 2017 Percona LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"log"
	"net/url"
	"time"

	"github.com/prometheus/common/model"

	"github.com/Percona-Lab/prom-migrate/remote"
)

func main() {
	log.SetFlags(log.Lmicroseconds)
	log.SetPrefix("main ")
	readF := flag.String("read", "http://127.0.0.1:9090/api/v1/read", "Prometheus 1.8 read API endpoint")
	writeF := flag.String("write", "data", "Prometheus 2.0 new data directory")
	checkF := flag.Bool("check", false, "Runs extra checks during migration")
	lastF := model.Duration(15 * 24 * time.Hour)
	flag.Var(&lastF, "last", "Migration starting point")
	retentionF := model.Duration(15 * 24 * time.Hour)
	flag.Var(&retentionF, "tsdb-retention", "TSDB: how long to retain samples in the storage")
	step := time.Minute
	flag.Parse()

	readURL, err := url.Parse(*readF)
	if err != nil {
		log.Fatal(err)
	}

	reader := NewReader(*readURL, *checkF)
	writer, err := NewWriter(*writeF, time.Duration(retentionF))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		writer.Close()
		log.Print("Done!")
	}()

	begin := time.Now().Add(-time.Duration(lastF)).Truncate(time.Minute)
	start := begin
	t := time.Tick(10 * time.Second)
	log.Printf("Migrating data since %s... ", begin)

	ch := make(chan []*remote.TimeSeries, 100)

	// start reader
	go func() {
		defer close(ch)

		for {
			end := start.Add(step)
			data, err := reader.Read(start, end)
			if err != nil {
				log.Fatalf("%+v", err)
			}
			ch <- data

			if start.After(time.Now()) {
				break
			}
			start = end

			select {
			case <-t:
				log.Printf("%s / %s (%.2f%%); writes queued %d/%d", start.Sub(begin), time.Since(begin).Truncate(time.Minute),
					float64(start.Sub(begin))/float64(time.Since(begin))*100, len(ch), cap(ch))
			default:
			}
		}
	}()

	// start writer
	for data := range ch {
		if len(data) == 0 {
			continue
		}

		if err := writer.Write(data); err != nil {
			log.Fatalf("%+v", err)
		}
	}
}
