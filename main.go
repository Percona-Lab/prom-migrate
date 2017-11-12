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
	"context"
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
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
	cpuprofileF := flag.String("cpuprofile", "", "Write cpu profile `file`")
	lastF := model.Duration(15 * 24 * time.Hour)
	flag.Var(&lastF, "last", "Migration starting point")
	retentionF := model.Duration(15 * 24 * time.Hour)
	flag.Var(&retentionF, "tsdb-retention", "TSDB: how long to retain samples in the storage")
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

	startedAt := time.Now()
	begin := time.Now().Add(-time.Duration(lastF)).Truncate(time.Minute)
	var end time.Time
	log.Printf("Migrating data since %s... ", begin)

	defer func() {
		if err := writer.Close(); err != nil {
			log.Fatal(err)
		}
		log.Printf("Done! Migrated %s of data (%s - %s) in %s.", end.Sub(begin), begin, end, time.Since(startedAt))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalsCh := make(chan os.Signal, 1)
	go func() {
		s := <-signalsCh
		log.Printf("Received %s, exiting.", s)
		signal.Stop(signalsCh)
		cancel()
	}()
	signal.Notify(signalsCh, syscall.SIGINT, syscall.SIGTERM)

	if *cpuprofileF != "" {
		f, err := os.Create(*cpuprofileF)
		if err != nil {
			log.Fatalf("Could not create CPU profile: %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Could not start CPU profile: %s", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}

	ch := make(chan []*remote.TimeSeries, 100)

	// start reader
	go func() {
		start := begin
		logProgress := func() {
			log.Printf("%s / %s (%.2f%%); writes queued %d/%d", start.Sub(begin), time.Since(begin).Truncate(time.Minute),
				float64(start.Sub(begin))/float64(time.Since(begin))*100, len(ch), cap(ch))
		}

		defer func() {
			logProgress()
			close(ch)
		}()

		const step = time.Minute
		t := time.Tick(10 * time.Second)
		for {
			select {
			case <-t:
				logProgress()
			case <-ctx.Done():
				return
			default:
			}

			end = start.Add(step)
			data, err := reader.Read(start, end)
			if err != nil {
				log.Fatalf("%+v", err)
			}
			ch <- data

			if start.After(time.Now()) {
				return
			}
			start = end
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
