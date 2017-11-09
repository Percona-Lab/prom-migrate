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
)

func main() {
	log.SetFlags(0)
	readF := flag.String("read", "http://127.0.0.1:9090/api/v1/read", "Prometheus 1.8 read API endpoint")
	writeF := flag.String("write", "data", "Prometheus 2.0 data directory (must be empty or not exist)")
	checkF := flag.Bool("check", false, "Runs extra checks during migration")
	startF := flag.Duration("start", 14*24*time.Hour, "")
	flag.Parse()

	readURL, err := url.Parse(*readF)
	if err != nil {
		log.Fatal(err)
	}

	reader := &Reader{
		URL:   *readURL,
		Check: *checkF,
	}
	writer, err := NewWriter(*writeF)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		writer.Close()
		log.Print("writer closed")
	}()

	start := time.Now().Add(-*startF)
	step := time.Minute
	for {
		end := start.Add(step)
		data, err := reader.Read(start, end)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		err = writer.Write(data)
		if err != nil {
			log.Fatalf("%+v", err)
		}
		if start.After(time.Now()) {
			break
		}
		start = end
	}
}
