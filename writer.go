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
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"

	"github.com/Percona-Lab/prom-migrate/remote"
)

// Writer writes data to new Prometheus 2.0 tsdb database.
type Writer struct {
	db *tsdb.DB
	l  log.Logger
}

func NewWriter(dir string, retention time.Duration) (*Writer, error) {
	l := log.With(log.NewLogfmtLogger(os.Stderr), "component", "tsdb")
	db, err := tsdb.Open(dir, l, nil, &tsdb.Options{
		WALFlushInterval:  10 * time.Second, // the same as Prometheus 2.0
		RetentionDuration: uint64(retention / time.Millisecond),
		BlockRanges:       tsdb.ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
	})
	if err != nil {
		return nil, err
	}
	return &Writer{db: db, l: l}, nil
}

func (w *Writer) Close() error {
	return w.db.Close()
}

func (w *Writer) Write(data []*remote.TimeSeries) error {
	a := w.db.Appender()
	var committed bool
	defer func() {
		if !committed {
			a.Rollback()
		}
	}()

	for _, ts := range data {
		l := make(labels.Labels, len(ts.Labels))
		for i, p := range ts.Labels {
			l[i] = labels.Label{Name: p.Name, Value: p.Value}
		}

		var ref uint64
		var err error
		for _, s := range ts.Samples {
			// use ref if available
			if ref != 0 {
				if err = a.AddFast(ref, s.TimestampMs, s.Value); err == nil {
					continue
				}
				w.l.Log("msg", "AddFast returned error", "error", err)
			}

			// slow path if ref is not available or AddFast returned error
			if ref, err = a.Add(l, s.TimestampMs, s.Value); err != nil {
				return err
			}
		}
	}

	if err := a.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
