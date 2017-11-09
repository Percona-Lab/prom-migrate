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

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"

	"github.com/Percona-Lab/prom-migrate/remote"
)

type Writer struct {
	db *tsdb.DB
}

func NewWriter(dir string) (*Writer, error) {
	db, err := tsdb.Open(dir, log.With(log.NewLogfmtLogger(os.Stderr), "component", "tsdb"), nil, nil)
	if err != nil {
		return nil, err
	}

	// TODO options, logger, compression, etc.
	// db.EnableCompactions()

	return &Writer{db: db}, nil
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

		for _, s := range ts.Samples {
			_, err := a.Add(l, s.TimestampMs, s.Value)
			if err != nil {
				return err
			}
			// TODO use AddFast
		}
	}

	err := a.Commit()
	if err != nil {
		return err
	}
	committed = true
	return nil
}
