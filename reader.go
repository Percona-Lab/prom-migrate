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
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"

	"github.com/Percona-Lab/prom-migrate/remote"
)

// Reader reads data from Prometheus 1.8 via API.
type Reader struct {
	url         url.URL
	extraChecks bool
	client      *http.Client
}

func NewReader(url url.URL, extraChecks bool) *Reader {
	return &Reader{
		url:         url,
		extraChecks: extraChecks,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     time.Minute,
			},
		},
	}
}

func compareLabelPairs(labels []*remote.LabelPair) func(i, j int) bool {
	return func(i, j int) bool {
		if labels[i].Name != labels[j].Name {
			return labels[i].Name < labels[j].Name
		}
		return labels[i].Value < labels[j].Value
	}
}

func (r *Reader) Read(start, end time.Time) ([]*remote.TimeSeries, error) {
	request := &remote.ReadRequest{
		Queries: []*remote.Query{{
			StartTimestampMs: start.UnixNano() / 1000000,
			EndTimestampMs:   end.UnixNano() / 1000000,
			Matchers: []*remote.LabelMatcher{{
				Type:  remote.MatchType_NOT_EQUAL,
				Name:  "__name__",
				Value: "",
			}},
		}},
	}
	b, err := proto.Marshal(request)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	b = snappy.Encode(nil, b)

	req, err := http.NewRequest("POST", r.url.String(), bytes.NewReader(b))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ = httputil.DumpResponse(resp, true)
		return nil, fmt.Errorf("unexpected response:\n%s", b)
	}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	b, err = snappy.Decode(nil, b)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var response remote.ReadResponse
	err = proto.Unmarshal(b, &response)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(response.Results) != 1 {
		return nil, fmt.Errorf("got %d results", len(response.Results))
	}
	timeseries := response.Results[0].Timeseries

	for _, ts := range timeseries {
		sort.Slice(ts.Labels, compareLabelPairs(ts.Labels))
	}

	if r.extraChecks {
		for _, ts := range timeseries {
			if !sort.SliceIsSorted(ts.Labels, compareLabelPairs(ts.Labels)) {
				return nil, errors.New("labels are not sorted")
			}

			if !sort.SliceIsSorted(ts.Samples, func(i, j int) bool {
				return ts.Samples[i].TimestampMs < ts.Samples[j].TimestampMs
			}) {
				return nil, errors.New("samples are not sorted")
			}
		}
	}

	return timeseries, nil
}
