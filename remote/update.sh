#!/bin/bash

set -ex

rm -f remote.*
rm -fr prom
git clone https://github.com/prometheus/prometheus.git prom

cd prom && git checkout v1.8.2
cp -v storage/remote/remote.* ..
cd ..

rm -fr prom
