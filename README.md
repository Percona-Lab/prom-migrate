# prom-migrate

[![Build Status](https://travis-ci.org/Percona-Lab/prom-migrate.svg?branch=master)](https://travis-ci.org/Percona-Lab/prom-migrate)

prom-migrate reads all data from Prometheus 1.8 via API and creates a new Prometheus 2.0 storage directory.

Status: alpha (it _may_ work for you).

## Installation

To install or upgrade prom-migrate run:
```sh
go get -u github.com/Percona-Lab/prom-migrate
```

## Running

```sh
./prom-migrate -h
```
