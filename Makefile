FDB_CHECK := $(shell command -v fdbcli 2> /dev/null)
ROCKSDB_CHECK := $(shell echo "int main() { return 0; }" | gcc -lrocksdb -x c++ -o /dev/null - 2>/dev/null; echo $$?)
SQLITE_CHECK := $(shell echo "int main() { return 0; }" | gcc -lsqlite3 -x c++ -o /dev/null - 2>/dev/null; echo $$?)

TAGS =

ifdef FDB_CHECK
	TAGS += foundationdb
endif

ifneq ($(shell go env GOOS), $(shell go env GOHOSTOS))
	CROSS_COMPILE := 1
endif
ifneq ($(shell go env GOARCH), $(shell go env GOHOSTARCH))
	CROSS_COMPILE := 1
endif

ifndef CROSS_COMPILE

# ifeq ($(SQLITE_CHECK), 0)
# 	TAGS += libsqlite3
# endif

ifeq ($(ROCKSDB_CHECK), 0)
	TAGS += rocksdb
	CGO_CXXFLAGS := "${CGO_CXXFLAGS} -std=c++11"
	CGO_FLAGS += CGO_CXXFLAGS=$(CGO_CXXFLAGS)
endif

endif

default: build

build: export GO111MODULE=on
build:
ifeq ($(TAGS),)
	$(CGO_FLAGS) go build -o bin/go-ycsb cmd/go-ycsb/*
else
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/go-ycsb cmd/go-ycsb/*
endif

check:
	golint -set_exit_status db/... cmd/... pkg/...


# Makefile for running go-ycsb against Cassandra (ACCORD experiments)

YCSB_BIN=./bin/go-ycsb
# WORKLOAD=workloads/workloada

# Настройки Cassandra
KEYSPACE=test
# CLUSTER?=localhost:9042,localhost:9043,localhost:9044
# CONNECTIONS?=4
USERNAME?=
PASSWORD?=

#etcd
ETCD_ENDPOINTS?=localhost:2379,localhost:2380,localhost:2391
ETCD_DIAL_TIMEOUT?=5s
ETCD_USERNAME?=
ETCD_PASSWORD?=

# Настройки согласованности и таймаутов
CONSISTENCY?=ONE
SERIAL_CONSISTENCY?=LOCAL_SERIAL
TIMEOUT?=100000000000
CONNECT_TIMEOUT?=1000000000

USE_LWT?=true
# RETRY_COUNT?=3

# Общие параметры нагрузки
THREADS?=8
COUNT?=10000
VERBOSE?=false
MY_LOG_LEVEL?=info

.PHONY: load run


load:
	$(YCSB_BIN) load cassandra \
		-P $(WORKLOAD) \
		-p cassandra.cluster=$(CLUSTER)\
		-p cassandra.keyspace=$(KEYSPACE) \
		-p cassandra.connections=$(CONNECTIONS) \
		-p cassandra.username=$(USERNAME) \
		-p cassandra.password=$(PASSWORD) \
		-p cassandra.consistency=$(CONSISTENCY) \
		-p cassandra.serial_consistency=$(SERIAL_CONSISTENCY) \
		-p cassandra.timeout=$(TIMEOUT) \
		-p cassandra.connect_timeout=$(CONNECT_TIMEOUT) \
		-p cassandra.use_lwt=$(USE_LWT) \
		-p cassandra.retry_count=$(RETRY_COUNT) \
		-p threadcount=$(THREADS) \
		-p operationcount=$(COUNT) \
		-p verbose=$(VERBOSE)

run:
	$(YCSB_BIN) run cassandra \
		-P $(WORKLOAD) \
		-p cassandra.cluster=$(CLUSTER) \
		-p cassandra.keyspace=$(KEYSPACE) \
		-p cassandra.connections=$(CONNECTIONS) \
		-p cassandra.username=$(USERNAME) \
		-p cassandra.password=$(PASSWORD) \
		-p cassandra.consistency=$(CONSISTENCY) \
		-p cassandra.serial_consistency=$(SERIAL_CONSISTENCY) \
		-p cassandra.timeout=$(TIMEOUT) \
		-p cassandra.connect_timeout=$(CONNECT_TIMEOUT) \
		-p cassandra.use_lwt=$(USE_LWT) \
		-p cassandra.retry_count=$(RETRY_COUNT) \
		-p threadcount=$(THREADS) \
		-p operationcount=$(COUNT) \
		-p verbose=$(VERBOSE)

load-etcd:
	$(YCSB_BIN) load etcd \
		-P $(WORKLOAD) \
		-p etcd.endpoints=$(ETCD_ENDPOINTS) \
		-p etcd.dial_timeout=$(ETCD_DIAL_TIMEOUT) \
		-p etcd.username=$(ETCD_USERNAME) \
		-p etcd.password=$(ETCD_PASSWORD) \
		-p threadcount=$(THREADS) \
		-p operationcount=$(COUNT) \
		-p verbose=$(VERBOSE)

run-etcd:
	$(YCSB_BIN) run etcd \
		-P $(WORKLOAD) \
		-p etcd.endpoints=$(ETCD_ENDPOINTS) \
		-p etcd.dial_timeout=$(ETCD_DIAL_TIMEOUT) \
		-p etcd.username=$(ETCD_USERNAME) \
		-p etcd.password=$(ETCD_PASSWORD) \
		-p threadcount=$(THREADS) \
		-p operationcount=$(COUNT) \
		-p verbose=$(VERBOSE) \
		-p problems=true

COCKROACH_DB?=ycsb
COCKROACH_TABLE?=ycsb_table
COCKROACH_CLUSTER?=localhost:26257,localhost:26257,localhost:26257,localhost:26257,localhost:26257
COCKROACH_URL?=postgresql://root@$(COCKROACH_CLUSTER)/$(COCKROACH_DB)?sslmode=disable

load-cockroach:
	$(YCSB_BIN) load cockroach \
		-P $(WORKLOAD) \
		-p cockroach.url=$(COCKROACH_URL) \
		-p table=$(COCKROACH_TABLE) \
		-p threadcount=$(THREADS) \
		-p operationcount=$(COUNT) \
		-p verbose=$(VERBOSE) \
		-p log_level=$(MY_LOG_LEVEL)

run-cockroach:
	$(YCSB_BIN) run cockroach \
		-P $(WORKLOAD) \
		-p cockroach.url=$(COCKROACH_URL) \
		-p cockroach.table=$(COCKROACH_TABLE) \
		-p table=$(COCKROACH_TABLE) \
		-p threadcount=$(THREADS) \
		-p operationcount=$(COUNT) \
		-p verbose=$(VERBOSE) \
		-p log_level=$(MY_LOG_LEVEL) \
		-p problems=true

.PHONY: load run bench bench-paxos bench-accord

# load-cockroach:
# 	$(YCSB_BIN) load pg \
# 		-P $(WORKLOAD) \
# 		-p pg.port=26257 \
# 		-p pg.host=localhost \
# 		-p pg.db=ycsb \
# 		-p threadcount=$(THREADS) \
# 		-p operationcount=$(COUNT) \
# 		-p verbose=$(VERBOSE) \
# 		-p log_level=$(MY_LOG_LEVEL)

# run-cockroach:
# 	$(YCSB_BIN) run pg \
# 		-P $(WORKLOAD) \
# 		-p pg.port=26257 \
# 		-p pg.host=cockroach1 \
# 		-p table=$(COCKROACH_TABLE) \
# 		-p threadcount=$(THREADS) \
# 		-p operationcount=$(COUNT) \
# 		-p verbose=$(VERBOSE) \
# 		-p log_level=$(MY_LOG_LEVEL) \
# 		-p problems=true

# Главный таргет: последовательная нагрузка
bench:
	$(MAKE) load
	$(MAKE) run

bench-etcd:
	$(MAKE) load-etcd MY_LOG_LEVEL=$(MY_LOG_LEVEL) CONTAINERS="etcd0,etcd1,etcd2,etcd3,etcd4"
	$(MAKE) run-etcd MY_LOG_LEVEL=$(MY_LOG_LEVEL) CONTAINERS="etcd0,etcd1,etcd2,etcd3,etcd4"

# Явно таргет под классический PAXOS
bench-accord:
	$(MAKE) bench CLUSTER=127.0.0.1:9142,127.0.0.1:9143,127.0.0.1:9144,127.0.0.1:9145,127.0.0.1:9146 MY_LOG_LEVEL=$(MY_LOG_LEVEL) CONTAINERS=cassandra-accord-node1,cassandra-accord-node2,cassandra-accord-node3,cassandra-accord-node4,cassandra-accord-node5

# Явно таргет под ACCORD
bench-paxos:
	$(MAKE) bench CLUSTER=localhost:9042,localhost:9043,localhost:9044,localhost:9045,localhost:9046 MY_LOG_LEVEL=$(MY_LOG_LEVEL) CONTAINERS=cassandra-1,cassandra-2,cassandra-3,cassandra-4,cassandra-5

bench-cockroach:
	$(MAKE) load-cockroach CLUSTER=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 MY_LOG_LEVEL=$(MY_LOG_LEVEL) CONTAINERS=cockroach1,cockroach2,cockroach3,cockroach4,cockroach5
	$(MAKE) run-cockroach CLUSTER=localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261 MY_LOG_LEVEL=$(MY_LOG_LEVEL) CONTAINERS=cockroach1,cockroach2,cockroach3,cockroach4,cockroach5


# CREATE KEYSPACE test WITH replication = {   'class': 'SimpleStrategy',   'replication_factor': 1 };
