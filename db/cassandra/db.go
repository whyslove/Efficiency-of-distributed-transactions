// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cassandra

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hailocab/go-hostpool"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/gocql/gocql"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// cassandra properties
const (
	cassandraCluster     = "cassandra.cluster"
	cassandraKeyspace    = "cassandra.keyspace"
	cassandraConnections = "cassandra.connections"
	cassandraUsername    = "cassandra.username"
	cassandraPassword    = "cassandra.password"

	cassandraUsernameDefault    = "cassandra"
	cassandraPasswordDefault    = "cassandra"
	cassandraClusterDefault     = "127.0.0.1:9042"
	cassandraKeyspaceDefault    = "test"
	cassandraConnectionsDefault = 2 // refer to https://github.com/gocql/gocql/blob/master/cluster.go#L52

	// Additional configuration properties
	cassandraConsistencyLevel       = "cassandra.consistency"
	cassandraSerialConsistencyLevel = "cassandra.serial_consistency"
	cassandraTimeout                = "cassandra.timeout"
	cassandraConnectTimeout         = "cassandra.connect_timeout"
	cassandraUseLWT                 = "cassandra.use_lwt"
	cassandraRetryCount             = "cassandra.retry_count"

	// Default values for new properties
	cassandraConsistencyLevelDefault       = "QUORUM"
	cassandraSerialConsistencyLevelDefault = "QUORUM"
	cassandraTimeoutDefault                = 10 * time.Millisecond
	cassandraConnectTimeoutDefault         = 10 * time.Millisecond
	cassandraUseLWTDefault                 = false
	cassandraRetryCountDefault             = 3
)

type cassandraCreator struct {
}

type cassandraDB struct {
	p       *properties.Properties
	session *gocql.Session
	verbose bool

	bufPool  *util.BufPool
	keySpace string

	fieldNames []string
	useLWT     bool
}

type contextKey string

const stateKey = contextKey("cassandraDB")

type cassandraState struct {
}

func (c cassandraCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(cassandraDB)
	d.p = p

	hosts := strings.Split(p.GetString(cassandraCluster, cassandraClusterDefault), ",")
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = p.GetString(cassandraKeyspace, cassandraKeyspaceDefault)
	d.keySpace = cluster.Keyspace

	cluster.NumConns = p.GetInt(cassandraConnections, cassandraConnectionsDefault)

	// Configure timeouts
	timeout := time.Duration(p.GetInt64(cassandraTimeout, int64(cassandraTimeoutDefault)))
	connectTimeout := time.Duration(p.GetInt64(cassandraConnectTimeout, int64(cassandraConnectTimeoutDefault)))
	cluster.Timeout = timeout
	cluster.ConnectTimeout = connectTimeout
	cluster.ReconnectionPolicy = &gocql.ConstantReconnectionPolicy{MaxRetries: 100, Interval: 100 * time.Millisecond}
	cluster.ReconnectInterval = 10 * time.Millisecond
	cluster.DisableInitialHostLookup = false
	cluster.PoolConfig = gocql.PoolConfig{
		HostSelectionPolicy: gocql.HostPoolHostPolicy(hostpool.New(nil)),
	}

	// Configure consistency levels
	consistencyLevel := p.GetString(cassandraConsistencyLevel, cassandraConsistencyLevelDefault)
	if level, err := gocql.ParseConsistencyWrapper(consistencyLevel); err != nil {
		return nil, fmt.Errorf("invalid consistency level: %v", err)
	} else {
		cluster.Consistency = level
	}

	// cluster.Consistency = gocql.Quorum
	// cluster.SerialConsistency = gocql.Serial

	// Configure serial consistency for LWT
	serialConsistencyLevel := p.GetString(cassandraSerialConsistencyLevel, cassandraSerialConsistencyLevelDefault)
	if serialConsistencyLevel == "SERIAL" {
		cluster.SerialConsistency = gocql.Serial
	} else if serialConsistencyLevel == "LOCAL_SERIAL" {
		cluster.SerialConsistency = gocql.LocalSerial
	} else {
		return nil, fmt.Errorf("invalid serial consistency level: must be SERIAL or LOCAL_SERIAL")
	}

	// Configure retry policy
	retryCount := p.GetInt(cassandraRetryCount, cassandraRetryCountDefault)
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: retryCount}

	username := p.GetString(cassandraUsername, cassandraUsernameDefault)
	password := p.GetString(cassandraPassword, cassandraPasswordDefault)
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.session = session
	d.bufPool = util.NewBufPool()
	d.useLWT = p.GetBool(cassandraUseLWT, cassandraUseLWTDefault)

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *cassandraDB) createTable() error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) {
		if err := db.session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db.keySpace, tableName)).Exec(); err != nil {
			return err
		}
	}

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	db.fieldNames = make([]string, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		db.fieldNames[i] = fmt.Sprintf("field%d", i)
	}

	buf := new(bytes.Buffer)

	s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (YCSB_KEY VARCHAR PRIMARY KEY", db.keySpace, tableName)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d VARCHAR", i))
	}

	buf.WriteString(")")
	buf.WriteString(";")

	if db.verbose {
		fmt.Println(buf.String())
	}

	err := db.session.Query(buf.String()).Exec()
	return err
}

func (db *cassandraDB) Close() error {
	if db.session == nil {
		return nil
	}

	db.session.Close()
	return nil
}

func (db *cassandraDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *cassandraDB) CleanupThread(_ctx context.Context) {

}

func (db *cassandraDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		fields = db.fieldNames
	}

	query = fmt.Sprintf(`SELECT %s FROM %s.%s WHERE YCSB_KEY = ?`, strings.Join(fields, ","), db.keySpace, table)

	if db.verbose {
		fmt.Printf("%s\n", query)
	}

	m := make(map[string][]byte, len(fields))
	dest := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		v := new([]byte)
		dest[i] = v
	}

	err := db.session.Query(query, key).WithContext(ctx).Scan(dest...)
	if err == gocql.ErrNotFound {
		return nil, nil
	} else if err != nil {
		fmt.Println("QUERY ERROR: ", err.Error())
		return nil, err
	}

	for i, v := range dest {
		m[fields[i]] = *v.(*[]byte)
	}

	return m, nil
}

func (db *cassandraDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *cassandraDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	err := db.session.Query(query, args...).WithContext(ctx).Exec()
	return err
}

func (db *cassandraDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("UPDATE ")
	buf.WriteString(fmt.Sprintf("%s.%s", db.keySpace, table))
	buf.WriteString(" SET ")
	firstField := true
	pairs := util.NewFieldPairs(values)
	args := make([]interface{}, 0, len(values)+1)
	for _, p := range pairs {
		if firstField {
			firstField = false
		} else {
			buf.WriteString(", ")
		}

		buf.WriteString(p.Field)
		buf.WriteString(`= ?`)
		args = append(args, p.Value)
	}
	buf.WriteString(" WHERE YCSB_KEY = ?")

	if db.useLWT {
		buf.WriteString(" IF EXISTS")
	}

	args = append(args, key)

	err := db.execQuery(ctx, buf.String(), args...)
	if err != nil {
		fmt.Println("UPDATE ERROR: ", err.Error())
	}
	return err
}

func (db *cassandraDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, key)

	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("INSERT INTO ")
	buf.WriteString(fmt.Sprintf("%s.%s", db.keySpace, table))
	buf.WriteString(" (YCSB_KEY")

	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, p.Value)
		buf.WriteString(" ,")
		buf.WriteString(p.Field)
	}
	buf.WriteString(") VALUES (?")

	for i := 0; i < len(pairs); i++ {
		buf.WriteString(", ?")
	}
	buf.WriteString(")")

	if db.useLWT {
		buf.WriteString(" IF NOT EXISTS")
	}

	err2 := db.execQuery(ctx, buf.String(), args...)
	if err2 != nil {
		fmt.Println("INSERT ERROR: ", err2.Error())
	}
	return err2
}

func (db *cassandraDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s.%s WHERE YCSB_KEY = ?`, db.keySpace, table)

	return db.execQuery(ctx, query, key)
}

func init() {
	ycsb.RegisterDBCreator("cassandra", cassandraCreator{})
	ycsb.RegisterDBCreator("scylla", cassandraCreator{})
}
