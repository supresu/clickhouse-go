// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"log"
	"time"

	"github.com/supresu/clickhouse-go/v2"
)

const ddl = `
CREATE TABLE example (
	  Col1 UInt64
	, Col2 String
	, Col3 Array(UInt8)
	, Col4 DateTime
) Engine = Memory
`

type row struct {
	Col1 uint64
	Col4 time.Time
	Col2 string
	Col3 []uint8
}

func example(conn clickhouse.Conn) error {
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO example")
	if err != nil {
		return err
	}
	for i := 0; i < 1_000; i++ {
		err := batch.AppendStruct(&row{
			Col1: uint64(i),
			Col2: "Golang SQL database driver",
			Col3: []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9},
			Col4: time.Now(),
		})
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func main() {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			//Debug:           true,
			DialTimeout:     time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS example`); err != nil {
		log.Fatal(err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		log.Fatal(err)
	}
	if err := example(conn); err != nil {
		log.Fatal(err)
	}
}
