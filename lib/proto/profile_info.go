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

package proto

import (
	"fmt"

	"github.com/supresu/clickhouse-go/v2/lib/binary"
)

type ProfileInfo struct {
	Rows                      uint64
	Bytes                     uint64
	Blocks                    uint64
	AppliedLimit              bool
	RowsBeforeLimit           uint64
	CalculatedRowsBeforeLimit bool
}

func (p *ProfileInfo) Decode(decoder *binary.Decoder, revision uint64) (err error) {
	if p.Rows, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.Blocks, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.Bytes, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.AppliedLimit, err = decoder.Bool(); err != nil {
		return err
	}
	if p.RowsBeforeLimit, err = decoder.Uvarint(); err != nil {
		return err
	}
	if p.CalculatedRowsBeforeLimit, err = decoder.Bool(); err != nil {
		return err
	}
	return nil
}

func (p *ProfileInfo) String() string {
	return fmt.Sprintf("rows=%d, bytes=%d, blocks=%d, rows before limit=%d, applied limit=%t, calculated rows before limit=%t",
		p.Rows,
		p.Bytes,
		p.Blocks,
		p.RowsBeforeLimit,
		p.AppliedLimit,
		p.CalculatedRowsBeforeLimit,
	)
}
