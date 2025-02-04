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

package column

import (
	"errors"
	"reflect"

	"github.com/supresu/clickhouse-go/v2/lib/binary"
)

type Nothing struct{}

func (Nothing) Type() Type                     { return "Nothing" }
func (Nothing) ScanType() reflect.Type         { return reflect.TypeOf(nil) }
func (Nothing) Rows() int                      { return 0 }
func (Nothing) Row(int, bool) interface{}      { return nil }
func (Nothing) ScanRow(interface{}, int) error { return nil }
func (Nothing) Append(interface{}) ([]uint8, error) {
	return nil, &Error{
		ColumnType: "Nothing",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}
func (Nothing) AppendRow(interface{}) error {
	return &Error{
		ColumnType: "Nothing",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}
func (Nothing) Decode(decoder *binary.Decoder, rows int) error {
	scratch := make([]byte, rows)
	if err := decoder.Raw(scratch); err != nil {
		return err
	}
	return nil
}
func (Nothing) Encode(*binary.Encoder) error {
	return &Error{
		ColumnType: "Nothing",
		Err:        errors.New("data type values can't be stored in tables"),
	}
}

var _ Interface = (*Nothing)(nil)
