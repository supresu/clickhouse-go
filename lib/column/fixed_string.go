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
	"encoding"
	"fmt"
	"reflect"

	"github.com/supresu/clickhouse-go/v2/lib/binary"
)

type FixedString struct {
	data []byte
	size int
}

func (col *FixedString) parse(t Type) (*FixedString, error) {
	if _, err := fmt.Sscanf(string(t), "FixedString(%d)", &col.size); err != nil {
		return nil, err
	}
	return col, nil
}

func (col *FixedString) Type() Type {
	return Type(fmt.Sprintf("FixedString(%d)", col.size))
}

func (col *FixedString) ScanType() reflect.Type {
	return scanTypeString
}

func (col *FixedString) Rows() int {
	if col.size == 0 {
		return 0
	}
	return len(col.data) / col.size
}

func (col *FixedString) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *FixedString) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.row(row)
	case **string:
		*d = new(string)
		**d = col.row(row)
	case encoding.BinaryUnmarshaler:
		return d.UnmarshalBinary(col.rowBytes(row))
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "FixedString",
		}
	}
	return nil
}

func (col *FixedString) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		for _, v := range v {
			if v == "" {
				col.data = append(col.data, make([]byte, col.size)...)
			} else {
				col.data = append(col.data, binary.Str2Bytes(v)...)
			}
		}
		nulls = make([]uint8, len(v))
	case []*string:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			if v == nil {
				nulls[i] = 1
			}
			switch {
			case v == nil:
				col.data = append(col.data, make([]byte, col.size)...)
			default:
				if *v == "" {
					col.data = append(col.data, make([]byte, col.size)...)
				} else {
					col.data = append(col.data, binary.Str2Bytes(*v)...)
				}
			}
		}
	case encoding.BinaryMarshaler:
		data, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		col.data, nulls = append(col.data, data...), make([]uint8, len(data)/col.size)
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "FixedString",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *FixedString) AppendRow(v interface{}) (err error) {
	data := make([]byte, col.size)
	switch v := v.(type) {
	case string:
		if v != "" {
			data = binary.Str2Bytes(v)
		}
	case *string:
		if v != nil {
			if *v != "" {
				data = binary.Str2Bytes(*v)
			}
		}
	case nil:
	case encoding.BinaryMarshaler:
		if data, err = v.MarshalBinary(); err != nil {
			return err
		}
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "FixedString",
			From: fmt.Sprintf("%T", v),
		}
	}
	col.data = append(col.data, data...)
	return nil
}

func (col *FixedString) Decode(decoder *binary.Decoder, rows int) error {
	col.data = make([]byte, col.size*rows)
	return decoder.Raw(col.data)
}

func (col *FixedString) Encode(encoder *binary.Encoder) error {
	if len(col.data)%col.size != 0 {
		return &Error{
			ColumnType: string(col.Type()),
			Err:        fmt.Errorf("invalid column size. must be a multiple of %d bytes got %d bytes", col.size, len(col.data)),
		}
	}
	return encoder.Raw(col.data)
}

func (col *FixedString) row(i int) string {
	return string(col.data[i*col.size : (i+1)*col.size])
}

func (col *FixedString) rowBytes(i int) []byte {
	return col.data[i*col.size : (i+1)*col.size]
}

var _ Interface = (*FixedString)(nil)
