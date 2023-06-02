package main

import (
	dsql "database/sql"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"io"
	"reflect"
	"strings"
)

// MySqlRowsIter is an iterator over rows returned by MySQL
type MySqlRowsIter struct {
	rows        *dsql.Rows
	types       []sql.Type
	columnTypes []*dsql.ColumnType
	schema      sql.Schema
}

var _ sql.RowIter = (*MySqlRowsIter)(nil)

// NewMySqlRowsIter creates a new MySqlRowsIter
func NewMySqlRowsIter(ctx *sql.Context, rows *dsql.Rows) (*MySqlRowsIter, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	types, err := schemaToFields(ctx, cols)
	if err != nil {
		return nil, err
	}
	sch := make(sql.Schema, len(cols))
	for i, col := range cols {
		isNullable, ok := col.Nullable()
		if !ok {
			isNullable = true
		}
		sch[i] = &sql.Column{
			Name:     col.Name(),
			Type:     types[i],
			Nullable: isNullable,
		}
	}

	return &MySqlRowsIter{
		rows:        rows,
		types:       types,
		schema:      sch,
		columnTypes: cols,
	}, nil
}

// Next returns the next sql.Row until all rows are returned at which point (nil, io.EOF) is returned.
func (b MySqlRowsIter) Next(_ *sql.Context) (sql.Row, error) {
	rows := b.rows
	if rows.Next() {
		row, err := scanResultRow(rows, b.columnTypes)
		return row, err
	}
	return nil, io.EOF
}

// Close closes the iterator.
func (b MySqlRowsIter) Close(_ *sql.Context) error {
	return b.rows.Close()
}

// Schema returns the schema for the rows returned by this iterator.
func (b MySqlRowsIter) Schema() sql.Schema {
	return b.schema
}

func scanResultRow(results *dsql.Rows, cols []*dsql.ColumnType) (sql.Row, error) {
	scanRow := make(sql.Row, len(cols))

	//for i := range cols {
	//	scanRow[i] = reflect.New(cols[i].ScanType()).Interface()
	//}

	for i, columnType := range cols {
		scanRow[i] = reflect.New(columnType.ScanType()).Interface()
	}

	if err := results.Scan(scanRow...); err != nil {
		return nil, err
	}
	for i, val := range scanRow {
		col := cols[i]
		typeName := col.DatabaseTypeName()
		typeName = strings.ToLower(typeName)
		isUnsigned := strings.Contains(typeName, "unsigned")
		if isUnsigned {
			// if the type is unsigned, drop the unsigned part so that the type name matches the type name in the schema
			typeName = strings.Replace(typeName, "unsigned", "", 1)
			typeName = strings.TrimSpace(typeName)
		}
		if strings.HasSuffix(typeName, "int") {
			err := updateScanRowForInteger(val, scanRow, i, isUnsigned, typeName)
			if err != nil {
				return nil, err
			}
		} else {
			updateScanRowByVal(val, scanRow, i, isUnsigned)
		}
	}
	return scanRow, nil
}

func updateScanRowForInteger(val interface{}, scanRow sql.Row, i int, isUnsigned bool, typeName string) error {
	sqlVal := val.(*dsql.NullInt64)
	if !sqlVal.Valid {
		scanRow[i] = nil
	} else {
		val64 := sqlVal.Int64
		switch typeName {
		case "tinyint":
			if isUnsigned {
				scanRow[i] = uint8(val64)
			} else {
				scanRow[i] = int8(val64)
			}
		case "smallint":
			if isUnsigned {
				scanRow[i] = uint16(val64)
			} else {
				scanRow[i] = int16(val64)
			}
		case "mediumint":
			if isUnsigned {
				scanRow[i] = uint32(val64)
			} else {
				scanRow[i] = int32(val64)
			}
		case "int":
			if isUnsigned {
				scanRow[i] = uint32(val64)
			} else {
				scanRow[i] = int32(val64)
			}
		case "bigint":
			if isUnsigned {
				scanRow[i] = uint64(val64)
			} else {
				scanRow[i] = val64
			}
		default:
			return fmt.Errorf("unknown integer type %s", typeName)
		}
	}

	return nil
}

func updateScanRowByVal(val interface{}, scanRow sql.Row, i int, isUnsigned bool) {
	v := reflect.ValueOf(val).Elem().Interface()
	switch t := v.(type) {
	case dsql.RawBytes:
		if t == nil {
			scanRow[i] = nil
		} else {
			scanRow[i] = string(t)
		}
	case dsql.NullBool:
		if t.Valid {
			scanRow[i] = t.Bool
		} else {
			scanRow[i] = nil
		}
	case dsql.NullByte:
		if t.Valid {
			if isUnsigned {
				scanRow[i] = t.Byte
			} else {
				scanRow[i] = int8(t.Byte)
			}
		} else {
			scanRow[i] = nil
		}
	case dsql.NullFloat64:
		if t.Valid {
			if isUnsigned {
				scanRow[i] = uint64(t.Float64)
			} else {
				scanRow[i] = t.Float64
			}
		} else {
			scanRow[i] = nil
		}
	case dsql.NullInt16:
		if t.Valid {
			if isUnsigned {
				scanRow[i] = uint16(t.Int16)
			} else {
				scanRow[i] = t.Int16
			}
		} else {
			scanRow[i] = nil
		}
	case dsql.NullInt32:
		if t.Valid {
			if isUnsigned {
				scanRow[i] = uint32(t.Int32)
			} else {
				scanRow[i] = t.Int32
			}
		} else {
			scanRow[i] = nil
		}
	case dsql.NullInt64:
		if t.Valid {
			if isUnsigned {
				scanRow[i] = uint64(t.Int64)
			} else {
				scanRow[i] = t.Int64
			}
		} else {
			scanRow[i] = nil
		}
	case dsql.NullString:
		if t.Valid {
			scanRow[i] = t.String
		} else {
			scanRow[i] = nil
		}
	case dsql.NullTime:
		if t.Valid {
			scanRow[i] = t.Time
		} else {
			scanRow[i] = nil
		}
	default:
		scanRow[i] = t
	}
}

var typeDefaults = map[string]string{
	"char":      "char(255)",
	"binary":    "binary(255)",
	"varchar":   "varchar(65535)",
	"varbinary": "varbinary(65535)",
}

func schemaToFields(ctx *sql.Context, cols []*dsql.ColumnType) ([]sql.Type, error) {
	types := make([]sql.Type, len(cols))

	var err error
	for i, col := range cols {
		typeStr := strings.ToLower(col.DatabaseTypeName())
		if length, ok := col.Length(); ok {
			// append length specifier to type
			typeStr = fmt.Sprintf("%s(%d)", typeStr, length)
		} else if ts, ok := typeDefaults[typeStr]; ok {
			// if no length specifier if given,
			// default to the maximum width
			typeStr = ts
		}
		if strings.Contains(typeStr, " ") {
			switch typeStr {
			case "unsigned bigint":
				typeStr = "bigint unsigned"
			case "unsigned int":
				typeStr = "int unsigned"
			case "unsigned float":
				typeStr = "float unsigned"
			case "unsigned double":
				typeStr = "double unsigned"
			default:
				return nil, fmt.Errorf("unsupported column type %s", typeStr)
			}
		}
		types[i], err = parse.ParseColumnTypeString(ctx, typeStr)
		if err != nil {
			return nil, err
		}
	}

	return types, nil
}
