package main

import (
	"context"
	mysql "database/sql"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

type testcase struct {
	query               string
	expectedResult      []interface{}
	expectedSchemaTypes []sql.Type
	disabledMessage     string
}

var bitType, _ = types.CreateBitType(6)
var decimalType, _ = types.CreateDecimalType(10, 5)

var tests = []testcase{
	{
		query: "show create table data",
	},
	{
		query:               "select id from data",
		expectedResult:      []interface{}{100},
		expectedSchemaTypes: []sql.Type{types.Int64},
	},
	{
		query:               "select bit1 from data",
		expectedResult:      []interface{}{"!"},
		expectedSchemaTypes: []sql.Type{bitType},
		disabledMessage:     "not supported, https://github.com/go-sql-driver/mysql/issues/1132",
	},
	{
		query:               "select integer2 from data",
		expectedResult:      []interface{}{2},
		expectedSchemaTypes: []sql.Type{types.Int32},
	},
	{
		query:               "select smallint3 from data",
		expectedResult:      []interface{}{3},
		expectedSchemaTypes: []sql.Type{types.Int16},
	},
	{
		query:               "select float_4 from data",
		expectedResult:      []interface{}{4},
		expectedSchemaTypes: []sql.Type{types.Float32},
	},
	{
		query:               "select double5 from data",
		expectedResult:      []interface{}{"5"},
		expectedSchemaTypes: []sql.Type{types.Float64},
	},
	{
		query:               "select bigInt6 from data",
		expectedResult:      []interface{}{"6"},
		expectedSchemaTypes: []sql.Type{types.Int64},
	},
	{
		query:               "select bool7 from data",
		expectedResult:      []interface{}{1},
		expectedSchemaTypes: []sql.Type{types.Int8},
	},
	{
		query:               "select tinyint8 from data",
		expectedResult:      []interface{}{8},
		expectedSchemaTypes: []sql.Type{types.Int8},
	},
	{
		query:               "select smallint9 from data",
		expectedResult:      []interface{}{"9"},
		expectedSchemaTypes: []sql.Type{types.Int16},
	},
	{
		query:               "select mediumint10 from data",
		expectedResult:      []interface{}{"10"},
		expectedSchemaTypes: []sql.Type{types.Int24},
	},
	{
		query:               "select decimal11 from data",
		expectedResult:      []interface{}{"11.01230"},
		expectedSchemaTypes: []sql.Type{decimalType},
	},
	{
		query:               "select date12, time13, datetime14 from data",
		expectedResult:      []interface{}{"2023-05-31", "18:45:39", "2023-05-31 18:45:39"},
		expectedSchemaTypes: []sql.Type{types.Date, types.Time, types.Datetime},
	},
	{
		query:               "select year15 from data",
		expectedResult:      []interface{}{"2015"},
		expectedSchemaTypes: []sql.Type{types.Year},
	},
	{
		query:               "select char16 from data",
		expectedResult:      []interface{}{"char16"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select varchar17 from data",
		expectedResult:      []interface{}{"varchar17"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select binary18 from data",
		expectedResult:      []interface{}{"binary--18"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select varbinary19 from data",
		expectedResult:      []interface{}{"vbinary19"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select json20 from data",
		expectedResult:      []interface{}{`[1, 2, 3, "four", {"a": 1, "b": "2", "c": 3, "d": {"e": 1, "f": 2, "g": 3}}]`},
		expectedSchemaTypes: []sql.Type{types.JSON},
	},
}
var setupScripts = []string{
	`create table data (
		id BIGINT primary key,
		bit1 BIT(6),
		integer2 INTEGER,
		smallint3 SMALLINT,
		float_4 FLOAT,
		double5 DOUBLE,
		bigInt6 BIGINT,
		bool7 BOOLEAN,
		tinyint8 TINYINT,
		smallint9 SMALLINT,
		mediumint10 MEDIUMINT,
		decimal11 DECIMAL(10, 5),
		date12 DATE,
		time13 TIME,
		datetime14 DATETIME,
		year15 YEAR,
		char16 CHAR(10),
		varchar17 VARCHAR(10),
		binary18 BINARY(10),
		varbinary19 VARBINARY(10),
		json20 JSON
	 );`,

	`insert into data values (
		100, 33, 2, 3, 4, 5, 6,
		true, 8, 9, 10, 11.0123, "2023-05-31", "18:45:39", "2023-05-31 18:45:39",
		2015, "char16", "varchar17", "binary--18", "vbinary19",
		"[ 1 , 2 , 3 , ""four"" , { ""a"": 1, ""b"": ""2"", ""c"": 3, ""d"": { ""e"": 1, ""f"": 2, ""g"": 3 } }]"
		);`,
}

func TestQueryistCases(t *testing.T) {
	for _, test := range tests {
		RunSingleTest(t, test)
	}
}

func RunSingleTest(t *testing.T, test testcase) {
	if len(test.disabledMessage) > 0 {
		//t.Skip(test.disabledMessage)
		//t.Errorf(test.disabledMessage)
		t.Fail()
	}

	t.Run(test.query+"-SqlEngineQueryist", func(t *testing.T) {
		if test.disabledMessage != "" {
			t.Skip(test.disabledMessage)
		}
		// setup server engine
		ctx := context.Background()
		sqlCtx := sql.NewContext(ctx)
		dEnv := dtestutils.CreateTestEnv()
		//sqlEngine, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
		//require.NoError(t, err)
		//sqlCtx, err := sqlEngine.NewLocalContext(ctx)
		//require.NoError(t, err)
		//sqlCtx.SetCurrentDatabase(dbName)
		//queryist := commands.NewSqlEngineQueryist(sqlEngine)

		// new experimental code
		sc, serverConfig := startServerFromDoltEnv(t, dEnv)
		conn, _ := newConnection(t, serverConfig)
		defer func() {
			conn.Close()
			dEnv.DoltDB.Close()
			sc.StopServer()
			sc.WaitForClose()
		}()
		//queryist := NewDbrQueryist(conn)
		queryist, err := createQueryist(conn)
		require.NoError(t, err)

		// initialize server
		initServer(t, sqlCtx, queryist)

		// run test
		runTestcase(t, sqlCtx, queryist, test)
	})
	t.Run(test.query+"-ConnectionQueryist", func(t *testing.T) {
		if test.disabledMessage != "" {
			t.Skip(test.disabledMessage)
		}
		//// setup server
		//dEnv, sc, serverConfig := startServer(t, true, "", "")
		//err := sc.WaitForStart()
		//require.NoError(t, err)
		//defer dEnv.DoltDB.Close()
		//conn, _ := newConnection(t, serverConfig)
		//queryist := sqlserver.NewConnectionQueryist(conn)
		//ctx := context.Background()
		//sqlCtx := sql.NewContext(ctx)

		// new experimental code
		ctx := context.Background()
		sqlCtx := sql.NewContext(ctx)
		dEnv, sc, serverConfig := startServer(t, true, "", "")
		conn, _ := newConnection(t, serverConfig)
		defer func() {
			conn.Close()
			sc.StopServer()
			sc.WaitForClose()
			dEnv.DoltDB.Close()
		}()
		//queryist := NewDbrQueryist(conn)
		queryist, err := createQueryist(conn)
		require.NoError(t, err)

		// initialize server
		initServer(t, sqlCtx, queryist)

		// run test
		runTestcase(t, sqlCtx, queryist, test)

		// close server
		//require.NoError(t, conn.Close())
		//sc.StopServer()
		//err := sc.WaitForClose()
		//require.NoError(t, err)
	})
}

func runTestcase(t *testing.T, sqlCtx *sql.Context, queryist cli.Queryist, test testcase) {
	// run test
	schema, rowIter, err := queryist.Query(sqlCtx, test.query)
	require.NoError(t, err)

	// get a row of data
	row, err := rowIter.Next(sqlCtx)
	require.NoError(t, err)

	if len(test.expectedResult) > 0 {
		// test result row
		assert.Equal(t, len(test.expectedResult), len(row))

		for i, val := range row {
			expected := test.expectedResult[i]
			assert.Equal(t, expected, val)
		}
	} else {
		// log result row
		sb := strings.Builder{}
		for i, val := range row {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%v", val))
		}
		t.Logf("result: %s", sb.String())
	}

	// test result schema
	if len(test.expectedSchemaTypes) > 0 {
		for i, col := range schema {
			expected := test.expectedSchemaTypes[i]
			actual := col.Type
			assert.Equal(t, expected, actual,
				"schema type mismatch for column %s: %s != %s", col.Name, expected.String(), actual.String())
		}
	}
}

func initServer(t *testing.T, sqlCtx *sql.Context, queryist cli.Queryist) {
	for _, setupScript := range setupScripts {
		_, rowIter, err := queryist.Query(sqlCtx, setupScript)
		require.NoError(t, err)

		for {
			row, err := rowIter.Next(sqlCtx)
			if err == io.EOF {
				break
			} else {
				require.NoError(t, err)
				require.NotNil(t, row)
			}
		}
	}
}

// startServer will start sql-server with given host, unix socket file path and whether to use specific port, which is defined randomly.
func startServer(t *testing.T, withPort bool, host string, unixSocketPath string) (*env.DoltEnv, *sqlserver.ServerController, sqlserver.ServerConfig) {
	dEnv := dtestutils.CreateTestEnv()
	//serverConfig := sqlserver.DefaultServerConfig()
	//
	//if withPort {
	//	rand.Seed(time.Now().UnixNano())
	//	port := 15403 + rand.Intn(25)
	//	serverConfig = serverConfig.WithPort(port)
	//}
	//if host != "" {
	//	serverConfig = serverConfig.WithHost(host)
	//}
	//if unixSocketPath != "" {
	//	serverConfig = serverConfig.WithSocket(unixSocketPath)
	//}
	//
	//sc := sqlserver.NewServerController()
	//go func() {
	//	_, _ = sqlserver.Serve(context.Background(), "0.0.0", serverConfig, sc, dEnv)
	//}()
	//err := sc.WaitForStart()
	//require.NoError(t, err)

	sc, serverConfig := startServerFromDoltEnv(t, dEnv)

	return dEnv, sc, serverConfig
}

// newConnection takes sqlserver.serverConfig and opens a connection, and will return that connection with a new session
func newConnection(t *testing.T, serverConfig sqlserver.ServerConfig) (*dbr.Connection, *dbr.Session) {
	const dbName = "dolt"
	conn, err := dbr.Open("mysql", sqlserver.ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess := conn.NewSession(nil)
	return conn, sess
}

func startServerFromDoltEnv(t *testing.T, dEnv *env.DoltEnv) (*sqlserver.ServerController, sqlserver.ServerConfig) {
	serverConfig := sqlserver.DefaultServerConfig()

	rand.Seed(time.Now().UnixNano())
	port := 15403 + rand.Intn(25)
	serverConfig = serverConfig.WithPort(port)

	sc := sqlserver.NewServerController()
	go func() {
		_, _ = sqlserver.Serve(context.Background(), "0.0.0", serverConfig, sc, dEnv)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

	return sc, serverConfig
}

// BasicQueryist stuff
// vvvvvvvvvvvvvvvvvvv

func createQueryist(conn *dbr.Connection) (cli.Queryist, error) {
	queryist := BasicQueryist{conn: conn}
	return queryist, nil
}

type BasicRowIter struct {
	rows   *mysql.Rows
	schema sql.Schema
}

func (b BasicRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if !b.rows.Next() {
		return nil, io.EOF
	}
	cols, err := b.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	cli.Printf("cols: %v\n", cols)

	// DOES NOT WORK
	//vRow := make([]*string, len(b.schema))
	//iRow := make([]interface{}, len(b.schema))
	//for i := range iRow {
	//	iRow[i] = &vRow[i]
	//}
	//
	//err = b.rows.Scan(iRow...)
	//if err != nil {
	//	return nil, err
	//}
	//row := vRow

	// DOES NOT WORK
	//row := make(sql.Row, len(cols))
	//
	//for i, columnType := range cols {
	//	row[i] = reflect.New(columnType.ScanType()).Interface()
	//}
	//err = b.rows.Scan(row...)
	//if err != nil {
	//	return nil, err
	//}

	//row := make(sql.Row, len(cols))
	//for i := range cols {
	//	row[i] = reflect.New(cols[i].ScanType()).Interface()
	//}
	//
	//for i, columnType := range cols {
	//	row[i] = reflect.New(columnType.ScanType()).Interface()
	//}

	dataRow := make([]interface{}, len(cols))
	scanRow := make([]interface{}, len(cols))
	for i := range dataRow {
		scanRow[i] = &dataRow[i]
	}
	if err := b.rows.Scan(scanRow...); err != nil {
		return nil, err
	}
	row := dataRow

	out := make(sql.Row, len(row))
	for i, v := range row {
		col := b.schema[i]
		nv, _, err := col.Type.Convert(v)
		if err != nil {
			return nil, err
		}
		out[i] = nv
	}

	return out, nil
}

func (b BasicRowIter) Close(c *sql.Context) error {
	return b.rows.Close()
}

var _ sql.RowIter = (*BasicRowIter)(nil)

type BasicQueryist struct {
	conn *dbr.Connection
}

var _ cli.Queryist = (*BasicQueryist)(nil)

func (b BasicQueryist) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	rows, err := b.conn.Query(query)
	if err != nil {
		return nil, nil, err
	}

	//cols, err := rows.ColumnTypes()
	//if err != nil {
	//	return nil, nil, err
	//}
	//
	//schema := make(sql.Schema, len(cols))
	//for i, col := range cols {
	//	isNullable, ok := col.Nullable()
	//	if !ok {
	//		isNullable = true
	//	}
	//	sqlType, err := getSqlTypeFromColumnType(col)
	//	if err != nil {
	//		return nil, nil, err
	//	}
	//	schema[i] = &sql.Column{
	//		Name:     col.Name(),
	//		Type:     sqlType,
	//		Nullable: isNullable,
	//	}
	//}
	//
	//iter := BasicRowIter{
	//	rows:   rows,
	//	schema: schema,
	//}
	//if err != nil {
	//	return nil, nil, err
	//}
	//return schema, iter, nil

	iter, err := NewMySqlRowsIter(ctx, rows)
	if err != nil {
		return nil, nil, err
	}
	return iter.Schema(), iter, nil
}

func getSqlTypeFromColumnType(columnType *mysql.ColumnType) (sql.Type, error) {
	name := columnType.DatabaseTypeName()
	//nullable := columnType.Nullable()
	switch name {
	case "INT":
		return types.Int32, nil
	case "FLOAT":
		return types.Float32, nil
	case "DOUBLE":
		return types.Float64, nil
	case "BIGINT":
		return types.Int64, nil
	case "TEXT":
		return types.Text, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", name)
	}
}

// ^^^^^^^^^^^^^^^^^^^
// BasicQueryist stuff

// DbrRowIter stuff
// vvvvvvvvvvvvvvvvv

type DbrRowIter struct {
	rows      *mysql.Rows
	rowLength int
	schema    sql.Schema
	vRow      []*interface{}
	iRow      []interface{}
}

var _ sql.RowIter = (*DbrRowIter)(nil)

func (d DbrRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if !d.rows.Next() {
		return nil, io.EOF
	}

	columns, err := d.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	values := make([]interface{}, len(columns))
	object := map[string]interface{}{}
	for i, column := range columns {
		object[column.Name()] = reflect.New(column.ScanType()).Interface()
		values[i] = object[column.Name()]
	}
	err = d.rows.Scan(values...)
	if err != nil {
		return nil, err
	}

	//columnTypes, err := d.rows.ColumnTypes()
	//if err != nil {
	//	return nil, err
	//}
	//dest := makeDestinationSlice(columnTypes)
	//err = d.rows.Scan(dest...)
	//row := make(sql.Row, len(dest))
	//for i, v := range dest {
	//	row[i] = &v
	//}

	//err := d.rows.Scan(d.iRow...)
	//if err != nil {
	//	return nil, err
	//}
	row := make(sql.Row, len(values))
	for i, v := range values {
		//st := d.schema[i].Type
		//nv, _, err := st.Convert(v)
		//if err != nil {
		//	return nil, err
		//}
		//row[i] = nv

		row[i] = v
	}

	//sqlRow, err := rowToSQL(ctx, d.schema, d.vRow)
	//if err != nil {
	//	return nil, err
	//}
	//row := make(sql.Row, len(d.iRow))
	//for i, v := range d.vRow {
	//	row[i] = v
	//}
	return row, nil
}

func makeDestinationSlice(columnTypes []*mysql.ColumnType) []interface{} {
	dest := make([]any, len(columnTypes))
	for i, columnType := range columnTypes {
		switch strings.ToLower(columnType.DatabaseTypeName()) {
		case "bit":
			var bit int8
			dest[i] = &bit
		case "int", "tinyint", "bigint":
			var integer int
			dest[i] = &integer
		case "text":
			var s string
			dest[i] = &s
		default:
			panic("unsupported type: " + columnType.DatabaseTypeName())
		}
	}

	return dest
}

func (d DbrRowIter) Close(c *sql.Context) error {
	return d.rows.Close()
}

func rowToSQL(ctx *sql.Context, s sql.Schema, row sql.Row) ([]sqltypes.Value, error) {
	o := make([]sqltypes.Value, len(row))
	var err error
	for i, v := range row {
		if v == nil {
			o[i] = sqltypes.NULL
			continue
		}
		var t sql.Type
		if i >= len(s) {
			t = types.LongText
		} else {
			t = s[i].Type
		}
		o[i], err = t.SQL(ctx, nil, v)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

// ^^^^^^^^^^^^^^^^^^^
// DbrRowIter stuff

// DbrQueryist stuff
// vvvvvvvvvvvvvvvvv

type DbrQueryist struct {
	conn *dbr.Connection
}

var _ cli.Queryist = (*DbrQueryist)(nil)

func NewDbrQueryist(conn *dbr.Connection) *DbrQueryist {
	return &DbrQueryist{
		conn: conn,
	}
}

func (d DbrQueryist) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	rows, err := d.conn.Query(query)
	if err != nil {
		return nil, nil, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	rowLength := len(columnTypes)
	schema := make(sql.Schema, rowLength)
	for i, columnType := range columnTypes {
		name := columnNames[i]
		dbTypeName := columnType.DatabaseTypeName()
		sqlType, err := parse.ParseColumnTypeString(ctx, dbTypeName)
		nullable, ok := columnType.Nullable()
		if !ok {
			nullable = false
		}
		if err != nil {
			return nil, nil, err
		}
		schema[i] = &sql.Column{
			Name:     name,
			Type:     sqlType,
			Nullable: nullable,
		}
		//scanType := columnType.ScanType()
		//scanValue := reflect.New(scanType)
		//scanInterface := scanValue.Interface()
		//vRow[i] = scanInterface
		//vRow[i], err = getScanType(dbTypeName)
		//if err != nil {
		//	return nil, nil, err
		//}

		//iRow[i] = &vRow[i]
	}

	//rowIter := newMySQLIter(rows)
	rowIter := &fetchRowIter{
		rows:   rows,
		schema: schema,
	}
	return schema, rowIter, nil

	//rowIter := &DbrRowIter{
	//	schema:    schema,
	//	rows:      rows,
	//	rowLength: rowLength,
	//	vRow:      vRow,
	//	iRow:      iRow,
	//}
	//return schema, rowIter, nil
}

func getScanType(dbTypeName string) (interface{}, error) {
	switch dbTypeName {
	case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
		return new(mysql.NullString), nil
	case "BOOL":
		return new(mysql.NullBool), nil
	case "INT4":
		return new(mysql.NullInt64), nil
	default:
		return nil, fmt.Errorf("unsupported type %s", dbTypeName)
	}
}

// ^^^^^^^^^^^^^^^^^^^
// DbrQueryist stuff

// fetchRowIter stuff
// vvvvvvvvvvvvvvvvvv

type fetchRowIter struct {
	rows   *mysql.Rows
	schema sql.Schema
}

func (f fetchRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if !f.rows.Next() {
		return nil, io.EOF
	}

	cols, err := f.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	ts, err := schemaToTypes(ctx, cols)
	if err != nil {
		return nil, err
	}

	scanRow, err := scanResultRow(f.rows, cols)
	if err != nil {
		return nil, err
	}

	row := make(sql.Row, len(ts))
	for i := range row {
		scanRow[i], _, err = ts[i].Convert(scanRow[i])
		if err != nil {
			return nil, err
		}
		row[i], err = ts[i].SQL(ctx, nil, scanRow[i])
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}

func (f fetchRowIter) Close(c *sql.Context) error {
	return f.rows.Close()
}

var _ sql.RowIter = (*fetchRowIter)(nil)

//	var typeDefaults = map[string]string{
//		"char":      "char(255)",
//		"binary":    "binary(255)",
//		"varchar":   "varchar(65535)",
//		"varbinary": "varbinary(65535)",
//	}
func schemaToTypes(ctx *sql.Context, cols []*mysql.ColumnType) ([]sql.Type, error) {
	ts := make([]sql.Type, len(cols))

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
		ts[i], err = parse.ParseColumnTypeString(ctx, typeStr)
		if err != nil {
			return nil, err
		}
	}
	return ts, nil
}

//func scanResultRow(results *mysql.Rows) (sql.Row, error) {
//	cols, err := results.ColumnTypes()
//	if err != nil {
//		return nil, err
//	}
//
//	scanRow := make(sql.Row, len(cols))
//	for i := range cols {
//		scanRow[i] = reflect.New(cols[i].ScanType()).Interface()
//	}
//
//	for i, columnType := range cols {
//		scanRow[i] = reflect.New(columnType.ScanType()).Interface()
//	}
//
//	if err = results.Scan(scanRow...); err != nil {
//		return nil, err
//	}
//	for i, val := range scanRow {
//		v := reflect.ValueOf(val).Elem().Interface()
//		switch t := v.(type) {
//		case mysql.RawBytes:
//			if t == nil {
//				scanRow[i] = nil
//			} else {
//				scanRow[i] = string(t)
//			}
//		case mysql.NullBool:
//			if t.Valid {
//				scanRow[i] = t.Bool
//			} else {
//				scanRow[i] = nil
//			}
//		case mysql.NullByte:
//			if t.Valid {
//				scanRow[i] = t.Byte
//			} else {
//				scanRow[i] = nil
//			}
//		case mysql.NullFloat64:
//			if t.Valid {
//				scanRow[i] = t.Float64
//			} else {
//				scanRow[i] = nil
//			}
//		case mysql.NullInt16:
//			if t.Valid {
//				scanRow[i] = t.Int16
//			} else {
//				scanRow[i] = nil
//			}
//		case mysql.NullInt32:
//			if t.Valid {
//				scanRow[i] = t.Int32
//			} else {
//				scanRow[i] = nil
//			}
//		case mysql.NullInt64:
//			if t.Valid {
//				scanRow[i] = t.Int64
//			} else {
//				scanRow[i] = nil
//			}
//		case mysql.NullString:
//			if t.Valid {
//				scanRow[i] = t.String
//			} else {
//				scanRow[i] = nil
//			}
//		case mysql.NullTime:
//			if t.Valid {
//				scanRow[i] = t.Time
//			} else {
//				scanRow[i] = nil
//			}
//		default:
//			scanRow[i] = t
//		}
//	}
//	return scanRow, nil
//}

// ^^^^^^^^^^^^^^^^^^^
// fetchRowIter stuff

// mysqlIter stuff
// vvvvvvvvvvvvvvvvvvv

// mysqlIter wraps an iterator returned by the MySQL connection.
type mysqlIter struct {
	rows  *mysql.Rows
	types []reflect.Type
}

var _ sql.RowIter = mysqlIter{}

// newMySQLIter returns a new mysqlIter.
func newMySQLIter(rows *mysql.Rows) mysqlIter {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}
	types := make([]reflect.Type, len(columnTypes))
	for i, columnType := range columnTypes {
		scanType := columnType.ScanType()
		switch scanType {
		case reflect.TypeOf(mysql.RawBytes{}):
			scanType = reflect.TypeOf("")
		case reflect.TypeOf(mysql.NullBool{}):
			scanType = reflect.TypeOf(true)
		case reflect.TypeOf(mysql.NullByte{}):
			scanType = reflect.TypeOf(byte(0))
		case reflect.TypeOf(mysql.NullFloat64{}):
			scanType = reflect.TypeOf(float64(0))
		case reflect.TypeOf(mysql.NullInt16{}):
			scanType = reflect.TypeOf(int16(0))
		case reflect.TypeOf(mysql.NullInt32{}):
			scanType = reflect.TypeOf(int32(0))
		case reflect.TypeOf(mysql.NullInt64{}):
			scanType = reflect.TypeOf(int64(0))
		case reflect.TypeOf(mysql.NullString{}):
			scanType = reflect.TypeOf("")
		case reflect.TypeOf(mysql.NullTime{}):
			scanType = reflect.TypeOf(time.Time{})
		}
		types[i] = scanType
	}
	return mysqlIter{rows, types}
}

// Next implements the interface sql.RowIter.
func (m mysqlIter) Next(ctx *sql.Context) (sql.Row, error) {
	if m.rows.Next() {
		output := make(sql.Row, len(m.types))
		for i, typ := range m.types {
			output[i] = reflect.New(typ).Interface()
		}
		err := m.rows.Scan(output...)
		if err != nil {
			return nil, err
		}
		for i, val := range output {
			reflectVal := reflect.ValueOf(val)
			if reflectVal.IsNil() {
				output[i] = nil
			} else {
				output[i] = reflectVal.Elem().Interface()
				if byteSlice, ok := val.([]byte); ok {
					output[i] = string(byteSlice)
				}
			}
		}
		return output, nil
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (m mysqlIter) Close(ctx *sql.Context) error {
	return m.rows.Close()
}

// ^^^^^^^^^^^^^^^^^^^
// mysqlIter stuff
