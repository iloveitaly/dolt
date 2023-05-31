package main

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"
)

type testcase struct {
	query               string
	expectedResult      []interface{}
	expectedSchemaTypes []sql.Type
}

var bitType, _ = types.CreateBitType(1)
var decimalType, _ = types.CreateColumnDecimalType(10, 5)

var tests = []testcase{
	//{
	//	query: "select * from data",
	//	expectedResult: []interface{}{
	//		int64(100),
	//		uint64(1),
	//		int32(2),
	//		int16(3),
	//		float32(4),
	//		float64(5),
	//		int64(6),
	//	},
	//	expectedSchemaTypes: []sql.Type{
	//		types.Int64,
	//		bitType,
	//		types.Int32,
	//		types.Int16,
	//		types.Float32,
	//		types.Float64,
	//		types.Int64,
	//	},
	//},
	{
		query: "show create table data",
	},
	//{
	//	query:               "select bit1 from data",
	//	expectedResult:      []interface{}{"\x01"},
	//	expectedSchemaTypes: []sql.Type{bitType},
	//},
	{
		query:               "select integer2 from data",
		expectedResult:      []interface{}{int32(2)},
		expectedSchemaTypes: []sql.Type{types.Int32},
	},
	{
		query:               "select smallint3 from data",
		expectedResult:      []interface{}{int16(3)},
		expectedSchemaTypes: []sql.Type{types.Int16},
	},
	{
		query:               "select float_4 from data",
		expectedResult:      []interface{}{float32(4)},
		expectedSchemaTypes: []sql.Type{types.Float32},
	},
	{
		query:               "select double5 from data",
		expectedResult:      []interface{}{float64(5)},
		expectedSchemaTypes: []sql.Type{types.Float64},
	},
	{
		query:               "select bigInt6 from data",
		expectedResult:      []interface{}{int64(6)},
		expectedSchemaTypes: []sql.Type{types.Int64},
	},
	{
		query:               "select bool7 from data",
		expectedResult:      []interface{}{int8(1)},
		expectedSchemaTypes: []sql.Type{types.Boolean},
	},
	{
		query:               "select tinyint8 from data",
		expectedResult:      []interface{}{int8(8)},
		expectedSchemaTypes: []sql.Type{types.Int8},
	},
	{
		query:               "select smallint9 from data",
		expectedResult:      []interface{}{int16(9)},
		expectedSchemaTypes: []sql.Type{types.Int16},
	},
	{
		query:               "select mediumint10 from data",
		expectedResult:      []interface{}{int32(10)},
		expectedSchemaTypes: []sql.Type{types.Int24},
	},
	{
		query:               "select decimal11 from data",
		expectedResult:      []interface{}{float64(11.0123)},
		expectedSchemaTypes: []sql.Type{decimalType},
	},
}
var setupScripts = []string{
	`create table data (
		id BIGINT primary key,
		bit1 BIT(5),
		integer2 INTEGER,
		smallint3 SMALLINT,
		float_4 FLOAT,
		double5 DOUBLE,
		bigInt6 BIGINT,
		bool7 BOOLEAN,
		tinyint8 TINYINT,
		smallint9 SMALLINT,
		mediumint10 MEDIUMINT,
		decimal11 DECIMAL(10, 5)
	 );`,
	`insert into data values
		(100, 13, 2, 3, 4, 5, 6, true, 8, 9, 10, 11.0123);`,
}

func TestQueryistCases(t *testing.T) {
	for _, test := range tests {
		RunSingleTest(t, test)
	}
}

func RunSingleTest(t *testing.T, test testcase) {
	t.Run(test.query+"-SqlEngineQueryist", func(t *testing.T) {
		// SqlEngineQueryist

		// setup server engine
		ctx := context.Background()
		dEnv := dtestutils.CreateTestEnv()
		defer dEnv.DoltDB.Close()
		sqlEngine, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
		require.NoError(t, err)
		sqlCtx, err := sqlEngine.NewLocalContext(ctx)
		require.NoError(t, err)
		sqlCtx.SetCurrentDatabase(dbName)
		queryist := commands.NewSqlEngineQueryist(sqlEngine)

		// initialize server
		initServer(t, sqlCtx, queryist)

		// run test
		runTestcase(t, sqlCtx, queryist, test)
	})
	t.Run(test.query+"-ConnectionQueryist", func(t *testing.T) {
		// ConnectionQueryist

		// setup server
		dEnv, sc, serverConfig := startServer(t, true, "", "")
		err := sc.WaitForStart()
		require.NoError(t, err)
		defer dEnv.DoltDB.Close()
		conn, _ := newConnection(t, serverConfig)
		queryist := commands.NewConnectionQueryist(conn)
		ctx := context.TODO()
		sqlCtx := sql.NewContext(ctx)

		// initialize server
		initServer(t, sqlCtx, queryist)

		// run test
		runTestcase(t, sqlCtx, queryist, test)

		// close server
		require.NoError(t, conn.Close())
		sc.StopServer()
		err = sc.WaitForClose()
		require.NoError(t, err)
	})
}

func runTestcase(t *testing.T, sqlCtx *sql.Context, queryist cli.Queryist, test testcase) {
	// run test
	schema, rowIter, err := queryist.Query(sqlCtx, test.query)
	require.NoError(t, err)

	// get a row of data
	row, err := rowIter.Next(sqlCtx)
	require.NoError(t, err)

	// test result rows
	if len(test.expectedResult) > 0 {
		assert.Equal(t, len(test.expectedResult), len(row))

		for i, val := range row {
			expected := test.expectedResult[i]
			assert.Equal(t, expected, val)
		}
	} else {
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
	serverConfig := sqlserver.DefaultServerConfig()

	if withPort {
		rand.Seed(time.Now().UnixNano())
		port := 15403 + rand.Intn(25)
		serverConfig = serverConfig.WithPort(port)
	}
	if host != "" {
		serverConfig = serverConfig.WithHost(host)
	}
	if unixSocketPath != "" {
		serverConfig = serverConfig.WithSocket(unixSocketPath)
	}

	sc := sqlserver.NewServerController()
	go func() {
		_, _ = sqlserver.Serve(context.Background(), "0.0.0", serverConfig, sc, dEnv)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

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
