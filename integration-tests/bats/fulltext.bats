#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "The remote engine complicates the test structure for now"
    fi
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "fulltext: basic persistence checking" {
    dolt sql -q "CREATE TABLE test (pk1 BIGINT UNSIGNED, pk2 BIGINT UNSIGNED, v1 VARCHAR(200), v2 VARCHAR(200), PRIMARY KEY (pk1, pk2), FULLTEXT idx (v1, v2));"
    dolt sql -q "INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');"
    run dolt sql -q "SELECT v2 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');" -r=json
    [[ "$output" =~ "{\"rows\": [{\"v2\":\"jkl\"}]}" ]] || false
}

@test "fulltext: basic merge" {
    dolt sql -q "CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));"
    dolt sql -q "INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');"

    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_global_count;"
    [[ "$output" =~ "| word | global_count |" ]] || false
    [[ "$output" =~ "| abc  | 1            |" ]] || false
    [[ "$output" =~ "| def  | 1            |" ]] || false
    [[ "$output" =~ "| ghi  | 1            |" ]] || false
    [[ "$output" =~ "| ghs  | 1            |" ]] || false
    [[ "$output" =~ "| jkl  | 1            |" ]] || false
    [[ "$output" =~ "| mno  | 2            |" ]] || false
    [[ "$output" =~ "| pqr  | 1            |" ]] || false
    [[ "$output" =~ "| shg  | 1            |" ]] || false
    [[ "$output" =~ "| stu  | 1            |" ]] || false
    [[ "$output" =~ "| vwx  | 1            |" ]] || false
    [[ "$output" =~ "| xyz  | 1            |" ]] || false
    [[ "$output" =~ "| yzx  | 1            |" ]] || false
    [[ "$output" =~ "| zyx  | 1            |" ]] || false

    dolt add -A
    dolt commit -m "Initial commit"
    dolt branch other

    dolt sql -q "DELETE FROM test WHERE pk = 3;"
    dolt add -A
    dolt commit -m "Main commit"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (6, 'jak', 'mno'), (7, 'mno', 'bot');"
    dolt add -A
    dolt commit -m "Other commit"

    dolt checkout main
    dolt merge other
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_global_count;"
    [[ "$output" =~ "| word | global_count |" ]] || false
    [[ "$output" =~ "| abc  | 1            |" ]] || false
    [[ "$output" =~ "| bot  | 1            |" ]] || false
    [[ "$output" =~ "| def  | 1            |" ]] || false
    [[ "$output" =~ "| ghi  | 1            |" ]] || false
    [[ "$output" =~ "| ghs  | 1            |" ]] || false
    [[ "$output" =~ "| jak  | 1            |" ]] || false
    [[ "$output" =~ "| jkl  | 1            |" ]] || false
    [[ "$output" =~ "| mno  | 3            |" ]] || false
    [[ "$output" =~ "| pqr  | 1            |" ]] || false
    [[ "$output" =~ "| shg  | 1            |" ]] || false
    [[ "$output" =~ "| stu  | 1            |" ]] || false
    [[ "$output" =~ "| vwx  | 1            |" ]] || false
    [[ "$output" =~ "| xyz  | 1            |" ]] || false
    [[ "$output" =~ "| yzx  | 1            |" ]] || false
    [[ "$output" =~ "| zyx  | 1            |" ]] || false
}

@test "fulltext: drop index, tables removed" {
    dolt sql -q "CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), FULLTEXT idx (v1));"
    dolt sql -q "INSERT INTO test VALUES (1, 'abc');"
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_position;"
    [[ "$output" =~ "| word | C0 | position |" ]] || false
    [[ "$output" =~ "| abc  | 1  | 0        |" ]] || false
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_doc_count;"
    [[ "$output" =~ "| word | C0 | doc_count |" ]] || false
    [[ "$output" =~ "| abc  | 1  | 1         |" ]] || false
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_global_count;"
    [[ "$output" =~ "| word | global_count |" ]] || false
    [[ "$output" =~ "| abc  | 1            |" ]] || false
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_row_count;"
    [[ "$output" =~ "| row_hash                                                         | row_count | unique_words |" ]] || false
    [[ "$output" =~ "| c38b3e71346a4847af87d87153e01eae2d83d905df14cc09ec1ac30516ec44ed | 1         | 1            |" ]] || false

    dolt sql -q "DROP INDEX idx ON test;"
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_position;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_doc_count;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_global_count;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_row_count;"
    [ "$status" -eq 1 ]
}

@test "fulltext: drop index on other branch, ff merge" {
    dolt sql -q "CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), FULLTEXT idx (v1));"
    dolt sql -q "INSERT INTO test VALUES (1, 'abc');"
    dolt add -A
    dolt commit -m "Initial commit"
    dolt checkout -b other
    dolt sql -q "DROP INDEX idx ON test;"
    dolt add -A
    dolt commit -m "Dropped index"

    dolt checkout main
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 0 ]
    dolt merge other
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_position;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_doc_count;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_global_count;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_row_count;"
    [ "$status" -eq 1 ]
}

@test "fulltext: drop index on other branch, no-ff merge" {
    dolt sql -q "CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), FULLTEXT idx (v1));"
    dolt sql -q "INSERT INTO test VALUES (1, 'abc');"
    dolt add -A
    dolt commit -m "Initial commit"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (2, 'def');"
    dolt add -A
    dolt commit -m "Insertion commit"

    dolt checkout other
    dolt sql -q "DROP INDEX idx ON test;"
    dolt sql -q "INSERT INTO test VALUES (3, 'ghi');"
    dolt add -A
    dolt commit -m "Dropped index"

    dolt checkout main
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 0 ]
    dolt merge other
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_position;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_doc_count;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_global_count;"
    [ "$status" -eq 1 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_row_count;"
    [ "$status" -eq 1 ]
}

@test "fulltext: create index on other branch, ff merge" {
    dolt sql -q "CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200));"
    dolt sql -q "INSERT INTO test VALUES (1, 'abc');"
    dolt add -A
    dolt commit -m "Initial commit"

    dolt checkout -b other
    dolt sql -q "CREATE FULLTEXT INDEX idx ON test (v1);"
    dolt sql -q "INSERT INTO test VALUES (3, 'ghi');"
    dolt add -A
    dolt commit -m "Created index"

    dolt checkout main
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 1 ]
    dolt merge other
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_position;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_doc_count;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_global_count;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_row_count;"
    [ "$status" -eq 0 ]
}

@test "fulltext: create index on other branch, no-ff merge" {
    dolt sql -q "CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200));"
    dolt sql -q "INSERT INTO test VALUES (1, 'abc');"
    dolt add -A
    dolt commit -m "Initial commit"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (2, 'def');"
    dolt add -A
    dolt commit -m "Insertion commit"

    dolt checkout other
    dolt sql -q "CREATE FULLTEXT INDEX idx ON test (v1);"
    dolt sql -q "INSERT INTO test VALUES (3, 'ghi');"
    dolt add -A
    dolt commit -m "Created index"

    dolt checkout main
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 1 ]
    dolt merge other
    run dolt sql -q "SELECT * FROM dolt_test_fts_config;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_position;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_doc_count;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_global_count;"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM dolt_test_idx_0_fts_row_count;"
    [ "$status" -eq 0 ]
}
