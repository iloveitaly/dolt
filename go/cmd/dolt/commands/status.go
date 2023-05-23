// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type printData struct {
	branchName string

	conflictsPresent,
	showIgnoredTables,
	statusPresent,
	mergeActive bool

	stagedTables,
	unstagedTables,
	untrackedTables,
	unmergedTables,
	ignoredTables map[string]string
}

var statusDocs = cli.CommandDocumentationContent{
	ShortDesc: "Show the working status",
	LongDesc:  `Displays working tables that differ from the current HEAD commit, tables that differ from the staged tables, and tables that are in the working tree that are not tracked by dolt. The first are what you would commit by running {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}; the second and third are what you could commit by running {{.EmphasisLeft}}dolt add .{{.EmphasisRight}} before running {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}.`,
	Synopsis:  []string{""},
}

type StatusCmd struct{}

func (cmd StatusCmd) RequiresRepo() bool {
	return false
}

var _ cli.RepoNotRequiredCommand = StatusCmd{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StatusCmd) Name() string {
	return "status"
}

// Description returns a description of the command
func (cmd StatusCmd) Description() string {
	return "Show the working tree status."
}

func (cmd StatusCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(statusDocs, ap)
}

func (cmd StatusCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(cli.ShowIgnoredFlag, "", "Show tables that are ignored (according to dolt_ignore)")
	return ap
}

// Exec executes the command
func (cmd StatusCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	// parse arguments
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, statusDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	showIgnoredTables := apr.Contains(cli.ShowIgnoredFlag)

	// configure SQL engine
	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return handleStatusVErr(err)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	// get ignored tables
	ignorePatterns, err := getIgnoredTablePatternsFromSql(queryist, sqlCtx)
	if err != nil {
		return handleStatusVErr(err)
	}

	// get current branch name
	branchName, err := getBranchName(queryist, sqlCtx)
	if err != nil {
		return handleStatusVErr(err)
	}

	// staged/working tables
	stagedTableNames := make(map[string]bool)
	workingTableNames := make(map[string]bool)
	diffs, err := getRowsForSql(queryist, sqlCtx, "select * from dolt_diff where commit_hash='WORKING' OR commit_hash='STAGED';")
	if err != nil {
		return handleStatusVErr(err)
	}
	for _, row := range diffs {
		commitHash := row[0].(string)
		tableName := row[1].(string)
		if commitHash == "STAGED" {
			stagedTableNames[tableName] = true
		} else {
			workingTableNames[tableName] = true
		}
	}

	// get merge status
	mergeRows, err := getRowsForSql(queryist, sqlCtx, "select * from dolt_merge_status;")
	if err != nil {
		return handleStatusVErr(err)
	}
	mergeActive := !(len(mergeRows) == 1 && mergeRows[0][0] == false)

	// get statuses
	statusRows, err := getRowsForSql(queryist, sqlCtx, "select * from dolt_status;")
	if err != nil {
		return handleStatusVErr(err)
	}
	statusPresent := len(statusRows) > 0

	// find conflicts in statuses
	conflictedTables := make(map[string]bool)
	for _, row := range statusRows {
		tableName := row[0].(string)
		status := row[2].(string)
		if status == "conflict" {
			conflictedTables[tableName] = true
		}
	}

	// sort tables into categories
	conflictsPresent := false
	stagedTables := map[string]string{}
	unstagedTables := map[string]string{}
	untrackedTables := map[string]string{}
	unmergedTables := map[string]string{}
	ignoredTables := map[string]string{}
	if statusPresent {
		for _, row := range statusRows {
			tableName := row[0].(string)
			stagedData := row[1]
			status := row[2].(string)

			// determine if table is staged
			var isStaged bool
			if isStagedString, ok := stagedData.(string); ok {
				isStaged = isStagedString == "1"
			} else if isStagedBool, ok := stagedData.(bool); ok {
				isStaged = isStagedBool
			} else {
				return handleStatusVErr(errors.New("unexpected type for staged column"))
			}

			// determine if the table should be ignored
			ignoreTableValue, err := ignorePatterns.IsTableNameIgnored(tableName)
			if err != nil {
				return handleStatusVErr(err)
			}
			shouldIgnoreTable := ignoreTableValue == doltdb.Ignore

			switch status {
			case "renamed":
				// for renamed tables, add both source and dest changes
				parts := strings.Split(tableName, " -> ")
				srcTableName := parts[0]
				dstTableName := parts[1]

				if workingTableNames[dstTableName] {
					unstagedTables[srcTableName] = "deleted"
					untrackedTables[dstTableName] = "new table"
				} else if stagedTableNames[dstTableName] {
					stagedTables[tableName] = status
				}
			case "conflict":
				conflictsPresent = true
				if isStaged {
					stagedTables[tableName] = status
				} else {
					unmergedTables[tableName] = "both modified"
				}
			case "deleted", "modified", "added", "new table":
				if shouldIgnoreTable {
					ignoredTables[tableName] = status
					continue
				}

				if isStaged {
					stagedTables[tableName] = status
				} else {
					isTableConflicted := conflictedTables[tableName]
					if !isTableConflicted {
						if status == "new table" {
							untrackedTables[tableName] = status
						} else {
							unstagedTables[tableName] = status
						}
					}
				}

			default:
				panic(fmt.Sprintf("table %s, unexpected merge status: %s", tableName, status))
			}
		}
	}

	// print everything
	printEverything(printData{
		branchName:        branchName,
		conflictsPresent:  conflictsPresent,
		showIgnoredTables: showIgnoredTables,
		statusPresent:     statusPresent,
		mergeActive:       mergeActive,
		stagedTables:      stagedTables,
		unstagedTables:    unstagedTables,
		untrackedTables:   untrackedTables,
		unmergedTables:    unmergedTables,
		ignoredTables:     ignoredTables,
	})

	return 0
}

func getBranchName(queryist cli.Queryist, sqlCtx *sql.Context) (string, error) {
	rows, err := getRowsForSql(queryist, sqlCtx, "select active_branch()")
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", errors.New("expected one row in dolt_branches")
	}
	branchName := rows[0][0].(string)
	return branchName, nil
}

func getRowsForSql(queryist cli.Queryist, sqlCtx *sql.Context, q string) ([]sql.Row, error) {
	schema, ri, err := queryist.Query(sqlCtx, q)
	if err != nil {
		return nil, err
	}
	rows, err := sql.RowIterToRows(sqlCtx, schema, ri)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func getIgnoredTablePatternsFromSql(queryist cli.Queryist, sqlCtx *sql.Context) (doltdb.IgnorePatterns, error) {
	var ignorePatterns []doltdb.IgnorePattern
	ignoreRows, err := getRowsForSql(queryist, sqlCtx, fmt.Sprintf("select * from %s", doltdb.IgnoreTableName))
	if err != nil {
		return nil, err
	}
	for _, row := range ignoreRows {
		pattern := row[0].(string)
		ignoreVal := row[1]

		var ignore bool
		if ignoreString, ok := ignoreVal.(string); ok {
			ignore = ignoreString == "1"
		} else if ignoreInt, ok := ignoreVal.(int8); ok {
			ignore = ignoreInt == 1
		} else {
			return nil, errors.New(fmt.Sprintf("unexpected type for ignore column, value = %s", ignoreVal))
		}

		ip := doltdb.NewIgnorePattern(pattern, ignore)
		ignorePatterns = append(ignorePatterns, ip)
	}
	return ignorePatterns, nil
}

func printEverything(data printData) {
	statusFmt := "\t%-18s%s"

	// branch name
	cli.Printf(branchHeader, data.branchName)

	// conflicts
	if data.conflictsPresent {
		cli.Println("\nYou have unmerged tables.")
		cli.Println("  (fix conflicts and run \"dolt commit\")")
		cli.Println("  (use \"dolt merge --abort\" to abort the merge)")
	}

	// staged tables
	if len(data.stagedTables) > 0 {
		cli.Println("Changes to be committed:")
		cli.Println("  (use \"dolt reset <table>...\" to unstage)")
		for tableName, status := range data.stagedTables {
			text := fmt.Sprintf(statusFmt, status+":", tableName)
			greenText := color.GreenString(text)
			cli.Println(greenText)
		}
	}

	// unstaged tables
	if len(data.unstagedTables) > 0 {
		cli.Println("\nChanges not staged for commit:")
		cli.Println("  (use \"dolt add <table>\" to update what will be committed)")
		cli.Println("  (use \"dolt checkout <table>\" to discard changes in working directory)")
		for tableName, status := range data.unstagedTables {
			text := fmt.Sprintf(statusFmt, status+":", tableName)
			redText := color.RedString(text)
			cli.Println(redText)
		}
	}

	// untracked tables
	if len(data.untrackedTables) > 0 {
		cli.Println("\nUntracked tables:")
		cli.Println("  (use \"dolt add <table>\" to include in what will be committed)")
		for tableName, status := range data.untrackedTables {
			text := fmt.Sprintf(statusFmt, status+":", tableName)
			redText := color.RedString(text)
			cli.Println(redText)
		}
	}

	// unmerged paths
	if len(data.unmergedTables) > 0 {
		cli.Println("\nUnmerged paths:")
		cli.Println("  (use \"dolt add <table>...\" to mark resolution)")
		cli.Println("  (use \"dolt commit\" once resolved)")
		for tableName, status := range data.unmergedTables {
			text := fmt.Sprintf(statusFmt, status+":", tableName)
			redText := color.RedString(text)
			cli.Println(redText)
		}
	}

	// ignored tables
	if data.showIgnoredTables && len(data.ignoredTables) > 0 {
		cli.Println("\nIgnored tables:")
		cli.Println("  (use \"dolt add -f <table>\" to include in what will be committed)")
		for tableName, status := range data.ignoredTables {
			text := fmt.Sprintf(statusFmt, status+":", tableName)
			redText := color.RedString(text)
			cli.Println(redText)
		}
	}

	// nothing to commit
	if !data.statusPresent && !data.mergeActive {
		cli.Println("nothing to commit, working tree clean")
	}
}

func handleStatusVErr(err error) int {
	cli.PrintErrln(errhand.VerboseErrorFromError(err).Verbose())
	return 1
}
