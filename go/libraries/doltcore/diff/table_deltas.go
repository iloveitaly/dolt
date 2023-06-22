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

package diff

import (
	"context"
	"fmt"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type TableDiffType int

const (
	AddedTable TableDiffType = iota
	ModifiedTable
	RenamedTable
	RemovedTable
)

type TableInfo struct {
	Name         string
	Sch          schema.Schema
	CreateStmt   string
	Fks          []ForeignKeyInfo
	FksParentSch map[string]schema.Schema
}

type ForeignKeyInfo struct {
	Name                            string
	IsResolved                      bool
	TableName                       string
	TableIndex                      string
	TableColumns                    []string
	ReferencedTableName             string
	ReferencedTableIndex            string
	ReferencedTableColumns          []string
	OnUpdate                        string
	OnDelete                        string
	UnresolvedTableColumns          []string
	UnresolvedReferenceTableColumns []string
}

// TableDelta represents the change of a single table between two roots.
// FromFKs and ToFKs contain Foreign Keys that constrain columns in this table,
// they do not contain Foreign Keys that reference this table.
type TableDelta struct {
	FromName         string
	ToName           string
	FromTable        *doltdb.Table
	ToTable          *doltdb.Table
	FromNodeStore    tree.NodeStore
	ToNodeStore      tree.NodeStore
	FromVRW          types.ValueReadWriter
	ToVRW            types.ValueReadWriter
	FromSch          schema.Schema
	ToSch            schema.Schema
	FromFks          []doltdb.ForeignKey
	ToFks            []doltdb.ForeignKey
	ToFksParentSch   map[string]schema.Schema
	FromFksParentSch map[string]schema.Schema
}

type TableDeltaSummary struct {
	DiffType      string
	DataChange    bool
	SchemaChange  bool
	TableName     string
	FromTableName string
	ToTableName   string
	AlterStmts    []string
	PkChanged     bool
}

// DeepEquals compares all attributes of a foreign key to another, including name and
// table names. Note that if one foreign key is resolved and the other is NOT resolved,
// then this function will not calculate equality correctly. When comparing a resolved
// FK with an unresolved FK, the ForeignKey.Equals() function should be used instead.
func (fk ForeignKeyInfo) DeepEquals(other ForeignKeyInfo) bool {
	if !fk.EqualDefs(other) {
		return false
	}
	return fk.Name == other.Name &&
		fk.TableName == other.TableName &&
		fk.ReferencedTableName == other.ReferencedTableName &&
		fk.TableIndex == other.TableIndex &&
		fk.ReferencedTableIndex == other.ReferencedTableIndex
}

// EqualDefs returns whether two foreign keys have the same definition over the same column sets.
// It does not compare table names or foreign key names.
func (fk ForeignKeyInfo) EqualDefs(other ForeignKeyInfo) bool {
	if len(fk.TableColumns) != len(other.TableColumns) || len(fk.ReferencedTableColumns) != len(other.ReferencedTableColumns) {
		return false
	}
	for i := range fk.TableColumns {
		if fk.TableColumns[i] != other.TableColumns[i] {
			return false
		}
	}
	for i := range fk.ReferencedTableColumns {
		if fk.ReferencedTableColumns[i] != other.ReferencedTableColumns[i] {
			return false
		}
	}
	return fk.Name == other.Name &&
		fk.OnUpdate == other.OnUpdate &&
		fk.OnDelete == other.OnDelete
}

// Equals compares this ForeignKey to |other| and returns true if they are equal. Foreign keys can either be in
// a "resolved" state, where the referenced columns in the parent and child tables are identified by column tags,
// or in an "unresolved" state where the reference columns in the parent and child are still identified by strings.
// If one foreign key is resolved and one is unresolved, the logic for comparing them requires resolving the string
// column names to column tags, which is why |fkSchemasByName| and |otherSchemasByName| are passed in. Each of these
// is a map of table schemas for |fk| and |other|, where the child table and every parent table referenced in the
// foreign key is present in the map.
func (fk ForeignKeyInfo) Equals(other ForeignKeyInfo) bool {
	// If both FKs are resolved or unresolved, we can just deeply compare them
	if fk.IsResolved == other.IsResolved {
		return fk.DeepEquals(other)
	}

	// Otherwise, one FK is resolved and one is not, so we need to work a little harder
	// to calculate equality since their referenced columns are represented differently.
	// First check the attributes that don't change when an FK is resolved or unresolved.
	if fk.Name != other.Name &&
		fk.TableName != other.TableName &&
		fk.ReferencedTableName != other.ReferencedTableName &&
		fk.TableIndex != other.TableIndex &&
		fk.ReferencedTableIndex != other.ReferencedTableIndex &&
		fk.OnUpdate == other.OnUpdate &&
		fk.OnDelete == other.OnDelete {
		return false
	}

	// Sort out which FK is resolved and which is not
	var resolvedFK, unresolvedFK ForeignKeyInfo
	if fk.IsResolved {
		resolvedFK, unresolvedFK = fk, other
	} else {
		resolvedFK, unresolvedFK = other, fk
	}

	// Check the columns on the child table
	if len(resolvedFK.TableColumns) != len(unresolvedFK.UnresolvedTableColumns) {
		return false
	}
	for i, colName := range resolvedFK.TableColumns {
		unresolvedColName := unresolvedFK.UnresolvedTableColumns[i]
		if colName != unresolvedColName {
			return false
		}
	}

	// Check the columns on the parent table
	if len(resolvedFK.ReferencedTableColumns) != len(unresolvedFK.UnresolvedReferenceTableColumns) {
		return false
	}
	for i, colName := range resolvedFK.ReferencedTableColumns {
		unresolvedColName := unresolvedFK.UnresolvedReferenceTableColumns[i]
		if colName != unresolvedColName {
			return false
		}
	}

	return true
}

// IsAdd returns true if the table was added between the fromRoot and toRoot.
func (tds TableDeltaSummary) IsAdd() bool {
	return tds.FromTableName == "" && tds.ToTableName != ""
}

// IsDrop returns true if the table was dropped between the fromRoot and toRoot.
func (tds TableDeltaSummary) IsDrop() bool {
	return tds.FromTableName != "" && tds.ToTableName == ""
}

// IsRename return true if the table was renamed between the fromRoot and toRoot.
func (tds TableDeltaSummary) IsRename() bool {
	if tds.IsAdd() || tds.IsDrop() {
		return false
	}
	return tds.FromTableName != tds.ToTableName
}

// GetStagedUnstagedTableDeltas represents staged and unstaged changes as TableDelta slices.
func GetStagedUnstagedTableDeltas(ctx context.Context, roots doltdb.Roots) (staged, unstaged []TableDelta, err error) {
	staged, err = GetTableDeltas(ctx, roots.Head, roots.Staged)
	if err != nil {
		return nil, nil, err
	}

	unstaged, err = GetTableDeltas(ctx, roots.Staged, roots.Working)
	if err != nil {
		return nil, nil, err
	}

	return staged, unstaged, nil
}

// GetTableDeltas returns a slice of TableDelta objects for each table that changed between fromRoot and toRoot.
// It matches tables across roots by finding Schemas with Column tags in common.
func GetTableDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) (deltas []TableDelta, err error) {
	fromVRW := fromRoot.VRW()
	fromNS := fromRoot.NodeStore()
	toVRW := toRoot.VRW()
	toNS := toRoot.NodeStore()

	fromDeltas := make([]TableDelta, 0)
	err = fromRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		c, err := fromRoot.GetForeignKeyCollection(ctx)
		if err != nil {
			return true, err
		}
		fks, _ := c.KeysForTable(name)
		parentSchs, err := getFkParentSchs(ctx, fromRoot, fks...)
		if err != nil {
			return false, err
		}

		fromDeltas = append(fromDeltas, TableDelta{
			FromName:         name,
			FromTable:        tbl,
			FromSch:          sch,
			FromFks:          fks,
			FromFksParentSch: parentSchs,
			FromVRW:          fromVRW,
			FromNodeStore:    fromNS,
			ToVRW:            toVRW,
			ToNodeStore:      toNS,
		})
		return
	})
	if err != nil {
		return nil, err
	}

	toDeltas := make([]TableDelta, 0)

	err = toRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		c, err := toRoot.GetForeignKeyCollection(ctx)
		if err != nil {
			return true, err
		}

		fks, _ := c.KeysForTable(name)
		parentSchs, err := getFkParentSchs(ctx, toRoot, fks...)
		if err != nil {
			return false, err
		}

		toDeltas = append(toDeltas, TableDelta{
			ToName:         name,
			ToTable:        tbl,
			ToSch:          sch,
			ToFks:          fks,
			ToFksParentSch: parentSchs,
			FromVRW:        fromVRW,
			FromNodeStore:  fromNS,
			ToVRW:          toVRW,
			ToNodeStore:    toNS,
		})
		return
	})
	if err != nil {
		return nil, err
	}

	deltas = matchTableDeltas(fromDeltas, toDeltas)
	deltas, err = filterUnmodifiedTableDeltas(deltas)
	if err != nil {
		return nil, err
	}

	// Make sure we always return the same order of deltas
	sort.Slice(deltas, func(i, j int) bool {
		if deltas[i].FromName == deltas[j].FromName {
			return deltas[i].ToName < deltas[j].ToName
		}
		return deltas[i].FromName < deltas[j].FromName
	})

	return deltas, nil
}

func getFkParentSchs(ctx context.Context, root *doltdb.RootValue, fks ...doltdb.ForeignKey) (map[string]schema.Schema, error) {
	schs := make(map[string]schema.Schema)
	for _, toFk := range fks {
		toRefTable, _, ok, err := root.GetTableInsensitive(ctx, toFk.ReferencedTableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // as the schemas are for display-only, we can skip on any missing parents (they were deleted, etc.)
		}
		toRefSch, err := toRefTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		schs[toFk.ReferencedTableName] = toRefSch
	}
	return schs, nil
}

func filterUnmodifiedTableDeltas(deltas []TableDelta) ([]TableDelta, error) {
	var filtered []TableDelta
	for _, d := range deltas {
		if d.ToTable == nil || d.FromTable == nil {
			// Table was added or dropped
			filtered = append(filtered, d)
			continue
		}

		hasChanges, err := d.HasChanges()
		if err != nil {
			return nil, err
		}

		if hasChanges {
			// Take only modified tables
			filtered = append(filtered, d)
		}
	}

	return filtered, nil
}

func matchTableDeltas(fromDeltas, toDeltas []TableDelta) (deltas []TableDelta) {
	var matchedNames []string
	from := make(map[string]TableDelta, len(fromDeltas))
	for _, f := range fromDeltas {
		from[f.FromName] = f
	}

	to := make(map[string]TableDelta, len(toDeltas))
	for _, t := range toDeltas {
		to[t.ToName] = t
		if _, ok := from[t.ToName]; ok {
			matchedNames = append(matchedNames, t.ToName)
		}
	}

	match := func(t, f TableDelta) TableDelta {
		return TableDelta{
			FromName:         f.FromName,
			ToName:           t.ToName,
			FromTable:        f.FromTable,
			ToTable:          t.ToTable,
			FromSch:          f.FromSch,
			ToSch:            t.ToSch,
			FromFks:          f.FromFks,
			ToFks:            t.ToFks,
			FromFksParentSch: f.FromFksParentSch,
			ToFksParentSch:   t.ToFksParentSch,
		}
	}

	deltas = make([]TableDelta, 0)

	for _, name := range matchedNames {
		t := to[name]
		f := from[name]
		if schemasOverlap(t.ToSch, f.FromSch) {
			matched := match(t, f)
			deltas = append(deltas, matched)
			delete(from, f.FromName)
			delete(to, t.ToName)
		}
	}

	for _, f := range from {
		for _, t := range to {
			if schemasOverlap(f.FromSch, t.ToSch) {
				matched := match(t, f)
				deltas = append(deltas, matched)
				delete(from, f.FromName)
				delete(to, t.ToName)
			}
		}
	}

	// append unmatched TableDeltas
	for _, f := range from {
		deltas = append(deltas, f)
	}
	for _, t := range to {
		deltas = append(deltas, t)
	}

	return deltas
}

func schemasOverlap(from, to schema.Schema) bool {
	f := set.NewUint64Set(from.GetAllCols().Tags)
	t := set.NewUint64Set(to.GetAllCols().Tags)
	return f.Intersection(t).Size() > 0
}

// IsAdd returns true if the table was added between the fromRoot and toRoot.
func (td TableDelta) IsAdd() bool {
	return td.FromTable == nil && td.ToTable != nil
}

// IsDrop returns true if the table was dropped between the fromRoot and toRoot.
func (td TableDelta) IsDrop() bool {
	return td.FromTable != nil && td.ToTable == nil
}

// IsRename return true if the table was renamed between the fromRoot and toRoot.
func (td TableDelta) IsRename() bool {
	if td.IsAdd() || td.IsDrop() {
		return false
	}
	return td.FromName != td.ToName
}

// HasHashChanged returns true if the hash of the table content has changed between
// the fromRoot and toRoot.
func (td TableDelta) HasHashChanged() (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	toHash, err := td.ToTable.HashOf()
	if err != nil {
		return false, err
	}

	fromHash, err := td.FromTable.HashOf()
	if err != nil {
		return false, err
	}

	return !toHash.Equal(fromHash), nil
}

// HasSchemaChanged returns true if the table schema has changed between the
// fromRoot and toRoot.
func (td TableDelta) HasSchemaChanged(ctx context.Context) (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	if td.HasFKChanges() {
		return true, nil
	}

	fromSchemaHash, err := td.FromTable.GetSchemaHash(ctx)
	if err != nil {
		return false, err
	}

	toSchemaHash, err := td.ToTable.GetSchemaHash(ctx)
	if err != nil {
		return false, err
	}

	return !fromSchemaHash.Equal(toSchemaHash), nil
}

func (td TableDelta) HasDataChanged(ctx context.Context) (bool, error) {
	if td.IsAdd() {
		isEmpty, err := isTableDataEmpty(ctx, td.ToTable)
		if err != nil {
			return false, err
		}

		return !isEmpty, nil
	}

	if td.IsDrop() {
		isEmpty, err := isTableDataEmpty(ctx, td.FromTable)
		if err != nil {
			return false, err
		}
		return !isEmpty, nil
	}

	fromRowDataHash, err := td.FromTable.GetRowDataHash(ctx)
	if err != nil {
		return false, err
	}

	toRowDataHash, err := td.ToTable.GetRowDataHash(ctx)
	if err != nil {
		return false, err
	}

	return !fromRowDataHash.Equal(toRowDataHash), nil
}

func (td TableDelta) HasPrimaryKeySetChanged() bool {
	return !schema.ArePrimaryKeySetsDiffable(td.Format(), td.FromSch, td.ToSch)
}

func (td TableDelta) HasChanges() (bool, error) {
	hashChanged, err := td.HasHashChanged()
	if err != nil {
		return false, err
	}

	return td.HasFKChanges() || td.IsRename() || td.HasPrimaryKeySetChanged() || hashChanged, nil
}

// CurName returns the most recent name of the table.
func (td TableDelta) CurName() string {
	if td.ToName != "" {
		return td.ToName
	}
	return td.FromName
}

func (td TableDelta) HasFKChanges() bool {
	if len(td.FromFks) != len(td.ToFks) {
		return true
	}

	sort.Slice(td.FromFks, func(i, j int) bool {
		return td.FromFks[i].Name < td.FromFks[j].Name
	})
	sort.Slice(td.ToFks, func(i, j int) bool {
		return td.ToFks[i].Name < td.ToFks[j].Name
	})

	fromSchemaMap := td.FromFksParentSch
	fromSchemaMap[td.FromName] = td.FromSch
	toSchemaMap := td.ToFksParentSch
	toSchemaMap[td.ToName] = td.ToSch

	for i := range td.FromFks {
		if !td.FromFks[i].Equals(td.ToFks[i], fromSchemaMap, toSchemaMap) {
			return true
		}
	}

	return false
}

// GetSchemas returns the table's schema at the fromRoot and toRoot, or schema.Empty if the table did not exist.
func (td TableDelta) GetSchemas(ctx context.Context) (from, to schema.Schema, err error) {
	if td.FromSch == nil {
		td.FromSch = schema.EmptySchema
	}
	if td.ToSch == nil {
		td.ToSch = schema.EmptySchema
	}
	return td.FromSch, td.ToSch, nil
}

// Format returns the format of the tables in this delta.
func (td TableDelta) Format() *types.NomsBinFormat {
	if td.FromTable != nil {
		return td.FromTable.Format()
	}
	return td.ToTable.Format()
}

func (td TableDelta) IsKeyless(ctx context.Context) (bool, error) {
	f, t, err := td.GetSchemas(ctx)
	if err != nil {
		return false, err
	}

	// nil table is neither keyless nor keyed
	from, to := schema.IsKeyless(f), schema.IsKeyless(t)
	if td.FromTable == nil {
		return to, nil
	} else if td.ToTable == nil {
		return from, nil
	} else {
		if from && to {
			return true, nil
		} else if !from && !to {
			return false, nil
		} else {
			return false, fmt.Errorf("mismatched keyless and keyed schemas for table %s", td.CurName())
		}
	}
}

// isTableDataEmpty return true if the table does not contain any data
func isTableDataEmpty(ctx context.Context, table *doltdb.Table) (bool, error) {
	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return false, err
	}

	return rowData.Empty()
}

// GetSummary returns a summary of the table delta.
func (td TableDelta) GetSummary(ctx context.Context) (*TableDeltaSummary, error) {
	dataChange, err := td.HasDataChanged(ctx)
	if err != nil {
		return nil, err
	}

	// Dropping a table is always a schema change, and also a data change if the table contained data
	if td.IsDrop() {
		return &TableDeltaSummary{
			TableName:     td.FromName,
			FromTableName: td.FromName,
			DataChange:    dataChange,
			SchemaChange:  true,
			DiffType:      "dropped",
		}, nil
	}

	// Creating a table is always a schema change, and also a data change if data was inserted
	if td.IsAdd() {
		return &TableDeltaSummary{
			TableName:    td.ToName,
			ToTableName:  td.ToName,
			DataChange:   dataChange,
			SchemaChange: true,
			DiffType:     "added",
		}, nil
	}

	// Renaming a table is always a schema change, and also a data change if the table data differs
	if td.IsRename() {
		return &TableDeltaSummary{
			TableName:     td.ToName,
			FromTableName: td.FromName,
			ToTableName:   td.ToName,
			DataChange:    dataChange,
			SchemaChange:  true,
			DiffType:      "renamed",
		}, nil
	}

	schemaChange, err := td.HasSchemaChanged(ctx)
	if err != nil {
		return nil, err
	}

	return &TableDeltaSummary{
		TableName:     td.FromName,
		FromTableName: td.FromName,
		ToTableName:   td.ToName,
		DataChange:    dataChange,
		SchemaChange:  schemaChange,
		DiffType:      "modified",
	}, nil
}

// GetRowData returns the table's row data at the fromRoot and toRoot, or an empty map if the table did not exist.
func (td TableDelta) GetRowData(ctx context.Context) (from, to durable.Index, err error) {
	if td.FromTable == nil && td.ToTable == nil {
		return nil, nil, fmt.Errorf("both from and to tables are missing from table delta")
	}

	if td.FromTable != nil {
		from, err = td.FromTable.GetRowData(ctx)
		if err != nil {
			return from, to, err
		}
	} else {
		// If there is no |FromTable| use the |ToTable|'s schema to make the index.
		from, _ = durable.NewEmptyIndex(ctx, td.FromVRW, td.FromNodeStore, td.ToSch)
	}

	if td.ToTable != nil {
		to, err = td.ToTable.GetRowData(ctx)
		if err != nil {
			return from, to, err
		}
	} else {
		// If there is no |ToTable| use the |FromTable|'s schema to make the index.
		to, _ = durable.NewEmptyIndex(ctx, td.ToVRW, td.ToNodeStore, td.FromSch)
	}

	return from, to, nil
}

// SqlSchemaDiff returns a slice of DDL statements that will transform the schema in the from delta to the schema in
// the to delta.
func SqlSchemaDiff(fromTableInfo, toTableInfo *TableInfo, tds TableDeltaSummary) ([]string, error) {

	var ddlStatements []string
	if tds.IsDrop() {
		ddlStatements = append(ddlStatements, sqlfmt.DropTableStmt(tds.FromTableName))
	} else if tds.IsAdd() {
		ddlStatements = append(ddlStatements, toTableInfo.CreateStmt)
	} else {
		stmts, err := GetNonCreateNonDropTableSqlSchemaDiff(tds, fromTableInfo, toTableInfo)
		if err != nil {
			return nil, err
		}
		ddlStatements = append(ddlStatements, stmts...)
	}

	return ddlStatements, nil
}

// GetNonCreateNonDropTableSqlSchemaDiff returns any schema diff in SQL statements that is NEITHER 'CREATE TABLE' NOR 'DROP TABLE' statements.
func GetNonCreateNonDropTableSqlSchemaDiff(tds TableDeltaSummary, fromTableInfo, toTableInfo *TableInfo) ([]string, error) {
	if tds.IsAdd() || tds.IsDrop() {
		// use add and drop specific methods
		return nil, nil
	}

	var ddlStatements []string
	if tds.FromTableName != tds.ToTableName {
		ddlStatements = append(ddlStatements, sqlfmt.RenameTableStmt(tds.FromTableName, tds.ToTableName))
	}

	fromSch := fromTableInfo.Sch
	toSch := toTableInfo.Sch

	eq := schema.SchemasAreEqual(fromSch, toSch)
	if eq && !hasFkChanges(fromTableInfo, toTableInfo) {
		return ddlStatements, nil
	}

	ddlStatements = append(ddlStatements, tds.AlterStmts...)

	// Print changes between a primary key set change. It contains an ALTER TABLE DROP and an ALTER TABLE ADD
	if !schema.ColCollsAreEqual(fromSch.GetPKCols(), toSch.GetPKCols()) {
		ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropPks(tds.ToTableName))
		if toSch.GetPKCols().Size() > 0 {
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddPrimaryKeys(tds.ToTableName, toSch.GetPKCols().GetColumnNames()))
		}
	}

	for _, idxDiff := range DiffSchIndexes(fromSch, toSch) {
		switch idxDiff.DiffType {
		case SchDiffNone:
		case SchDiffAdded:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddIndexStmt(tds.ToTableName, idxDiff.To))
		case SchDiffRemoved:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropIndexStmt(tds.FromTableName, idxDiff.From))
		case SchDiffModified:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropIndexStmt(tds.FromTableName, idxDiff.From))
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddIndexStmt(tds.ToTableName, idxDiff.To))
		}
	}

	for _, fkDiff := range DiffForeignKeyInfos(fromTableInfo.Fks, toTableInfo.Fks) {
		switch fkDiff.DiffType {
		case SchDiffNone:
		case SchDiffAdded:
			to := fkDiff.To
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddForeignKeyStmtSimple(to.TableName, to.Name, to.ReferencedTableName, to.TableColumns, to.ReferencedTableColumns))
		case SchDiffRemoved:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From.TableName, fkDiff.From.Name))
		case SchDiffModified:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From.TableName, fkDiff.From.Name))
			to := fkDiff.To
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddForeignKeyStmtSimple(to.TableName, to.Name, to.ReferencedTableName, to.TableColumns, to.ReferencedTableColumns))
		}
	}

	return ddlStatements, nil
}

func hasFkChanges(fromTableInfo, toTableInfo *TableInfo) bool {
	fromFks := fromTableInfo.Fks
	toFks := toTableInfo.Fks

	if len(fromFks) != len(toFks) {
		return true
	}

	sort.Slice(fromFks, func(i, j int) bool {
		return fromFks[i].Name < fromFks[j].Name
	})
	sort.Slice(toFks, func(i, j int) bool {
		return toFks[i].Name < toFks[j].Name
	})

	for i := range fromTableInfo.Fks {
		ffk := fromTableInfo.Fks[i]
		tfk := toTableInfo.Fks[i]
		if !ffk.Equals(tfk) {
			return true
		}
	}

	return false
}

// GetDataDiffStatement returns any data diff in SQL statements for given table including INSERT, UPDATE and DELETE row statements.
func GetDataDiffStatement(tableName string, sch schema.Schema, row sql.Row, rowDiffType ChangeType, colDiffTypes []ChangeType) (string, error) {
	if len(row) != len(colDiffTypes) {
		return "", fmt.Errorf("expected the same size for columns and diff types, got %d and %d", len(row), len(colDiffTypes))
	}

	switch rowDiffType {
	case Added:
		return sqlfmt.SqlRowAsInsertStmt(row, tableName, sch)
	case Removed:
		return sqlfmt.SqlRowAsDeleteStmt(row, tableName, sch, 0)
	case ModifiedNew:
		updatedCols := set.NewEmptyStrSet()
		for i, diffType := range colDiffTypes {
			if diffType != None {
				updatedCols.Add(sch.GetAllCols().GetByIndex(i).Name)
			}
		}
		return sqlfmt.SqlRowAsUpdateStmt(row, tableName, sch, updatedCols)
	case ModifiedOld:
		// do nothing, we only issue UPDATE for ModifiedNew
		return "", nil
	default:
		return "", fmt.Errorf("unexpected row diff type: %v", rowDiffType)
	}
}

// GenerateCreateTableStatement returns CREATE TABLE statement for given table. This function was made to share the same
// 'create table' statement logic as GMS. We initially were running `SHOW CREATE TABLE` query to get the statement;
// however, it cannot be done for cases that need this statement in sql shell mode. Dolt uses its own Schema and
// Column and other object types which are not directly compatible with GMS, so we try to use as much shared logic
// as possible with GMS to get 'create table' statement in Dolt.
func GenerateCreateTableStatement(tblName string, sch schema.Schema, pkSchema sql.PrimaryKeySchema, fks []doltdb.ForeignKey, fksParentSch map[string]schema.Schema) (string, error) {
	sqlSch := pkSchema.Schema
	colStmts := make([]string, len(sqlSch))

	// Statement creation parts for each column
	for i, col := range sch.GetAllCols().GetColumns() {
		colStmts[i] = sqlfmt.GenerateCreateTableIndentedColumnDefinition(col)
	}

	primaryKeyCols := sch.GetPKCols().GetColumnNames()
	if len(primaryKeyCols) > 0 {
		primaryKey := sql.GenerateCreateTablePrimaryKeyDefinition(primaryKeyCols)
		colStmts = append(colStmts, primaryKey)
	}

	indexes := sch.Indexes().AllIndexes()
	for _, index := range indexes {
		// The primary key may or may not be declared as an index by the table. Don't print it twice if it's here.
		if isPrimaryKeyIndex(index, sch) {
			continue
		}
		colStmts = append(colStmts, sqlfmt.GenerateCreateTableIndexDefinition(index))
	}

	for _, fk := range fks {
		colStmts = append(colStmts, sqlfmt.GenerateCreateTableForeignKeyDefinition(fk, sch, fksParentSch[fk.ReferencedTableName]))
	}

	for _, check := range sch.Checks().AllChecks() {
		colStmts = append(colStmts, sqlfmt.GenerateCreateTableCheckConstraintClause(check))
	}

	coll := sql.CollationID(sch.GetCollation())
	createTableStmt := sql.GenerateCreateTableStatement(tblName, colStmts, coll.CharacterSet().Name(), coll.Name())
	return fmt.Sprintf("%s;", createTableStmt), nil
}

// isPrimaryKeyIndex returns whether the index given matches the table's primary key columns. Order is not considered.
func isPrimaryKeyIndex(index schema.Index, sch schema.Schema) bool {
	var pks = sch.GetPKCols().GetColumns()
	var pkMap = make(map[string]struct{})
	for _, c := range pks {
		pkMap[c.Name] = struct{}{}
	}

	indexCols := index.ColumnNames()
	if len(indexCols) != len(pks) {
		return false
	}

	for _, c := range index.ColumnNames() {
		if _, ok := pkMap[c]; !ok {
			return false
		}
	}

	return true
}
