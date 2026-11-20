/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planbuilder

import (
	"slices"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func buildPlanForBypass(stmt sqlparser.Statement, _ *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	keyspace, err := vschema.SelectedKeyspace()
	if err != nil {
		return nil, err
	}
	switch dest := vschema.ShardDestination().(type) {
	case key.DestinationExactKeyRange:
		if _, ok := stmt.(*sqlparser.Insert); ok {
			return nil, vterrors.VT03023(vschema.TargetString())
		}
	case key.DestinationShard:
		if !vschema.IsShardRoutingEnabled() {
			break
		}
		shard := string(dest)
		targetKeyspace, err := GetShardRoute(vschema, keyspace.Name, shard)
		if err != nil {
			return nil, err
		}
		if targetKeyspace != nil {
			keyspace = targetKeyspace
		}
	}

	hints := &queryHints{}
	if comments, ok := stmt.(sqlparser.Commented); ok {
		if qh := getHints(comments.GetParsedComments()); qh != nil {
			hints = qh
		}
	}

	// Bypass plans previously carried no tables, so planStartsImplicitTx
	// could never see the tables a shard-targeted SELECT touches and an
	// autocommit=0 SELECT ... FOR UPDATE never started the implicit
	// transaction. Record the tables from the statement, like the normal
	// planner does, so the implicit-transaction decision matches.
	// Collect before RemoveSpecificKeyspace strips the keyspace qualifier.
	tables := bypassTables(stmt, keyspace.Name)

	sqlparser.RemoveSpecificKeyspace(stmt, keyspace.Name)

	send := &engine.Send{
		Keyspace:             keyspace,
		TargetDestination:    vschema.ShardDestination(),
		Query:                sqlparser.String(stmt),
		IsDML:                sqlparser.IsDMLStatement(stmt),
		SingleShardOnly:      false,
		MultishardAutocommit: hints.multiShardAutocommit,
		QueryTimeout:         hints.queryTimeout,
	}
	return newPlanResult(send, tables...), nil
}

// bypassTables returns the tables the statement touches, as keyspace.table
// strings, mirroring what the normal planner records in TablesUsed:
// unqualified tables are qualified with the selected keyspace, CTE names
// are left out, and the result is sorted and deduplicated.
func bypassTables(stmt sqlparser.Statement, keyspace string) []string {
	cteNames := map[string]struct{}{}
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if with, ok := node.(*sqlparser.With); ok {
			for _, cte := range with.CTEs {
				cteNames[cte.ID.String()] = struct{}{}
			}
		}
		return true, nil
	}, stmt)

	var tables []string
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		tbl, ok := node.(sqlparser.TableName)
		if !ok {
			return true, nil
		}
		// An empty table name is not a real table: column qualifiers
		// are stored as TableName nodes (unqualified columns carry the
		// zero value), and the parser represents FROM dual the same way.
		if tbl.Name.IsEmpty() {
			return true, nil
		}
		if tbl.Qualifier.IsEmpty() {
			if _, isCTE := cteNames[tbl.Name.String()]; isCTE {
				return true, nil
			}
			tables = append(tables, singleTable(keyspace, tbl.Name.String()))
		} else {
			tables = append(tables, singleTable(tbl.Qualifier.String(), tbl.Name.String()))
		}
		return true, nil
	}, stmt)

	slices.Sort(tables)
	return slices.Compact(tables)
}

func GetShardRoute(vschema plancontext.VSchema, keyspace, shard string) (*vindexes.Keyspace, error) {
	targetKeyspaceName, err := vschema.FindRoutedShard(keyspace, shard)
	if err != nil {
		return nil, err
	}
	return vschema.FindKeyspace(targetKeyspaceName)
}
