/*
 * Copyright 2014 Xuyuan Pang <xuyuanp # gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gots

import (
	"github.com/Xuyuanp/gots/protobuf"
	"github.com/golang/protobuf/proto"
)

type Encoder struct {
	encoding string
}

func (e *Encoder) EncodeListTable() (proto.Message, error) {
	listTableRequest := &protobuf.ListTableRequest{}
	return listTableRequest, nil
}

func (e *Encoder) EncodeDescribeTable(name string) (proto.Message, error) {
	dtr := &protobuf.DescribeTableRequest{
		TableName: new(string),
	}
	*dtr.TableName = name
	return dtr, nil
}

func (e *Encoder) EncodeUpdateTable(name string, rt *ReservedThroughput) (proto.Message, error) {
	updateTableRequest := &protobuf.UpdateTableRequest{}
	updateTableRequest.TableName = &name
	updateTableRequest.ReservedThroughput = rt.Unparse()
	return updateTableRequest, nil
}

func (e *Encoder) EncodeCreateTable(name string, primaryKey []*ColumnSchema, rt *ReservedThroughput) (proto.Message, error) {
	tm := &TableMeta{
		TableName:  name,
		PrimaryKey: primaryKey,
	}
	createTableRequest := &protobuf.CreateTableRequest{
		TableMeta:          tm.Unparse(),
		ReservedThroughput: rt.Unparse(),
	}
	return createTableRequest, nil
}

func (e *Encoder) EncodeDeleteTable(name string) (proto.Message, error) {
	dtr := &protobuf.DeleteTableRequest{
		TableName: new(string),
	}
	*dtr.TableName = name
	return dtr, nil
}

func (e *Encoder) EncodeGetRow(name string, primaryKey map[string]interface{}, columnNames []string) (proto.Message, error) {
	pk := ColumnsFromMap(primaryKey)
	pbGRR := &protobuf.GetRowRequest{
		TableName:    new(string),
		PrimaryKey:   make([]*protobuf.Column, len(pk)),
		ColumnsToGet: columnNames,
	}
	*pbGRR.TableName = name
	for i, p := range pk {
		pbGRR.PrimaryKey[i] = p.Unparse()
	}
	return pbGRR, nil
}

func (e *Encoder) EncodePutRow(name string, condition *Condition, primaryKey map[string]interface{}, columns map[string]interface{}) (proto.Message, error) {
	pk := ColumnsFromMap(primaryKey)
	cols := ColumnsFromMap(columns)
	pbPR := &protobuf.PutRowRequest{
		TableName:        new(string),
		Condition:        condition.Unparse(),
		PrimaryKey:       make([]*protobuf.Column, len(pk)),
		AttributeColumns: make([]*protobuf.Column, len(cols)),
	}
	*pbPR.TableName = name
	for i, p := range pk {
		pbPR.PrimaryKey[i] = p.Unparse()
	}
	for i, col := range cols {
		pbPR.AttributeColumns[i] = col.Unparse()
	}
	return pbPR, nil
}

func (e *Encoder) EncodeDeleteRow(name string, condition *Condition, primaryKey map[string]interface{}) (proto.Message, error) {
	pbDRR := &protobuf.DeleteRowRequest{
		TableName:  new(string),
		Condition:  condition.Unparse(),
		PrimaryKey: make([]*protobuf.Column, len(primaryKey)),
	}
	*pbDRR.TableName = name
	for i, pk := range ColumnsFromMap(primaryKey) {
		pbDRR.PrimaryKey[i] = pk.Unparse()
	}
	return pbDRR, nil
}

func (e *Encoder) EncodeUpdateRow(name string, condition *Condition, primaryKey map[string]interface{}, columnsPut map[string]interface{}, columnsDelete []string) (proto.Message, error) {
	pbURR := &protobuf.UpdateRowRequest{
		TableName:        new(string),
		Condition:        condition.Unparse(),
		PrimaryKey:       make([]*protobuf.Column, len(primaryKey)),
		AttributeColumns: make([]*protobuf.ColumnUpdate, len(columnsPut)+len(columnsDelete)),
	}
	*pbURR.TableName = name
	for i, pk := range ColumnsFromMap(primaryKey) {
		pbURR.GetPrimaryKey()[i] = pk.Unparse()
	}
	index := 0
	for k, v := range columnsPut {
		pbCU := &protobuf.ColumnUpdate{
			Name:  new(string),
			Type:  new(protobuf.OperationType),
			Value: NewColumnValue(v).Unparse(),
		}
		*pbCU.Name = k
		*pbCU.Type = protobuf.OperationType_PUT
		pbURR.GetAttributeColumns()[index] = pbCU
		index++
	}
	for _, n := range columnsDelete {
		pbCU := &protobuf.ColumnUpdate{
			Name: new(string),
			Type: new(protobuf.OperationType),
		}
		*pbCU.Name = n
		*pbCU.Type = protobuf.OperationType_DELETE
		pbURR.GetAttributeColumns()[index] = pbCU
		index++
	}
	return pbURR, nil
}

func (e *Encoder) EncodeBatchGetRow(items map[string]BatchGetRowItem) (proto.Message, error) {
	pbBGRR := &protobuf.BatchGetRowRequest{
		Tables: make([]*protobuf.TableInBatchGetRowRequest, len(items)),
	}

	index := 0
	for name, bgri := range items {
		pbTRR := &protobuf.TableInBatchGetRowRequest{
			TableName:    new(string),
			Rows:         make([]*protobuf.RowInBatchGetRowRequest, len(bgri.PrimaryKeys)),
			ColumnsToGet: make([]string, len(bgri.ColumnNames)),
		}
		*pbTRR.TableName = name

		for i, pks := range bgri.PrimaryKeys {
			pbRGRR := &protobuf.RowInBatchGetRowRequest{
				PrimaryKey: make([]*protobuf.Column, len(pks)),
			}
			for j, pk := range ColumnsFromMap(pks) {
				pbRGRR.GetPrimaryKey()[j] = pk.Unparse()
			}
			pbTRR.GetRows()[i] = pbRGRR
		}

		for i, n := range bgri.ColumnNames {
			pbTRR.GetColumnsToGet()[i] = n
		}

		pbBGRR.GetTables()[index] = pbTRR
		index++
	}

	return pbBGRR, nil
}
