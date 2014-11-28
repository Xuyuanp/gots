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
