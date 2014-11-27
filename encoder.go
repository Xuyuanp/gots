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

func (e *Encoder) EncodeCreateTable(tm *TableMeta, rt *ReservedThroughput) (proto.Message, error) {
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

func (e *Encoder) EncodePutRow(name string, condition *Condition, primaryKey []*Column, columns []*Column) (proto.Message, error) {
	pbPR := &protobuf.PutRowRequest{
		TableName:        new(string),
		Condition:        condition.Unparse(),
		PrimaryKey:       make([]*protobuf.Column, len(primaryKey)),
		AttributeColumns: make([]*protobuf.Column, len(columns)),
	}
	*pbPR.TableName = name
	for i, pk := range primaryKey {
		pbPR.PrimaryKey[i] = pk.Unparse()
	}
	for i, col := range columns {
		pbPR.AttributeColumns[i] = col.Unparse()
	}
	return pbPR, nil
}
