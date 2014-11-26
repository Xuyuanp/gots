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

import "github.com/Xuyuanp/gots/protobuf"

type ColumnType int32

const (
	ColumnTypeINFMin ColumnType = iota
	ColumnTypeINFMax
	ColumnTypeInteger
	ColumnTypeString
	ColumnTypeBoolean
	ColumnTypeDouble
	ColumnTypeBinary
)

var ColumnTypeName = map[ColumnType]string{
	ColumnTypeINFMin:  "INF_MIN",
	ColumnTypeINFMax:  "INF_MAX",
	ColumnTypeInteger: "INTEGER",
	ColumnTypeString:  "STRING",
	ColumnTypeBoolean: "BOOLEAN",
	ColumnTypeDouble:  "DOUBLE",
	ColumnTypeBinary:  "BINARY",
}

var ColumnTypeValue = map[string]ColumnType{
	"INF_MIN": ColumnTypeINFMin,
	"INF_MAX": ColumnTypeINFMax,
	"INTEGER": ColumnTypeInteger,
	"STRING":  ColumnTypeString,
	"BOOLEAN": ColumnTypeBoolean,
	"DOUBLE":  ColumnTypeDouble,
	"BINARY":  ColumnTypeBinary,
}

func (t ColumnType) String() string {
	return ColumnTypeName[t]
}

type RowExistenceExpectation int32

const (
	RowExistenceExpectationIgnore RowExistenceExpectation = iota
	RowExistenceExpectationExpectExist
	RowExistenceExpectationExpectNotExist
)

var RowExistenceExpectationName = map[RowExistenceExpectation]string{
	RowExistenceExpectationIgnore:         "IGNORE",
	RowExistenceExpectationExpectExist:    "EXPECT_EXIST",
	RowExistenceExpectationExpectNotExist: "EXPECT_NOT_EXIST",
}

var RowExistenceExpectationValue = map[string]RowExistenceExpectation{
	"IGNORE":           RowExistenceExpectationIgnore,
	"EXPECT_EXIST":     RowExistenceExpectationExpectExist,
	"EXPECT_NOT_EXIST": RowExistenceExpectationExpectNotExist,
}

type OperationType int32

const (
	OperationTypePut    OperationType = 1
	OperationTypeDelete OperationType = 2
)

var OperationTypeName = map[OperationType]string{
	OperationTypePut:    "PUT",
	OperationTypeDelete: "DELETE",
}

var OperationTypeValue = map[string]OperationType{
	"PUT":    OperationTypePut,
	"DELETE": OperationTypeDelete,
}

type Direction int32

const (
	DirectionForward Direction = iota
	DirectionBackward
)

var DirectionName = map[Direction]string{
	DirectionForward:  "FORWARD",
	DirectionBackward: "BACKWARD",
}

var DirectionValue = map[string]Direction{
	"FORWARD":  DirectionForward,
	"BACKWARD": DirectionBackward,
}

type ColumnSchema struct {
	Name string
	Type ColumnType
}

func (cs *ColumnSchema) Parse(pbCS *protobuf.ColumnSchema) *ColumnSchema {
	cs.Name = pbCS.GetName()
	cs.Type = ColumnType(pbCS.GetType())
	return cs
}

type ColumnValue struct {
	Type     ColumnType
	VInt     int64
	VString  string
	VBoolean bool
	VDouble  float64
	VBinary  []byte
}

type Column struct {
	Name  string
	Value *ColumnValue
}

type Row struct {
	PrimaryKeyColumns []*Column
	AttributeColumns  []*Column
}

type TableMeta struct {
	TableName  string
	PrimaryKey []*ColumnSchema
}

func (tm *TableMeta) Parse(pbTM *protobuf.TableMeta) *TableMeta {
	tm.TableName = pbTM.GetTableName()
	tm.PrimaryKey = make([]*ColumnSchema, len(pbTM.GetPrimaryKey()))
	for i, pbCS := range pbTM.GetPrimaryKey() {
		tm.PrimaryKey[i] = (&ColumnSchema{}).Parse(pbCS)
	}
	return tm
}

type CapacityUnit struct {
	Read  int32
	Write int32
}

func (cu *CapacityUnit) Parse(pbCU *protobuf.CapacityUnit) *CapacityUnit {
	cu.Read = pbCU.GetRead()
	cu.Write = pbCU.GetWrite()
	return cu
}

func (cu *CapacityUnit) Unparse() *protobuf.CapacityUnit {
	read := new(int32)
	write := new(int32)
	*read = cu.Read
	*write = cu.Write
	return &protobuf.CapacityUnit{
		Read:  read,
		Write: write,
	}
}

type Condition struct {
	RowExistence RowExistenceExpectation
}

type ReservedThroughput struct {
	CapacityUnit *CapacityUnit
}

func (rt *ReservedThroughput) Unparse() *protobuf.ReservedThroughput {
	return &protobuf.ReservedThroughput{
		CapacityUnit: rt.CapacityUnit.Unparse(),
	}
}

type ReservedThoughputDetails struct {
	CapacityUnit         *CapacityUnit
	LastIncreaseTime     int64
	LastDescreaseTime    int64
	NumOfDescreasesToday int32
}

func (rtd *ReservedThoughputDetails) Parse(pbRTD *protobuf.ReservedThroughputDetails) *ReservedThoughputDetails {
	rtd.CapacityUnit = (&CapacityUnit{}).Parse(pbRTD.GetCapacityUnit())
	rtd.LastIncreaseTime = pbRTD.GetLastIncreaseTime()
	rtd.LastDescreaseTime = pbRTD.GetLastDecreaseTime()
	rtd.NumOfDescreasesToday = pbRTD.GetNumberOfDecreasesToday()
	return rtd
}

type ConsumedCapacity struct {
	CapacityUnit *CapacityUnit
}

type CreateTableResponse struct {
}

type UpdateTableResponse struct {
	ReservedThoughputDetails *ReservedThoughputDetails
}

func (utr *UpdateTableResponse) Parse(pbUTR *protobuf.UpdateTableResponse) *UpdateTableResponse {
	utr.ReservedThoughputDetails = (&ReservedThoughputDetails{}).Parse(pbUTR.GetReservedThroughputDetails())
	return utr
}

type DescribeTableResponse struct {
	TableMeta                *TableMeta
	ReservedThoughputDetails *ReservedThoughputDetails
}

type ListTableResponse struct {
	TableNames []string
}

type DeleteTableResponse struct {
}

type GetRowResponse struct {
	Consumed *ConsumedCapacity
	Row      *Row
}

type ColumnUpdate struct {
	Type  OperationType
	Name  string
	Value ColumnValue
}

type UpdateRowResponse struct {
	Consumed *ConsumedCapacity
}

type PutRowResponse struct {
	Consumed *ConsumedCapacity
}

type DeleteRowResponse struct {
	Consumed *ConsumedCapacity
}

type BatchGetRowItem struct {
	PrimaryKey []*Column
}

type BatchGetRowTableItem struct {
	TableName    string
	Rows         []*BatchGetRowItem
	ColumnsToGet []string
}

type RowInBatchGetRowResponse struct {
	IsOk     bool
	Error    error
	Consumed *ConsumedCapacity
	Row      *Row
}

type TableInBatchGetRowResponse struct {
	TableName string
	Rows      []*RowInBatchGetRowResponse
}

type BatchGetRowResponse struct {
	Tables []*TableInBatchGetRowResponse
}

type RowInBatchWriteRowResponse struct {
	IsOk     bool
	Error    error
	Consumed *ConsumedCapacity
}

type TableInBatchWriteRowResponse struct {
	TableName  string
	PutRows    []*RowInBatchWriteRowResponse
	UpdateRows []*RowInBatchWriteRowResponse
	DeleteRows []*RowInBatchWriteRowResponse
}

type BatchWriteRowResponse struct {
	Tables []*TableInBatchWriteRowResponse
}

type GetRangeResponse struct {
	Consumed            *ConsumedCapacity
	NextStartPrimaryKey []*Column
	Rows                []*Row
}
