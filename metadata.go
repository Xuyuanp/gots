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

func (t ColumnType) Unparse() *protobuf.ColumnType {
	pbCT := new(protobuf.ColumnType)
	*pbCT = protobuf.ColumnType(t)
	return pbCT
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

type Error struct {
	Code    string
	Message string
}

func (e *Error) Parse(pbE *protobuf.Error) *Error {
	e.Code = pbE.GetCode()
	e.Message = pbE.GetMessage()
	return e
}

func (e *Error) Unparse() *protobuf.Error {
	pbE := &protobuf.Error{
		Code:    new(string),
		Message: new(string),
	}
	*pbE.Code = e.Code
	*pbE.Message = e.Message
	return pbE
}

type ColumnSchema struct {
	Name string
	Type ColumnType
}

func (cs *ColumnSchema) Unparse() *protobuf.ColumnSchema {
	name := new(string)
	*name = cs.Name
	ctype := new(protobuf.ColumnType)
	*ctype = protobuf.ColumnType(cs.Type)
	return &protobuf.ColumnSchema{
		Name: name,
		Type: ctype,
	}
}

func (cs *ColumnSchema) Parse(pbCS *protobuf.ColumnSchema) *ColumnSchema {
	cs.Name = pbCS.GetName()
	cs.Type = ColumnType(pbCS.GetType())
	return cs
}

type ColumnValue struct {
	Type    ColumnType
	VInt    int64
	VString string
	VBool   bool
	VDouble float64
	VBinary []byte
}

func NewColumnValue(v interface{}) *ColumnValue {
	cv := &ColumnValue{}
	switch v.(type) {
	case int64:
		cv.Type = ColumnTypeInteger
		cv.VInt = v.(int64)
	case int:
		cv.Type = ColumnTypeInteger
		cv.VInt = int64(v.(int))
	case int32:
		cv.Type = ColumnTypeInteger
		cv.VInt = int64(v.(int32))
	case string:
		cv.Type = ColumnTypeString
		cv.VString = v.(string)
	case float64:
		cv.Type = ColumnTypeDouble
		cv.VDouble = v.(float64)
	case float32:
		cv.Type = ColumnTypeDouble
		cv.VDouble = float64(v.(float32))
	case bool:
		cv.Type = ColumnTypeBoolean
		cv.VBool = v.(bool)
	case []byte:
		cv.Type = ColumnTypeBinary
		cv.VBinary = v.([]byte)
	default:
		cv = nil
	}
	return cv
}

func (cv *ColumnValue) Parse(pbCV *protobuf.ColumnValue) *ColumnValue {
	cv.Type = ColumnType(pbCV.GetType())
	cv.VInt = pbCV.GetVInt()
	cv.VString = pbCV.GetVString()
	cv.VBool = pbCV.GetVBool()
	cv.VDouble = pbCV.GetVDouble()
	cv.VBinary = pbCV.GetVBinary()
	return cv
}

func (cv *ColumnValue) Unparse() *protobuf.ColumnValue {
	pbCV := &protobuf.ColumnValue{
		Type:    cv.Type.Unparse(),
		VInt:    new(int64),
		VString: new(string),
		VBool:   new(bool),
		VDouble: new(float64),
		VBinary: make([]byte, len(cv.VBinary)),
	}
	*pbCV.VInt = cv.VInt
	*pbCV.VString = cv.VString
	*pbCV.VBool = cv.VBool
	*pbCV.VDouble = cv.VDouble
	copy(pbCV.VBinary, cv.VBinary)
	return pbCV
}

func (cv *ColumnValue) Value() interface{} {
	switch cv.Type {
	case ColumnTypeString:
		return cv.VString
	case ColumnTypeInteger:
		return cv.VInt
	case ColumnTypeDouble:
		return cv.VDouble
	case ColumnTypeBoolean:
		return cv.VBool
	case ColumnTypeBinary:
		return cv.VBinary
	}
	return nil
}

type Column struct {
	Name  string
	Value *ColumnValue
}

func ColumnsFromMap(colMap map[string]interface{}) []*Column {
	columns := make([]*Column, len(colMap))
	index := 0
	for k, v := range colMap {
		col := &Column{
			Name:  k,
			Value: NewColumnValue(v),
		}
		columns[index] = col
		index++
	}
	return columns
}

func (col *Column) Parse(pbCol *protobuf.Column) *Column {
	col.Name = pbCol.GetName()
	col.Value = (&ColumnValue{}).Parse(pbCol.GetValue())
	return col
}

func (col *Column) Unparse() *protobuf.Column {
	pbCol := &protobuf.Column{
		Name:  new(string),
		Value: col.Value.Unparse(),
	}
	*pbCol.Name = col.Name
	return pbCol
}

type Row struct {
	PrimaryKeyColumns []*Column
	AttributeColumns  []*Column
}

func (r *Row) Parse(pbRow *protobuf.Row) *Row {
	r.PrimaryKeyColumns = make([]*Column, len(pbRow.GetPrimaryKeyColumns()))
	r.AttributeColumns = make([]*Column, len(pbRow.GetAttributeColumns()))
	for i, col := range pbRow.GetPrimaryKeyColumns() {
		r.PrimaryKeyColumns[i] = (&Column{}).Parse(col)
	}
	for i, col := range pbRow.GetAttributeColumns() {
		r.AttributeColumns[i] = (&Column{}).Parse(col)
	}
	return r
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

func (tm *TableMeta) Unparse() *protobuf.TableMeta {
	name := new(string)
	*name = tm.TableName
	schemas := make([]*protobuf.ColumnSchema, len(tm.PrimaryKey))
	for i, s := range tm.PrimaryKey {
		schemas[i] = s.Unparse()
	}
	return &protobuf.TableMeta{
		TableName:  name,
		PrimaryKey: schemas,
	}
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

func (c *Condition) Unparse() *protobuf.Condition {
	pbC := &protobuf.Condition{
		RowExistence: new(protobuf.RowExistenceExpectation),
	}
	*pbC.RowExistence = protobuf.RowExistenceExpectation(c.RowExistence)
	return pbC
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

func (cc *ConsumedCapacity) Parse(pbCC *protobuf.ConsumedCapacity) *ConsumedCapacity {
	cc.CapacityUnit = (&CapacityUnit{}).Parse(pbCC.GetCapacityUnit())
	return cc
}

type CreateTableResponse struct {
}

func (ctr *CreateTableResponse) Parse(pbCTR *protobuf.CreateTableResponse) *CreateTableResponse {
	return ctr
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

func (dtr *DeleteTableResponse) Parse(pbDTR *protobuf.DeleteTableResponse) *DeleteTableResponse {
	return dtr
}

type GetRowResponse struct {
	Consumed *ConsumedCapacity
	Row      *Row
}

func (grr *GetRowResponse) Parse(pbGRR *protobuf.GetRowResponse) *GetRowResponse {
	grr.Consumed = (&ConsumedCapacity{}).Parse(pbGRR.GetConsumed())
	grr.Row = (&Row{}).Parse(pbGRR.GetRow())
	return grr
}

type ColumnUpdate struct {
	Type  OperationType
	Name  string
	Value ColumnValue
}

type UpdateRowResponse struct {
	Consumed *ConsumedCapacity
}

func (urr *UpdateRowResponse) Parse(pbURR *protobuf.UpdateRowResponse) *UpdateRowResponse {
	urr.Consumed = (&ConsumedCapacity{}).Parse(pbURR.GetConsumed())
	return urr
}

type PutRowResponse struct {
	Consumed *ConsumedCapacity
}

func (prr *PutRowResponse) Parse(pbPRR *protobuf.PutRowResponse) *PutRowResponse {
	prr.Consumed = (&ConsumedCapacity{}).Parse(pbPRR.GetConsumed())
	return prr
}

type DeleteRowResponse struct {
	Consumed *ConsumedCapacity
}

func (drr *DeleteRowResponse) Parse(pbDRR *protobuf.DeleteRowResponse) *DeleteRowResponse {
	drr.Consumed = (&ConsumedCapacity{}).Parse(pbDRR.GetConsumed())
	return drr
}

type BatchGetRowItem struct {
	PrimaryKeys []map[string]interface{}
	ColumnNames []string
}

type RowInBatchGetRowResponse struct {
	IsOk     bool
	Error    *Error
	Consumed *ConsumedCapacity
	Row      *Row
}

func (rgrr *RowInBatchGetRowResponse) Parse(pbRGRR *protobuf.RowInBatchGetRowResponse) *RowInBatchGetRowResponse {
	rgrr.IsOk = pbRGRR.GetIsOk()
	rgrr.Consumed = (&ConsumedCapacity{}).Parse(pbRGRR.GetConsumed())
	rgrr.Row = (&Row{}).Parse(pbRGRR.GetRow())
	rgrr.Error = (&Error{}).Parse(pbRGRR.GetError())
	return rgrr
}

type TableInBatchGetRowResponse struct {
	TableName string
	Rows      []*RowInBatchGetRowResponse
}

func (tgrr *TableInBatchGetRowResponse) Parse(pbTGRR *protobuf.TableInBatchGetRowResponse) *TableInBatchGetRowResponse {
	tgrr.TableName = pbTGRR.GetTableName()
	tgrr.Rows = make([]*RowInBatchGetRowResponse, len(pbTGRR.GetRows()))
	for i, row := range pbTGRR.GetRows() {
		tgrr.Rows[i] = (&RowInBatchGetRowResponse{}).Parse(row)
	}
	return tgrr
}

type BatchGetRowResponse struct {
	Tables []*TableInBatchGetRowResponse
}

func (bgrr *BatchGetRowResponse) Parse(pbBGRR *protobuf.BatchGetRowResponse) *BatchGetRowResponse {
	bgrr.Tables = make([]*TableInBatchGetRowResponse, len(pbBGRR.GetTables()))
	for i, t := range pbBGRR.GetTables() {
		bgrr.Tables[i] = (&TableInBatchGetRowResponse{}).Parse(t)
	}
	return bgrr
}

type RowInBatchWriteRowResponse struct {
	IsOk     bool
	Error    *Error
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
