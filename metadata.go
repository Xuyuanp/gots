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

import "time"

type Type string

const (
	String  Type = "STRING"
	Integer      = "INTEGER"
	Double       = "DOUBLE"
	Boolean      = "BOOLEAN"
	Binary       = "BINARY"
)

type Direction string

const (
	Forward  Direction = "FORWARD"
	Backward           = "BACKWARD"
)

type PrimaryKey struct {
	Name  string
	Type  Type
	Value interface{}
}

type TableMeta struct {
	TableName        string
	PrimaryKeySchema []PrimaryKey
}

type CapacityUnit struct {
	Read  int
	Write int
}

type ReservedThoughput struct {
	CapacityUnit *CapacityUnit
}

type ReservedThoughputDetails struct {
	CapacityUnit         *CapacityUnit
	LastIncreaseTime     time.Time
	LastDescreaseTime    time.Time
	NumOfDescreasesToday int
}

type UpdateTableResponse struct {
	ReservedThoughputDetails *ReservedThoughputDetails
}

type DescribeTableResponse struct {
	TableMeta                *TableMeta
	ReservedThoughputDetails *ReservedThoughputDetails
}

type RowDataItem struct {
}

type Condition struct {
}

type PutRowItem struct {
}

type UpdateRowItem struct {
}

type DeleteRowItem struct {
}

type BatchWriteRowResponseItem struct {
}

type InfMin struct {
}

type InfMax struct {
}
