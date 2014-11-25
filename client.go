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
	"log"
	"net/http"
)

const (
	DefaultEncoding      = "utf8"
	DefaultSocketTimeout = 50
	DefaultMaxConnection = 50
)

type Client struct {
	EndPoint      string
	AccessID      string
	AccessKey     string
	InstanceName  string
	Encoding      string
	SocketTimeout float32
	MaxConnection int
	Debug         bool
	Logger        *log.Logger
	protocol      *Protocol
	encoder       *Encoder
	decoder       *Decoder
}

func NewClient(endPoint, accessID, accessKey, instanceName string) *Client {
	return &Client{
		EndPoint:      endPoint,
		AccessID:      accessID,
		AccessKey:     accessKey,
		InstanceName:  instanceName,
		Encoding:      DefaultEncoding,
		SocketTimeout: DefaultSocketTimeout,
		MaxConnection: DefaultMaxConnection,
		Debug:         false,
	}
}

func (c *Client) Init() error {
	return nil
}

func (c *Client) Visit(apiName string, body []byte) (data []byte, err error) {
	req, err := c.protocol.MakeRequest(apiName, body)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	return c.protocol.ParseResponse(apiName, response)
}

func (c *Client) ListTable() (names []string, err error) {
	body, err := c.encoder.EncodeListTable()
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("ListTable", body)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeListTable(data)
}

func (c *Client) CreateTable(meta *TableMeta, reservedThoughput *ReservedThoughput) error {
	return nil
}

func (c *Client) DeleteTable(name string) error {
	return nil
}

func (c *Client) DescribeTable(name string) (describeTableResponse *DescribeTableResponse, err error) {
	return nil, nil
}

func (c *Client) UpdateTable(name string, reservedThoughput *ReservedThoughput) error {
	return nil
}

func (c *Client) GetRow(name string, primaryKeys []PrimaryKey, columns []string) (consumed *CapacityUnit, primaryCols map[string]interface{}, attributeCols map[string]interface{}, err error) {
	return nil, nil, nil, nil
}

func (c *Client) PutRow(name string, condition *Condition, primaryKeys []PrimaryKey, columns map[string]interface{}) (consumed *CapacityUnit, err error) {
	return nil, nil
}

func (c *Client) UpdateRow(name string, condition *Condition, primaryKeys []PrimaryKey, columns map[string]interface{}) (consumed *CapacityUnit, err error) {
	return nil, nil
}

func (c *Client) DeleteRow(name string, condition *Condition, primaryKeys []PrimaryKey) (consumed *CapacityUnit, err error) {
	return nil, nil
}

func (c *Client) BatchGetRow([]map[string]interface{}) (items [][]RowDataItem, err error) {
	return nil, nil
}

func (c *Client) BatchWriteRow(batchList []map[string]interface{}) (items []map[string]interface{}, err error) {
	return nil, nil
}

func (c *Client) GetRange(name string, direction Direction, incStartPrimaryKey *PrimaryKey, excEndPrimaryKey *PrimaryKey, colums []string, limit int) (consumed *CapacityUnit, next []*PrimaryKey, rows []interface{}, err error) {
	return nil, nil, nil, nil
}

func (c *Client) XGetRange(name string, direction Direction, incStartPrimaryKey *PrimaryKey, excEndPrimaryKey *PrimaryKey, consumedCounter *CapacityUnit, colums []string, limit int) (consumed *CapacityUnit, next []*PrimaryKey, rows []interface{}, err error) {
	return nil, nil, nil, nil
}
