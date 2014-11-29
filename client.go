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
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/golang/protobuf/proto"
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
	c.protocol = &Protocol{
		EndPoint:     c.EndPoint,
		AccessID:     c.AccessID,
		AccessKey:    c.AccessKey,
		InstanceName: c.InstanceName,
	}
	c.encoder = &Encoder{encoding: c.Encoding}
	c.decoder = &Decoder{encoding: c.Encoding}
	return nil
}

func (c *Client) Visit(apiName string, message proto.Message) (data []byte, err error) {
	body, err := proto.Marshal(message)
	if err != nil {
		return nil, &OTSClientError{Message: fmt.Sprintf("%s Marshal protocol buffer failed", err.Error())}
	}
	if c.Debug && c.Logger != nil {
		c.Logger.Printf(`Request: %s data: %s`, apiName, message.String())
	}
	req, err := c.protocol.MakeRequest(apiName, body)
	if err != nil {
		return nil, err
	}
	// TODO: Use connection pool
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &OTSClientError{Message: fmt.Sprintf("%s Send request failed", err.Error())}
	}
	defer response.Body.Close()
	data, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, &OTSClientError{Message: "Read data faild in response"}
	}

	headers := response.Header
	newHeaders := make(map[string]string, len(headers))
	for k, _ := range headers {
		lk := strings.ToLower(k)
		newHeaders[lk] = headers.Get(k)
	}
	return data, c.protocol.ParseResponse(apiName, response.StatusCode, newHeaders, data)
}

func (c *Client) ListTable() (names []string, err error) {
	message, err := c.encoder.EncodeListTable()
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("ListTable", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeListTable(data)
}

func (c *Client) CreateTable(name string, primaryKey []*ColumnSchema, rt *ReservedThroughput) (*CreateTableResponse, error) {
	message, err := c.encoder.EncodeCreateTable(name, primaryKey, rt)
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("CreateTable", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeCreateTable(data)
}

func (c *Client) DeleteTable(name string) (*DeleteTableResponse, error) {
	message, err := c.encoder.EncodeDeleteTable(name)
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("DeleteTable", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeDeleteTable(data)
}

func (c *Client) DescribeTable(name string) (*TableMeta, *ReservedThoughputDetails, error) {
	message, err := c.encoder.EncodeDescribeTable(name)
	if err != nil {
		return nil, nil, err
	}
	data, err := c.Visit("DescribeTable", message)
	if err != nil {
		return nil, nil, err
	}
	return c.decoder.DecodeDescribeTable(data)
}

func (c *Client) UpdateTable(name string, reservedThroughput *ReservedThroughput) (*UpdateTableResponse, error) {
	message, err := c.encoder.EncodeUpdateTable(name, reservedThroughput)
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("UpdateTable", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeUpdateTable(data)
}

func (c *Client) GetRow(name string, primaryKey map[string]interface{}, columnNames []string) (*GetRowResponse, error) {
	message, err := c.encoder.EncodeGetRow(name, primaryKey, columnNames)
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("GetRow", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeGetRow(data)
}

func (c *Client) PutRow(name string, condition *Condition, primaryKey map[string]interface{}, columns map[string]interface{}) (response *PutRowResponse, err error) {
	message, err := c.encoder.EncodePutRow(name, condition, primaryKey, columns)
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("PutRow", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodePutRow(data)
}

func (c *Client) UpdateRow(name string, condition *Condition, primaryKey map[string]interface{}, columnsPut map[string]interface{}, columnsDelete []string) (*UpdateRowResponse, error) {
	message, err := c.encoder.EncodeUpdateRow(name, condition, primaryKey, columnsPut, columnsDelete)
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("UpdateRow", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeUpdateRow(data)
}

func (c *Client) DeleteRow(name string, condition *Condition, primaryKey map[string]interface{}) (*DeleteRowResponse, error) {
	message, err := c.encoder.EncodeDeleteRow(name, condition, primaryKey)
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("DeleteRow", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeDeleteRow(data)
}

func (c *Client) BatchGetRow(items map[string]BatchGetRowItem) (*BatchGetRowResponse, error) {
	message, err := c.encoder.EncodeBatchGetRow(items)
	if err != nil {
		return nil, err
	}
	data, err := c.Visit("BatchGetRow", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeBatchGetRow(data)
}

// func (c *Client) BatchWriteRow(batchList []map[string]interface{}) (items []map[string]interface{}, err error) {
// 	return nil, nil
// }
//
// func (c *Client) GetRange(name string, direction Direction, incStartPrimaryKey *PrimaryKey, excEndPrimaryKey *PrimaryKey,
// 	colums []string, limit int) (consumed *CapacityUnit, next []*PrimaryKey, rows []interface{}, err error) {
// 	return nil, nil, nil, nil
// }
//
// func (c *Client) XGetRange(name string, direction Direction, incStartPrimaryKey *PrimaryKey, excEndPrimaryKey *PrimaryKey,
// 	consumedCounter *CapacityUnit, colums []string, limit int) (consumed *CapacityUnit, next []*PrimaryKey, rows []interface{}, err error) {
// 	return nil, nil, nil, nil
// }
