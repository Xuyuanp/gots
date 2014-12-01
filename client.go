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
	// DefaultEncoding 默认编码
	DefaultEncoding = "utf8"
	// DefaultSocketTimeout 默认socket链接过期时间
	DefaultSocketTimeout = 50
	// DefaultMaxConnection 默认链接池最大链接数
	DefaultMaxConnection = 50
)

// Client 实现了OTS服务的所有接口。用户可以通过NewClient方法创建Client实例
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

// NewClient 方法返回一个Client实例
// 示例:
//
// import "github.com/Xuyuanp/gots
// client := ots.NewClient("your_instance_endpoint", "your_user_id", "your_user_key", "your_instance_name")
// client.Init()
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

// Init 方法初始化Client，在创建Client实例之后，调用任何访问接口之前必须调用此方法
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

func (c *Client) vist(apiName string, message proto.Message) (data []byte, err error) {
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
	for k := range headers {
		lk := strings.ToLower(k)
		newHeaders[lk] = headers.Get(k)
	}
	return data, c.protocol.ParseResponse(apiName, response.StatusCode, newHeaders, data)
}

// ListTable 方法用于获取所有表名。
// 示例:
//
// names, err := client.ListTable()
func (c *Client) ListTable() (names []string, err error) {
	message, err := c.encoder.EncodeListTable()
	if err != nil {
		return nil, err
	}
	data, err := c.vist("ListTable", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeListTable(data)
}

// CreateTable 方法用于创建新表。
// name: 表名
// primaryKey: 主键列表
// rt: 预留读写吞吐量
// 示例：
//
// primaryKey := []*gots.ColumnSchema{
//      &gots.ColumnSchema{
//              Name: "gid",
//              Type: gots.ColumnTypeInteger,
//      },
//      &gots.ColumnSchema{
//              Name: "uid",
//              Type: gots.ColumnTypeInteger,
//      },
// }
// rt := &gots.ReservedThroughput{
//      CapacityUnit: &gots.CapacityUnit{
//              Read:  100,
//              Write: 100,
//      },
// }
//
// resp, err := client.CreateTable("sample_table", primaryKey, rt)
func (c *Client) CreateTable(name string, primaryKey []*ColumnSchema, rt *ReservedThroughput) (*CreateTableResponse, error) {
	message, err := c.encoder.EncodeCreateTable(name, primaryKey, rt)
	if err != nil {
		return nil, err
	}
	data, err := c.vist("CreateTable", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeCreateTable(data)
}

// DeleteTable 方法用于删除表
// name: 表名
// 示例:
//
// resp, err := client.DeleteTable("sample_table")
func (c *Client) DeleteTable(name string) (*DeleteTableResponse, error) {
	message, err := c.encoder.EncodeDeleteTable(name)
	if err != nil {
		return nil, err
	}
	data, err := c.vist("DeleteTable", message)
	if err != nil {
		return nil, err
	}
	return c.decoder.DecodeDeleteTable(data)
}

// DescribeTable 方法用于获取表描述信息
// name: 表名
// 示例:
//
// resp, err := client.DescribeTable("sample_table")
func (c *Client) DescribeTable(name string) (*TableMeta, *ReservedThoughputDetails, error) {
	message, err := c.encoder.EncodeDescribeTable(name)
	if err != nil {
		return nil, nil, err
	}
	data, err := c.vist("DescribeTable", message)
	if err != nil {
		return nil, nil, err
	}
	return c.decoder.DecodeDescribeTable(data)
}

// UpdateTable 跟新表属性，目前只支持修改预留读写吞吐量
// name: 表名
// reservedThroughput: 预留读写吞吐量
// 示例:
//
// rt := &gots.ReservedThroughput{
//      CapacityUnit: &gots.CapacityUnit{
//              Read:  150,
//              Write: 150,
//      },
// }
// resp, err := client.UpdateTable("sample_table", rt)
func (c *Client) UpdateTable(name string, reservedThroughput *ReservedThroughput) (*UpdateTableResponse, error) {
	message, err := c.encoder.EncodeUpdateTable(name, reservedThroughput)
	if err != nil {
		return nil, err
	}
	data, err := c.vist("UpdateTable", message)
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
	data, err := c.vist("GetRow", message)
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
	data, err := c.vist("PutRow", message)
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
	data, err := c.vist("UpdateRow", message)
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
	data, err := c.vist("DeleteRow", message)
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
	data, err := c.vist("BatchGetRow", message)
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
