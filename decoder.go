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

type Decoder struct {
	encoding string
}

func (d *Decoder) DecodeListTable(data []byte) ([]string, error) {
	listTableResponse := protobuf.ListTableResponse{}
	err := proto.Unmarshal(data, &listTableResponse)
	if err != nil {
		return nil, err
	}
	return listTableResponse.GetTableNames(), nil
}

func (d *Decoder) DecodeDescribeTable(data []byte) (*TableMeta, *ReservedThoughputDetails, error) {
	pbDTR := protobuf.DescribeTableResponse{}
	err := proto.Unmarshal(data, &pbDTR)
	if err != nil {
		return nil, nil, err
	}
	tm := (&TableMeta{}).Parse(pbDTR.GetTableMeta())
	pbRTD := pbDTR.GetReservedThroughputDetails()
	rtd := (&ReservedThoughputDetails{}).Parse(pbRTD)
	return tm, rtd, nil
}

func (d *Decoder) DecodeUpdateTable(data []byte) (*UpdateTableResponse, error) {
	pbUTR := protobuf.UpdateTableResponse{}
	err := proto.Unmarshal(data, &pbUTR)
	if err != nil {
		return nil, err
	}
	upr := (&UpdateTableResponse{}).Parse(&pbUTR)
	return upr, nil
}

func (d *Decoder) DecodeCreateTable(data []byte) (*CreateTableResponse, error) {
	pbCTR := &protobuf.CreateTableResponse{}
	err := proto.Unmarshal(data, pbCTR)
	if err != nil {
		return nil, err
	}
	ctr := (&CreateTableResponse{}).Parse(pbCTR)
	return ctr, nil
}

func (d *Decoder) DecodeDeleteTable(data []byte) (*DeleteTableResponse, error) {
	pbDTR := &protobuf.DeleteTableResponse{}
	err := proto.Unmarshal(data, pbDTR)
	if err != nil {
		return nil, err
	}
	dtr := (&DeleteTableResponse{}).Parse(pbDTR)
	return dtr, nil
}

func (d *Decoder) DecodeGetRow(data []byte) (*GetRowResponse, error) {
	pbGRR := &protobuf.GetRowResponse{}
	err := proto.Unmarshal(data, pbGRR)
	if err != nil {
		return nil, err
	}
	grr := (&GetRowResponse{}).Parse(pbGRR)
	return grr, nil
}

func (d *Decoder) DecodePutRow(data []byte) (*PutRowResponse, error) {
	pbPRR := &protobuf.PutRowResponse{}
	err := proto.Unmarshal(data, pbPRR)
	if err != nil {
		return nil, err
	}
	prr := (&PutRowResponse{}).Parse(pbPRR)
	return prr, nil
}

func (d *Decoder) DecodeDeleteRow(data []byte) (*DeleteRowResponse, error) {
	pbDRR := &protobuf.DeleteRowResponse{}
	err := proto.Unmarshal(data, pbDRR)
	if err != nil {
		return nil, err
	}
	drr := (&DeleteRowResponse{}).Parse(pbDRR)
	return drr, nil
}
