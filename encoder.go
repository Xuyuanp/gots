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
}

func (e *Encoder) EncodeListTable() ([]byte, error) {
	listTableRequest := &protobuf.ListTableRequest{}
	data, err := proto.Marshal(listTableRequest)
	return data, err
}
