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
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Xuyuanp/gots/protobuf"
	"github.com/golang/protobuf/proto"
)

var AllowedAPI = map[string]bool{
	"CreateTable":   true,
	"ListTable":     true,
	"DeleteTable":   true,
	"DescribeTable": true,
	"UpdateTable":   true,
	"GetRow":        true,
	"PutRow":        true,
	"UpdateRow":     true,
	"DeleteRow":     true,
	"BatchGetRow":   true,
	"BatchWriteRow": true,
	"GetRange":      true,
}

const (
	HeaderOTSDate         = "x-ots-date"
	HeaderOTSAPIVersion   = "x-ots-apiversion"
	HeaderOTSAccessID     = "x-ots-accessid"
	HeaderOTSInstanceName = "x-ots-instancename"
	HeaderOTSContentMd5   = "x-ots-contentmd5"
	HeaderOTSSignature    = "x-ots-signature"
	HeaderOTSRequestID    = "x-ots-requestid"
	HeaderOTSContentType  = "x-ots-contenttype"

	DefaultAPIVersion = "2014-08-08"
)

type Protocol struct {
	EndPoint     string
	AccessID     string
	AccessKey    string
	InstanceName string
}

func (p *Protocol) headerString(headers map[string]string) string {
	otsHeaders := make([]string, len(headers))
	index := 0
	for k, v := range headers {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-ots") {
			otsHeaders[index] = fmt.Sprintf("%s:%s", k, strings.TrimSpace(v))
			index++
		}
	}
	otsHeaders = otsHeaders[:index]
	sort.Strings(otsHeaders)
	headerString := strings.Join(otsHeaders, "\n")
	return headerString
}

func (p *Protocol) makeSignature(query string, headers map[string]string) string {
	stringToSign := query + "\n" + "PORT" + "\n\n" + p.headerString(headers) + "\n"
	h := hmac.New(sha1.New, []byte(p.AccessKey))
	signature := base64.StdEncoding.EncodeToString(h.Sum([]byte(stringToSign)))
	return signature
}

func (p *Protocol) makeHeaders(query string, body []byte) map[string]string {
	m := md5.Sum(body)
	basemd5 := base64.StdEncoding.EncodeToString(m[:])
	date := time.Now().Format(time.RFC822)

	headers := map[string]string{
		HeaderOTSDate:         date,
		HeaderOTSAPIVersion:   DefaultAPIVersion,
		HeaderOTSInstanceName: p.InstanceName,
		HeaderOTSContentMd5:   basemd5,
		HeaderOTSAccessID:     p.AccessID,
	}

	signature := p.makeSignature(query, headers)
	headers[HeaderOTSSignature] = signature

	return headers
}

func (p *Protocol) MakeRequest(apiName string, body []byte) (*http.Request, error) {
	if _, ok := AllowedAPI[apiName]; !ok {
		return nil, &OTSClientError{Message: fmt.Sprintf("API %s is not supported", apiName)}
	}
	query := "/" + apiName
	headers := p.makeHeaders(query, body)

	rd := bytes.NewReader(body)
	request, err := http.NewRequest("POST", p.EndPoint, rd)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		request.Header.Set(k, v)
	}
	return request, nil
}

func (p *Protocol) checkHeaders(headers http.Header, body []byte) error {
	headerNames := []string{
		HeaderOTSDate,
		HeaderOTSContentMd5,
		HeaderOTSRequestID,
		HeaderOTSContentType,
	}
	for _, name := range headerNames {
		if _, ok := headers[name]; !ok {
			return &OTSClientError{Message: fmt.Sprintf(`"%s" is missing in response header`, name)}
		}
	}

	m := md5.Sum(body)
	basemd5 := base64.StdEncoding.EncodeToString(m[:])

	if headers.Get(HeaderOTSContentMd5) != basemd5 {
		return &OTSClientError{Message: "MD5 mismatch in response"}
	}

	serverTime, err := time.Parse(time.RFC822, headers.Get(HeaderOTSDate))
	if err != nil {
		return &OTSClientError{Message: "Invalid date format in response"}
	}

	now := time.Now()
	dur := now.Sub(serverTime)
	if dur > 15*time.Minute {
		return &OTSClientError{Message: "The difference between date in response and system time is more than 15 minutes"}
	}

	return nil
}

func (p *Protocol) checkAuthorization(apiName string, headers http.Header) error {
	return nil
}

func (p *Protocol) ParseResponse(apiName string, response *http.Response) (data []byte, err error) {
	if _, ok := AllowedAPI[apiName]; !ok {
		return nil, &OTSClientError{Message: fmt.Sprintf("API %s is not supported", apiName)}
	}

	defer response.Body.Close()
	data, err = ioutil.ReadAll(response.Body)

	headers := response.Header
	if err = p.checkHeaders(headers, data); err != nil {
		return nil, err
	}

	status := response.StatusCode
	if status != 403 {
		if err = p.checkAuthorization(apiName, headers); err != nil {
			return nil, &OTSClientError{Message: fmt.Sprintf("%s HTTP status: %d", err.Error(), status)}
		}
	}

	if status >= 200 && status < 300 {
		return data, nil
	}

	requestID := headers.Get(HeaderOTSRequestID)
	pbError := &protobuf.Error{}
	if err = proto.Unmarshal(data, pbError); err != nil {
		return nil, &OTSClientError{Status: status, Message: fmt.Sprintf("HTTP status: %s", status)}
	}

	errorCode := pbError.GetCode()
	errorMessage := pbError.GetMessage()

	if status == 403 && errorCode != "OTSAuthFailed" {
		authError := p.checkAuthorization(apiName, headers)
		if authError != nil {
			return nil, &OTSClientError{Status: status, Message: fmt.Sprintf("%s HTTP status: %d", authError.Error(), status)}
		}
	}

	return nil, &OTSServiceError{
		Status:    status,
		Code:      errorCode,
		Message:   errorMessage,
		RequestID: requestID,
	}
}
