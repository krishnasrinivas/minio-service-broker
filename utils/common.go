/*
* Minio Client (C) 2017 Minio, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */
package utils

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// WriteResponse marshalls json into an object and writes server response
func WriteResponse(w http.ResponseWriter, code int, object interface{}) {
	data, err := json.Marshal(object)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(code)
	fmt.Fprintf(w, string(data))
}

// GetEndpointURL - construct a new endpoint.
func GetEndpointURL(endpoint string, secure bool) (*url.URL, error) {
	if strings.Contains(endpoint, ":") {
		_, _, err := net.SplitHostPort(endpoint)
		if err != nil {
			return nil, err
		}
	}
	// If secure is false, use 'http' scheme.
	scheme := "https"
	if !secure {
		scheme = "http"
	}

	// Construct a secured endpoint URL.
	endpointURLStr := scheme + "://" + endpoint
	endpointURL, err := url.Parse(endpointURLStr)
	if err != nil {
		return nil, err
	}

	return endpointURL, nil
}
