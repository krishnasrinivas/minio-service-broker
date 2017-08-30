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
	"os"

	"github.com/minio/minio-service-broker/auth"
)

type Credentials struct {
	EndpointURL string
	AccessKey   string
	SecretKey   string
	Region      string
}

// Config - TODO needs to change
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Secure    bool
}

// AccessCredentials container for access and secret keys.
type AccessCredentials struct {
	AccessKey     string `json:"accessKey,omitempty"`
	SecretKey     string `json:"secretKey,omitempty"`
	secretKeyHash []byte
}

type serverConfig struct {
	Version string `json:"version"`

	// S3 API configuration.
	Credential AccessCredentials `json:"credential"`
	Region     string            `json:"region"`
}

// GetCredentialsFromConfig fetches access key and secret key from config file
func GetCredentialsFromConfig(configFilePath string) (auth.CredentialsV4, error) {

	srvCfg := &serverConfig{}
	configFile, err := os.Open(configFilePath)
	defer configFile.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonParser := json.NewDecoder(configFile)

	jsonParser.Decode(&srvCfg)
	return auth.CredentialsV4{srvCfg.Credential.AccessKey, srvCfg.Credential.SecretKey, srvCfg.Region}, nil
}
