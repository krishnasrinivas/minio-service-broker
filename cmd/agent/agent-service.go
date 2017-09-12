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
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/gorilla/mux"
	"github.com/minio/minio-service-broker/auth"
)

const (
	// Path to minio binary on CF
	globalMinioPath = "/var/vcap/packages/minio/minio"
	// Root dir of minio-agent
	globalRootDir = "/var/vcap/store/minio-agent"
	// Path to dir where instance state is maintained.
	globalInstancesDir = globalRootDir + "/instances"
	// Path to dir where minio server instances are maintained.
	globalMinioDir = globalRootDir + "/minio"
	// Base port number for instances.
	globalInstanceBasePort = 9001
)

// Port number at which agent listens in.
var globalAgentPort = "9000"

// Max number of instances allowed - artificial restriction for now.
var globalMaxInstances = 100

// instance config
type instanceConfig struct {
	Port int
}

// minio access credentials
type minioCredential struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

// minio server config
type minioConfig struct {
	Credential minioCredential `json:"credential"`
	Region     string          `json:"region"`
}

// return path to directory where minio instance is stored.
func getMinioDir(instanceID string) string {
	return fmt.Sprintf("%s/%s", globalMinioDir, instanceID)
}

// return path to minio config folder for this instance
func getMinioConfigDir(instanceID string) string {
	return fmt.Sprintf("%s/config", getMinioDir(instanceID))
}

// Get full path to the minio config file for this instance.
func getMinioConfigFile(instanceID string) string {
	return fmt.Sprintf("%s/config.json", getMinioConfigDir(instanceID))
}

// Get full path of directory where the minio instance's data is stored.
func getMinioDataDir(instanceID string) string {
	return fmt.Sprintf("%s/data", getMinioDir(instanceID))
}

// Get full path to server logs directory for a minio instance
func getMinioLogsDir(instanceID string) string {
	return fmt.Sprintf("%s/logs", getMinioDir(instanceID))
}

// Get full path to server log file for a minio instance
func getMinioLogFile(instanceID string) string {
	return fmt.Sprintf("%s/minio.log", getMinioLogsDir(instanceID))
}

// Get config file for this instance. This is used by the agent to manage instances.
func getInstanceConfigFile(instanceID string) string {
	return fmt.Sprintf("%s/%s.json", globalInstancesDir, instanceID)
}

// Get the instance config for this instanceID.
func getInstanceConfig(instanceID string) (config instanceConfig, err error) {
	configPath := getInstanceConfigFile(instanceID)
	contents, err := ioutil.ReadFile(configPath)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(contents, &config)
	return config, err
}

// Assign an unused port for an instance starting at the globalInstanceBasePort
func getFreePort() (port int, err error) {
	entries, err := ioutil.ReadDir(globalInstancesDir)
	if err != nil {
		return port, err
	}
	portAllocated := make(map[int]bool)
	var config instanceConfig
	for _, entry := range entries {
		instanceID := strings.TrimSuffix(entry.Name(), ".json")
		config, err = getInstanceConfig(instanceID)
		if err != nil {
			return port, err
		}
		portAllocated[config.Port] = true
	}
	for port = globalInstanceBasePort; port < globalInstanceBasePort+globalMaxInstances; port++ {
		if !portAllocated[port] {
			return port, nil
		}
	}
	return -1, errors.New("maximum instances already allocated")
}

// get the minio server config file for this instance of minio.
func getMinioConfig(instanceID string) (minioConfig, error) {
	var config minioConfig
	minioConfigFile := getMinioConfigFile(instanceID)
	contents, err := ioutil.ReadFile(minioConfigFile)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(contents, &config)
	return config, err
}

// MinioServiceAgent holds the map of service name to status
type MinioServiceAgent struct {
	log   lager.Logger
	creds auth.CredentialsV4
	sync.Mutex
}

// CreateInstanceHandler creates an instance of minio server
func (agent *MinioServiceAgent) CreateInstanceHandler(w http.ResponseWriter, r *http.Request) {
	if !agent.creds.IsSigned(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	agent.Lock()
	defer agent.Unlock()

	vars := mux.Vars(r)
	instanceID := vars["instance-id"]
	agent.log.Info("create instance::" + r.RequestURI + "::" + instanceID)

	_, err := os.Stat(getInstanceConfigFile(instanceID))
	if err == nil {
		agent.log.Error(fmt.Sprintf("instance %s already exists", instanceID), err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Spawn minio instance
	port, err := getFreePort()
	if err != nil {
		agent.log.Error("getFreePort() error", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	err = agent.createInstance(instanceID, port)
	if err != nil {
		agent.log.Error("Failed to provision instance", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	// Wait for minio process to write the config files
	time.Sleep(time.Second * 4)
}

// DeleteInstanceHandler kills this instance of minio server and deletes its data from disk.
func (agent *MinioServiceAgent) DeleteInstanceHandler(w http.ResponseWriter, r *http.Request) {
	if !agent.creds.IsSigned(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	agent.Lock()
	defer agent.Unlock()

	vars := mux.Vars(r)
	instanceID := vars["instance-id"]

	_, err := os.Stat(getInstanceConfigFile(instanceID))
	if err != nil {
		agent.log.Error(fmt.Sprintf("Instance %s not found", instanceID), err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	minioConf, err := getMinioConfig(instanceID)
	if err != nil {
		agent.log.Error(fmt.Sprintf("Instance %s", instanceID), err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	accessKey := minioConf.Credential.AccessKey
	secretKey := minioConf.Credential.SecretKey

	instanceConf, err := getInstanceConfig(instanceID)
	if err != nil {
		agent.log.Error(fmt.Sprintf("Instance %s", instanceID), err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	port := instanceConf.Port
	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:%d/?service", port), nil)
	if err != nil {
		agent.log.Error(fmt.Sprintf("Instance %s", instanceID), err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req.Header.Set("x-minio-operation", "stop")
	auth.CredentialsV4{accessKey, secretKey, "us-east-1"}.Sign(req)

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		agent.log.Error(fmt.Sprintf("Delete instance %s", instanceID), err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = os.RemoveAll(getMinioDir(instanceID))
	if err != nil {
		agent.log.Error(fmt.Sprintf("Delete instance %s", instanceID), err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = os.RemoveAll(getInstanceConfigFile(instanceID))
	if err != nil {
		agent.log.Error(fmt.Sprintf("Delete instance %s", instanceID), err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// GetInstanceHandler returns access credentials and instance URL
func (agent *MinioServiceAgent) GetInstanceHandler(w http.ResponseWriter, r *http.Request) {
	if !agent.creds.IsSigned(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	agent.Lock()
	defer agent.Unlock()

	agent.log.Info("Entering GetInstanceHandler handler ...")
	vars := mux.Vars(r)
	instanceID := vars["instance-id"]

	instanceConf, err := getInstanceConfig(instanceID)
	if err != nil {
		agent.log.Error(fmt.Sprintf("Instance %s", instanceID), err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	minioConf, err := getMinioConfig(instanceID)
	if err != nil {
		agent.log.Error(fmt.Sprintf("Instance %s", instanceID), err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	info := struct {
		AccessKey    string
		SecretKey    string
		Region       string
		DashboardURL string
	}{
		minioConf.Credential.AccessKey,
		minioConf.Credential.SecretKey,
		minioConf.Region,
		fmt.Sprintf("https://%d.minio.%s", instanceConf.Port, os.Getenv("CF_DOMAIN")),
	}
	contents, err := json.Marshal(info)
	if err != nil {
		agent.log.Error(fmt.Sprintf("Instance %s", instanceID), err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Write(contents)
}

// actual doer to start a minio instance.
func (agent *MinioServiceAgent) createInstance(instanceID string, port int) error {
	minioConfigDir := getMinioConfigDir(instanceID)
	minioDataDir := getMinioDataDir(instanceID)
	minioLogsDir := getMinioLogsDir(instanceID)
	minioLogFile := getMinioLogFile(instanceID)
	instanceConfigFile := getInstanceConfigFile(instanceID)

	err := os.MkdirAll(minioConfigDir, 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(minioDataDir, 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(minioLogsDir, 0755)
	if err != nil {
		return err
	}

	logFile, err := os.Create(minioLogFile)
	if err != nil {
		return err
	}

	cmd := exec.Command(globalMinioPath, "server", "--address", ":"+strconv.Itoa(port), "--config-dir", minioConfigDir, minioDataDir)
	cmd.Stderr = logFile
	cmd.Stdout = logFile
	cmd.Env = append(
		os.Environ(),
		"MINIO_ACCESS_KEY=minio",
		"MINIO_SECRET_KEY=minio123",
	)
	err = cmd.Start() // will wait for command to return
	if err != nil {
		return err
	}
	go func() {
		cmd.Wait()
	}()
	configContents, err := json.Marshal(instanceConfig{port})
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(instanceConfigFile, configContents, 0644)
	return err
}

//Init all the instances of minio on their respective ports(as per the
// config saved in the globalInstancesDir)
func (agent *MinioServiceAgent) Init() error {
	if _, err := os.Stat(globalInstancesDir); os.IsNotExist(err) {
		return nil
	}
	entries, err := ioutil.ReadDir(globalInstancesDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		instanceID := strings.TrimSuffix(entry.Name(), ".json")
		conf, err := getInstanceConfig(instanceID)
		if err != nil {
			return err
		}
		err = agent.createInstance(instanceID, conf.Port)
		if err != nil {
			return err
		}
	}
	return nil
}
