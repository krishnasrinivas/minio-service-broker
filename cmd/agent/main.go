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
	"fmt"
	"net/http"
	"os"

	"code.cloudfoundry.org/lager"
	"github.com/gorilla/mux"
	"github.com/minio/minio-service-broker/auth"
)

var log = lager.NewLogger("minio-serviceagent")

func main() {
	fmt.Println("agent listening...")

	// Create logger
	log.RegisterSink(lager.NewWriterSink(os.Stderr, lager.DEBUG))
	log.RegisterSink(lager.NewWriterSink(os.Stderr, lager.INFO))
	// Setup the agent
	agent := &MinioServiceAgent{
		log: log,
		creds: auth.CredentialsV4{
			os.Getenv("AGENT_IDENTITY"),
			os.Getenv("AGENT_PASSWORD"),
			"us-east-1",
		},
	}

	// Bring up all instances
	if err := agent.Init(); err != nil {
		log.Fatal("Unable to Init()", err)
		return
	}

	if err := os.MkdirAll(globalMinioDir, 0755); err != nil {
		log.Fatal("Unable to create "+globalMinioDir, err)
		return
	}
	if err := os.MkdirAll(globalInstancesDir, 0755); err != nil {
		log.Fatal("Unable to create "+globalInstancesDir, err)
		return
	}
	if os.Getenv("MINIO_AGENT_PORT") != "" {
		globalAgentPort = os.Getenv("MINIO_AGENT_PORT")
	}

	router := mux.NewRouter()
	router.Methods("PUT").Path("/instances/{instance-id}").HandlerFunc(agent.CreateInstanceHandler)
	router.Methods("DELETE").Path("/instances/{instance-id}").HandlerFunc(agent.DeleteInstanceHandler)
	router.Methods("GET").Path("/instances/{instance-id}").HandlerFunc(agent.GetInstanceHandler)

	if err := http.ListenAndServe(":"+globalAgentPort, router); err != nil {
		log.Fatal("Unable to listen on port "+globalAgentPort, err)
	}
}
