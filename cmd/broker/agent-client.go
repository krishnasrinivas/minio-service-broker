package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

type instanceInfo struct {
	AccessKey    string
	SecretKey    string
	Region       string
	DashboardURL string
}

type agentClient struct {
	u         url.URL
	accessKey string
	secretKey string
}

func (a agentClient) CreateInstance(instanceID string) error {
	fmt.Println("CREATE", instanceID)
	_, err := a.execute("PUT", instanceID)
	return err
}

func (a agentClient) InstanceInfo(instanceID string) (info instanceInfo, err error) {
	r, err := a.execute("GET", instanceID)
	if err != nil {
		return info, err
	}
	contents, err := ioutil.ReadAll(r)
	if err != nil {
		return info, err
	}
	err = json.Unmarshal(contents, &info)
	return info, err
}

func (a agentClient) DeleteInstance(instanceID string) error {
	_, err := a.execute("DELETE", instanceID)
	return err
}

func (a agentClient) execute(method string, instanceID string) (r io.ReadCloser, err error) {
	a.u.Path = fmt.Sprintf("/instances/%s", instanceID)
	req, err := http.NewRequest(method, a.u.String(), nil)
	if err != nil {
		fmt.Println(err)
		fmt.Println(a.u.String())
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}
	return resp.Body, nil
}
