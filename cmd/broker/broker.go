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
	"context"

	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
)

// Broker
type MinioServiceBroker struct {
	log lager.Logger

	// Serviceplan Info
	serviceName        string
	serviceID          string
	serviceDescription string
	serviceTags        []string

	// plan-specific customization
	planName        string
	planDescription string
	planID          string
	bindablePlan    bool

	agent agentClient
}

// Services Api
func (b *MinioServiceBroker) Services(ctx context.Context) []brokerapi.Service {
	b.log.Info("Building services catalog...")

	return []brokerapi.Service{
		brokerapi.Service{
			ID:            b.serviceID,
			Name:          b.serviceName,
			Description:   b.serviceDescription,
			Tags:          []string{},
			PlanUpdatable: b.bindablePlan,
			Plans: []brokerapi.ServicePlan{
				brokerapi.ServicePlan{
					ID:          b.planID,
					Name:        b.planName,
					Description: b.planDescription,
					Free:        brokerapi.FreeValue(true),
				},
			},
		},
	}
}

//Provision Api
func (b *MinioServiceBroker) Provision(ctx context.Context, instanceID string, serviceDetails brokerapi.ProvisionDetails, asyncAllowed bool) (spec brokerapi.ProvisionedServiceSpec, err error) {
	b.log.Info("Provisioning new instance ...")

	if err := b.agent.CreateInstance(instanceID); err != nil {
		return spec, err
	}

	info, err := b.agent.InstanceInfo(instanceID)
	if err != nil {
		return spec, err
	}

	return brokerapi.ProvisionedServiceSpec{
		IsAsync:      false,
		DashboardURL: info.DashboardURL,
	}, nil
}

// Deprovision Api
func (b *MinioServiceBroker) Deprovision(ctx context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.DeprovisionServiceSpec, error) {
	b.log.Info("Deprovisioning new instance...")
	spec := brokerapi.DeprovisionServiceSpec{}

	err := b.agent.DeleteInstance(instanceID)
	return spec, err
}

// Bind Api
func (b *MinioServiceBroker) Bind(ctx context.Context, instanceID, bindingID string, details brokerapi.BindDetails) (binding brokerapi.Binding, err error) {
	b.log.Debug("Binding service...", lager.Data{
		"binding-id":  bindingID,
		"instance-id": instanceID,
	})
	info, err := b.agent.InstanceInfo(instanceID)
	if err != nil {
		return binding, err
	}
	credential := make(map[string]string)
	credential["AccessKey"] = info.AccessKey
	credential["SecretKey"] = info.SecretKey
	credential["Region"] = info.Region
	return brokerapi.Binding{Credentials: credential}, brokerapi.ErrInstanceDoesNotExist
}

// Unbind Api
func (b *MinioServiceBroker) Unbind(ctx context.Context, instanceID, bindingID string, details brokerapi.UnbindDetails) error {
	b.log.Info("Unbinding service...", lager.Data{
		"binding-id":  bindingID,
		"instance-id": instanceID,
	})

	return nil
}

// LastOperation ...
func (b *MinioServiceBroker) LastOperation(ctx context.Context, instanceID, operationData string) (brokerapi.LastOperation, error) {
	b.log.Info("Last operation", lager.Data{
		"instance-id": instanceID,
	})
	return brokerapi.LastOperation{}, nil
}

// Update implements brokerapi.ServiceBroker
func (b *MinioServiceBroker) Update(ctx context.Context, instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	b.log.Debug("Updating service...", lager.Data{
		"instance-id": instanceID,
	})
	return brokerapi.UpdateServiceSpec{}, nil
}
