package main

import (
	"context"
	"errors"
	"fmt"

	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
)

type Credentials struct {
	EndpointURL string
	AccessKey   string
	SecretKey   string
	// for now -- this is the credentials
	instanceID string
	bindingID  string
}

type BrokerConfig struct {
	Password string
	Username string
}

// InstanceCreator upholds this contract
type InstanceCreator interface {
	Create(instanceID string) error
	Destroy(instanceID string) error
	Exists(instanceID string) (bool, error)
}

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
	instanceMgr     *InstanceMgr

	// Broker Config
	Config BrokerConfig
}

// Services Api
func (b *MinioServiceBroker) Services(ctx context.Context) []brokerapi.Service {
	b.log.Info("Building services catalog...")
	brokerID := "minio-broker-id" //uuid.NewV4().String()
	return []brokerapi.Service{
		brokerapi.Service{
			ID:            brokerID,
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

//Provision ...
func (b *MinioServiceBroker) Provision(ctx context.Context, instanceID string, serviceDetails brokerapi.ProvisionDetails, asyncAllowed bool) (spec brokerapi.ProvisionedServiceSpec, err error) {
	b.log.Info("Provisioning new instance ...")

	spec = brokerapi.ProvisionedServiceSpec{IsAsync: false}
	exists, err := b.instanceMgr.Exists(instanceID)
	if exists {
		return spec, errors.New("Instance already exists")
	}
	if serviceDetails.ServiceID != b.serviceID {
		return spec, fmt.Errorf("Service %s does not exist.Should be %s", serviceDetails.ServiceID, b.serviceID)
	}

	if serviceDetails.PlanID == "" {
		return spec, errors.New("planId required")
	}
	// Only default plan for now
	if serviceDetails.PlanID != b.planID {
		return spec, errors.New("plan id not recognized")
	}

	instance := b.instanceMgr.getInstanceByID(instanceID)
	if instance != nil {
		return spec, errors.New("instance already provisioned") // should return 409 here.
	}
	fmt.Println("is instance manager init", b.instanceMgr)
	err = b.instanceMgr.Create(instanceID)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, err
	}
	b.instanceMgr.instances[instanceID] = &InstanceInfo{instanceID: instanceID}
	spec.DashboardURL = "http://example-dashboard.example.com/9189kdfsk0vfnku" // Set to bucketpath here....
	return spec, nil
}

// Deprovision Api
func (b *MinioServiceBroker) Deprovision(ctx context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.DeprovisionServiceSpec, error) {
	b.log.Info("Deprovisioning new instance...")
	spec := brokerapi.DeprovisionServiceSpec{}

	// TODO: Need to ensure no binding exists - bindingInfo needs to change to have instanceID as state
	exists, _ := b.instanceMgr.Exists(instanceID)
	if exists {
		return spec, b.instanceMgr.Destroy(instanceID)
	}
	return spec, brokerapi.ErrInstanceDoesNotExist

}

// Bind Api
func (b *MinioServiceBroker) Bind(ctx context.Context, instanceID, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
	b.log.Debug("Binding service...", lager.Data{
		"binding-id":  bindingID,
		"instance-id": instanceID,
	})

	return brokerapi.Binding{}, brokerapi.ErrInstanceDoesNotExist
}

// Unbind Api
func (b *MinioServiceBroker) Unbind(ctx context.Context, instanceID, bindingID string, details brokerapi.UnbindDetails) error {
	b.log.Info("Unbinding service...", lager.Data{
		"binding-id":  bindingID,
		"instance-id": instanceID,
	})

	return brokerapi.ErrInstanceDoesNotExist
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
