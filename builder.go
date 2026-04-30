package main

import (
	"context"
	"embed"

	"github.com/codefly-dev/core/agents/services"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/shared"
	"github.com/codefly-dev/core/standards"
	"github.com/codefly-dev/core/templates"
)

type Builder struct {
	services.BuilderServer
	*Service
}

func NewBuilder() *Builder {
	return &Builder{
		Service: NewService(),
	}
}

func (s *Builder) Load(ctx context.Context, req *builderv0.LoadRequest) (*builderv0.LoadResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	err := s.Base.Load(ctx, req.Identity, s.Settings)
	if err != nil {
		return nil, err
	}

	requirements.Localize(s.Location)

	if req.CreationMode != nil {
		s.Builder.CreationMode = req.CreationMode
		s.Builder.GettingStarted, err = templates.ApplyTemplateFrom(ctx, shared.Embed(factoryFS), "templates/factory/GETTING_STARTED.md", s.Information)
		if err != nil {
			return s.Builder.LoadError(err)
		}
		return s.Builder.LoadResponse()
	}

	s.Endpoints, err = s.Base.Service.LoadEndpoints(ctx)
	if err != nil {
		return s.Builder.LoadError(err)
	}

	s.grpcEndpoint, err = resources.FindTCPEndpointWithName(ctx, "grpc", s.Endpoints)
	if err != nil {
		return s.Builder.LoadError(err)
	}

	s.httpEndpoint, err = resources.FindTCPEndpointWithName(ctx, "http", s.Endpoints)
	if err != nil {
		return s.Builder.LoadError(err)
	}

	return s.Builder.LoadResponse()
}

func (s *Builder) Init(ctx context.Context, req *builderv0.InitRequest) (*builderv0.InitResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	s.Builder.LogInitRequest(req)

	s.DependencyEndpoints = req.DependenciesEndpoints

	return s.Builder.InitResponse()
}

func (s *Builder) Update(ctx context.Context, req *builderv0.UpdateRequest) (*builderv0.UpdateResponse, error) {
	defer s.Wool.Catch()
	return &builderv0.UpdateResponse{}, nil
}

func (s *Builder) Sync(ctx context.Context, req *builderv0.SyncRequest) (*builderv0.SyncResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	return s.Builder.SyncResponse()
}

func (s *Builder) Build(ctx context.Context, req *builderv0.BuildRequest) (*builderv0.BuildResponse, error) {
	return s.Builder.BuildResponse()
}

func (s *Builder) Deploy(ctx context.Context, req *builderv0.DeploymentRequest) (*builderv0.DeploymentResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	var k *builderv0.KubernetesDeployment
	var err error
	if k, err = s.Builder.KubernetesDeploymentRequest(ctx, req); err != nil {
		return s.Builder.DeployError(err)
	}

	// Add dependency configurations
	err = s.EnvironmentVariables.AddConfigurations(ctx, req.DependenciesConfigurations...)
	if err != nil {
		return s.Builder.DeployError(err)
	}

	configs, err := s.EnvironmentVariables.Configurations()
	if err != nil {
		return s.Builder.DeployError(err)
	}
	cm, err := services.EnvsAsConfigMapData(configs...)
	if err != nil {
		return s.Builder.DeployError(err)
	}

	secrets, err := services.EnvsAsSecretData(s.EnvironmentVariables.Secrets()...)
	if err != nil {
		return s.Builder.DeployError(err)
	}

	params := services.DeploymentParameters{
		ConfigMap: cm,
		SecretMap: secrets,
	}

	err = s.Builder.KustomizeDeploy(ctx, req.Environment, k, deploymentFS, params)
	if err != nil {
		return s.Builder.DeployError(err)
	}

	return s.Builder.DeployResponse()
}

func (s *Builder) Create(ctx context.Context, req *builderv0.CreateRequest) (*builderv0.CreateResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	// Add the postgres service dependency -- temporal requires postgres for persistence
	s.Service.Service.ServiceDependencies = append(s.Service.Service.ServiceDependencies, &resources.ServiceDependency{
		Name:   "postgres",
		Module: s.Identity.Module,
	})

	err := s.CreateEndpoints(ctx)
	if err != nil {
		return s.Builder.CreateError(err)
	}

	return s.Builder.CreateResponse(ctx, s.Settings)
}

func (s *Builder) CreateEndpoints(ctx context.Context) error {
	tcp, err := resources.LoadTCPAPI(ctx)
	if err != nil {
		return s.Wool.Wrapf(err, "cannot load TCP api")
	}

	// gRPC endpoint (Temporal frontend service)
	grpcBase := s.Base.BaseEndpoint(standards.TCP)
	grpcBase.Name = "grpc"
	s.grpcEndpoint, err = resources.NewAPI(ctx, grpcBase, resources.ToTCPAPI(tcp))
	if err != nil {
		return s.Wool.Wrapf(err, "cannot create grpc tcp endpoint")
	}

	// HTTP endpoint (Temporal Web UI)
	httpBase := s.Base.BaseEndpoint(standards.TCP)
	httpBase.Name = "http"
	s.httpEndpoint, err = resources.NewAPI(ctx, httpBase, resources.ToTCPAPI(tcp))
	if err != nil {
		return s.Wool.Wrapf(err, "cannot create http tcp endpoint")
	}

	s.Endpoints = []*basev0.Endpoint{s.grpcEndpoint, s.httpEndpoint}
	return nil
}

//go:embed templates/factory
var factoryFS embed.FS

//go:embed templates/builder
var builderFS embed.FS

//go:embed templates/deployment
var deploymentFS embed.FS
