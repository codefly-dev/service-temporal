package main

import (
	"context"
	"embed"
	"fmt"

	"github.com/codefly-dev/core/agents/services"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/standards"
)

type Builder struct {
	*services.DefaultBuilder
	*Service
}

// temporalServerConfig is the profile-independent, non-secret Temporal server
// configuration shared by every deployment. Keeping it in one place lets the
// manifest-guard render exercise exactly what the production Deploy emits.
func temporalServerConfig() []*resources.EnvironmentVariable {
	return []*resources.EnvironmentVariable{
		resources.Env("DB", "postgres12_pgx"),
		resources.Env("SKIP_DB_CREATE", false),
		resources.Env("DBNAME", "temporal"),
		resources.Env("VISIBILITY_DBNAME", "temporal_visibility"),
		resources.Env("DEFAULT_NAMESPACE", "default"),
	}
}

func NewBuilder() *Builder {
	service := NewService()
	return &Builder{
		DefaultBuilder: services.NewDefaultBuilder(service.Builder),
		Service:        service,
	}
}

func (s *Builder) Load(ctx context.Context, req *builderv0.LoadRequest) (*builderv0.LoadResponse, error) {
	defer s.Wool.Catch()

	return s.Builder.LoadService(ctx, req, services.BuilderLoad{
		Settings:         s.Settings,
		Requirements:     requirements,
		FactoryTemplates: factoryFS,
		ResolveEndpoints: func(ctx context.Context, endpoints []*basev0.Endpoint) error {
			grpcEndpoint, err := resources.FindTCPEndpointWithName(ctx, "grpc", endpoints)
			if err != nil {
				return err
			}
			httpEndpoint, err := resources.FindTCPEndpointWithName(ctx, "http", endpoints)
			if err != nil {
				return err
			}
			s.grpcEndpoint = grpcEndpoint
			s.httpEndpoint = httpEndpoint
			return nil
		},
	})
}

func (s *Builder) Init(ctx context.Context, req *builderv0.InitRequest) (*builderv0.InitResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	s.Builder.LogInitRequest(req)

	s.DependencyEndpoints = req.DependenciesEndpoints

	return s.Builder.InitResponse()
}

func (s *Builder) Audit(ctx context.Context, req *builderv0.AuditRequest) (*builderv0.AuditResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	return s.Builder.AuditContainer(ctx, req, image.FullName())
}

func (s *Builder) SBOM(ctx context.Context, _ *builderv0.SBOMRequest) (*builderv0.SBOMResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	return s.Builder.SBOMContainer(ctx, image.FullName())
}

func (s *Builder) Deploy(ctx context.Context, req *builderv0.DeploymentRequest) (*builderv0.DeploymentResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	s.Base.SetDockerImage(image)

	return s.Builder.DeployKustomize(ctx, req, services.KustomizeDeployment{
		EnvironmentVariables: s.EnvironmentVariables,
		Templates:            deploymentFS,
		Prepare: func(_ context.Context, deployment *services.KustomizeDeploymentContext) error {
			deployment.AddConfigMap(temporalServerConfig()...)
			// The restricted profile forbids receiving or serializing secret
			// values, so the Postgres owner-connection (a secret) is not parsed
			// here: POSTGRES_USER and POSTGRES_PWD are consumed from
			// externally-managed Secret references carried in the request.
			if services.IsRestrictedOutputProfile(deployment.Profile) {
				return nil
			}
			var connection string
			for _, configuration := range req.GetDependenciesConfigurations() {
				value, err := resources.GetConfigurationValue(ctx, configuration, "postgres", "owner-connection")
				if err != nil {
					return err
				}
				if value != "" {
					connection = value
					break
				}
			}
			if connection == "" {
				return fmt.Errorf("temporal requires the postgres owner-connection migration capability")
			}
			host, port, user, password, err := parsePostgresConnectionString(connection)
			if err != nil {
				return err
			}
			deployment.AddConfigMap(
				resources.Env("POSTGRES_SEEDS", host),
				resources.Env("DB_PORT", port),
			)
			deployment.AddSecrets(
				resources.Env("POSTGRES_USER", user),
				resources.Env("POSTGRES_PWD", password),
			)
			return nil
		},
	})
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

//go:embed templates/deployment
var deploymentFS embed.FS
