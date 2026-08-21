package main

import (
	"context"
	"embed"
	"fmt"
	"strings"

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
//
// SKIP_DB_CREATE is true because rendered Postgres runtime roles are
// NOCREATEDB: auto-setup's create step would fail against them, so the database
// is provisioned out of band and auto-setup only applies the schema.
func temporalServerConfig() []*resources.EnvironmentVariable {
	return []*resources.EnvironmentVariable{
		resources.Env("DB", "postgres12_pgx"),
		resources.Env("SKIP_DB_CREATE", true),
		resources.Env("DBNAME", "temporal"),
		resources.Env("VISIBILITY_DBNAME", "temporal_visibility"),
		resources.Env("DEFAULT_NAMESPACE", "default"),
	}
}

// requiredAutoSetupEnv lists the environment variables the stock auto-setup
// entrypoint needs to reach its PostgreSQL database. The entrypoint has no
// fallback for these: absent them the container crash-loops on DB discovery
// instead of failing the render, so the deployment asserts their presence.
func requiredAutoSetupEnv() []string {
	return []string{"DB", "DBNAME", "VISIBILITY_DBNAME", "POSTGRES_SEEDS", "POSTGRES_USER", "POSTGRES_PWD"}
}

// missingAutoSetupEnv returns the required auto-setup variables absent from the
// set of keys the render will inject (ConfigMap data, Secret data, and external
// Secret references combined).
func missingAutoSetupEnv(present map[string]struct{}) []string {
	var missing []string
	for _, key := range requiredAutoSetupEnv() {
		if _, ok := present[key]; !ok {
			missing = append(missing, key)
		}
	}
	return missing
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
			// here: POSTGRES_SEEDS, POSTGRES_USER and POSTGRES_PWD are consumed
			// from externally-managed Secret references carried in the request.
			if !services.IsRestrictedOutputProfile(deployment.Profile) {
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
			}
			present := make(map[string]struct{})
			for _, env := range deployment.ConfigMap {
				present[env.Key] = struct{}{}
			}
			for _, env := range deployment.Secrets {
				present[env.Key] = struct{}{}
			}
			for name := range deployment.Kubernetes.GetSecretReferences() {
				present[name] = struct{}{}
			}
			if missing := missingAutoSetupEnv(present); len(missing) > 0 {
				return fmt.Errorf("auto-setup entrypoint requires environment not injected by the render: %s", strings.Join(missing, ", "))
			}
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
