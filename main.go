package main

import (
	"context"
	"embed"

	"github.com/codefly-dev/core/builders"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/templates"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/codefly-dev/core/agents"
	"github.com/codefly-dev/core/agents/services"
	agentv0 "github.com/codefly-dev/core/generated/go/codefly/services/agent/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/shared"
)

// Agent version
var agent = shared.Must(resources.LoadFromFs[resources.Agent](shared.Embed(infoFS)))

var requirements = builders.NewDependencies(agent.Name,
	builders.NewDependency("service.codefly.yaml"),
)

type Settings struct {
	Watch bool `yaml:"watch"`
}

type Service struct {
	*services.Base

	// Settings
	*Settings

	grpcEndpoint *basev0.Endpoint
	httpEndpoint *basev0.Endpoint
}

func (s *Service) GetAgentInformation(ctx context.Context, _ *agentv0.AgentInformationRequest) (*agentv0.AgentInformation, error) {
	defer s.Wool.Catch()

	readme, err := templates.ApplyTemplateFrom(ctx, shared.Embed(readmeFS), "templates/agent/README.md", s.Information)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &agentv0.AgentInformation{
		RuntimeRequirements: []*agentv0.Runtime{
			{Type: agentv0.Runtime_GO},
		},
		Capabilities: []*agentv0.Capability{
			{Type: agentv0.Capability_BUILDER},
			{Type: agentv0.Capability_RUNTIME},
		},
		Protocols: []*agentv0.Protocol{},
		ConfigurationDetails: []*agentv0.ConfigurationValueDetail{
			{
				Name: "connection", Description: "Temporal connection details",
				Fields: []*agentv0.ConfigurationValueInformation{
					{Name: "grpc", Description: "Temporal frontend gRPC endpoint"},
					{Name: "http", Description: "Temporal Web UI endpoint"},
				},
			},
		},
		ReadMe: readme,
	}, nil
}

func NewService() *Service {
	return &Service{
		Base:     services.NewServiceBase(context.Background(), agent.Of(resources.ServiceAgent)),
		Settings: &Settings{},
	}
}

func main() {
	svc := NewService()
	agents.Serve(agents.PluginRegistration{
		Agent:   svc,
		Runtime: NewRuntime(),
		Builder: NewBuilder(),
	})
}

//go:embed agent.codefly.yaml
var infoFS embed.FS

//go:embed templates/agent
var readmeFS embed.FS
