package main

import (
	"testing"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	agentv0 "github.com/codefly-dev/core/generated/go/codefly/services/agent/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedRuntimeSelectsNativeDatabaseConfiguration(t *testing.T) {
	configuration := func(runtime *basev0.RuntimeContext, connection string) *basev0.Configuration {
		return &basev0.Configuration{RuntimeContext: runtime, Infos: []*basev0.ConfigurationInformation{{
			Name: "postgres", ConfigurationValues: []*basev0.ConfigurationValue{{Key: "owner-connection", Value: connection, Secret: true}},
		}}}
	}
	container := configuration(resources.NewRuntimeContextContainer(), "postgresql://container:wrong@host.docker.internal:6432/postgres")
	native := configuration(resources.NewRuntimeContextNative(), "postgresql://native:correct@localhost:5433/postgres")
	for _, configs := range [][]*basev0.Configuration{{container, native}, {native, container}} {
		runtime := NewRuntime()
		require.NoError(t, runtime.resolvePostgresFromDependencies(configs))
		require.Equal(t, "localhost", runtime.postgresHost)
		require.Equal(t, 5433, runtime.postgresPort)
		require.Equal(t, "native", runtime.postgresUser)
		require.Equal(t, "correct", runtime.postgresPassword)
	}
	for _, configs := range [][]*basev0.Configuration{nil, {container}, {nil, container}} {
		require.Error(t, NewRuntime().resolvePostgresFromDependencies(configs), "a container-only projection must not become a host connection")
	}
	info, err := NewService().GetAgentInformation(t.Context(), &agentv0.AgentInformationRequest{})
	require.NoError(t, err)
	for _, backend := range info.GetSupportedBackends() {
		require.NotEqual(t, agentv0.Backend_DOCKER, backend.GetType(), "deployment's managed image is not an implemented runtime backend")
	}
}
