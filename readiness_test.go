package main

import (
	"testing"

	agentv0 "github.com/codefly-dev/core/generated/go/codefly/services/agent/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/stretchr/testify/require"
)

func TestReadinessAdvertisesItsLiveStack(t *testing.T) {
	info, err := NewService().GetAgentInformation(t.Context(), &agentv0.AgentInformationRequest{})
	require.NoError(t, err)
	tests := info.GetValidation().GetTest()
	require.True(t, tests.GetSupported())
	require.Len(t, tests.GetSuites(), 1)
	suite := tests.GetSuites()[0]
	require.Equal(t, "readiness", suite.GetName())
	require.True(t, suite.GetDefaultSuite())
	require.Equal(t, agentv0.TestDependencyMode_TEST_DEPENDENCY_MODE_START_STACK, suite.GetDependencyMode())
}

func TestReadinessCannotPassWithoutRunningStack(t *testing.T) {
	response, err := NewRuntime().Test(t.Context(), &runtimev0.TestRequest{})
	require.NoError(t, err)
	require.Equal(t, runtimev0.TestStatus_ERROR, response.GetStatus().GetState())
	require.Contains(t, response.GetStatus().GetMessage(), "started service stack")
}

func TestReadinessRejectsUnsupportedRequests(t *testing.T) {
	for _, request := range []*runtimev0.TestRequest{
		{Suite: "missing"}, {Target: "./..."}, {Filters: []string{"test"}},
		{Timeout: "invalid"}, {Timeout: "0s"}, {Race: true}, {Coverage: true},
		{ExtraArgs: []string{"--flag"}}, {Formula: &runtimev0.TestFormula{}},
		{Selection: &runtimev0.TestSelection{Scope: &runtimev0.TestSelection_Suite{Suite: &runtimev0.TestSuiteSelection{Name: "missing"}}}},
	} {
		response, err := NewRuntime().Test(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, runtimev0.TestStatus_ERROR, response.GetStatus().GetState())
		require.NotContains(t, response.GetStatus().GetMessage(), "started service stack")
	}
}
