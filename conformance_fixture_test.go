package main

import (
	"testing"

	"github.com/codefly-dev/core/resources"
)

func TestConformanceFixtureLoadsCandidateWithPostgresDependency(t *testing.T) {
	workspace, err := resources.LoadWorkspaceFromDir(t.Context(), "testdata/workspace")
	if err != nil {
		t.Fatal(err)
	}
	services, err := workspace.LoadServices(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var candidate, database *resources.Service
	for _, service := range services {
		switch service.Name {
		case "temporal":
			candidate = service
		case "postgres":
			database = service
		}
	}
	if candidate == nil || database == nil || candidate.Agent.Version != "latest" || database.Agent.Version != "0.0.134" {
		t.Fatal("fixture must qualify the candidate against its declared published database agent")
	}
	if len(candidate.ServiceDependencies) != 1 || candidate.ServiceDependencies[0].Name != database.Name {
		t.Fatal("Temporal conformance must include its actual persistence dependency")
	}
}
