package main

import (
	"context"
	"os"
	"testing"

	"github.com/codefly-dev/core/agents/services"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/wool"
	"github.com/stretchr/testify/require"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
)

// TestManifestGuardRender renders the plugin's Kubernetes manifest tree through
// the production Kustomize path into CODEFLY_MANIFEST_DESTINATION. The shared
// manifest-guard workflow (codefly-dev/.github) invokes it twice under the
// restricted/portable profile and requires the two trees to be byte-for-byte
// identical. It skips when the destination is unset so plain `go test ./...`
// stays usable.
func TestManifestGuardRender(t *testing.T) {
	destination := os.Getenv("CODEFLY_MANIFEST_DESTINATION")
	if destination == "" {
		t.Skip("CODEFLY_MANIFEST_DESTINATION is unset; skipping manifest guard render")
	}
	environment := os.Getenv("CODEFLY_MANIFEST_ENVIRONMENT")
	namespace := os.Getenv("CODEFLY_MANIFEST_NAMESPACE")
	profileName := os.Getenv("CODEFLY_MANIFEST_PROFILE")
	require.NotEmpty(t, environment, "CODEFLY_MANIFEST_ENVIRONMENT is required")
	require.NotEmpty(t, namespace, "CODEFLY_MANIFEST_NAMESPACE is required")

	profile, ok := builderv0.KubernetesOutputProfile_value[profileName]
	require.Truef(t, ok, "unknown CODEFLY_MANIFEST_PROFILE %q", profileName)

	ctx := context.Background()
	identity := &resources.ServiceIdentity{
		Workspace: "workspace",
		Module:    "module",
		Name:      "temporal",
		Version:   "0.0.0",
	}
	base := &services.Base{
		Wool:     wool.Get(ctx),
		Identity: identity,
		Information: &services.Information{
			Service: resources.ToServiceWithCase(identity),
			Module:  resources.ToModuleWithCase(identity),
		},
	}
	base.SetDockerImage(image)
	builder := &services.BuilderWrapper{Base: base}
	base.Builder = builder

	deployment := &builderv0.KubernetesDeployment{
		Namespace:   namespace,
		Destination: destination,
		Profile:     builderv0.KubernetesOutputProfile(profile),
		SecretReferences: map[string]*builderv0.KubernetesSecretKeyReference{
			"POSTGRES_USER": {Name: "temporal-postgres", Key: "user"},
			"POSTGRES_PWD":  {Name: "temporal-postgres", Key: "password"},
		},
	}
	configMap, err := services.EnvsAsConfigMapData(temporalServerConfig()...)
	require.NoError(t, err)
	params := services.DeploymentParameters{
		ConfigMap:        configMap,
		SecretReferences: deployment.GetSecretReferences(),
	}

	err = builder.KustomizeDeploy(ctx, &basev0.Environment{Name: environment}, deployment, deploymentFS, params)
	require.NoError(t, err)
}
