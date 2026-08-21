package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codefly-dev/core/agents/services"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/wool"
	"github.com/stretchr/testify/require"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
)

// renderDeployment renders the plugin's Kustomize templates for one profile
// into a fresh directory and returns it, exercising the same production render
// function the builder uses.
func renderDeployment(t *testing.T, params services.DeploymentParameters, profile builderv0.KubernetesOutputProfile) string {
	t.Helper()
	ctx := context.Background()
	identity := &resources.ServiceIdentity{Workspace: "workspace", Module: "module", Name: "temporal", Version: "0.0.0"}
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

	destination := t.TempDir()
	deployment := &builderv0.KubernetesDeployment{
		Namespace:        "codefly-test",
		Destination:      destination,
		Profile:          profile,
		SecretReferences: params.SecretReferences,
	}
	require.NoError(t, builder.KustomizeDeploy(ctx, &basev0.Environment{Name: "test"}, deployment, deploymentFS, params))
	return destination
}

func readFile(t *testing.T, dir, rel string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
	require.NoError(t, err)
	return string(content)
}

func treeHasSecretResource(t *testing.T, dir string) bool {
	t.Helper()
	found := false
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for line := range strings.SplitSeq(string(content), "\n") {
			if strings.TrimSpace(line) == "kind: Secret" {
				found = true
			}
		}
		return nil
	}))
	return found
}

func TestTemporalServerConfigSkipDBCreateByProfile(t *testing.T) {
	// Restricted/promotable runtime roles are NOCREATEDB, so auto-setup must not
	// try to create databases. The ephemeral profile connects with owner
	// credentials and nothing else creates temporal_visibility, so it must.
	restricted, err := services.EnvsAsConfigMapData(temporalServerConfig(true)...)
	require.NoError(t, err)
	require.Equal(t, "true", restricted["SKIP_DB_CREATE"])

	ephemeral, err := services.EnvsAsConfigMapData(temporalServerConfig(false)...)
	require.NoError(t, err)
	require.Equal(t, "false", ephemeral["SKIP_DB_CREATE"],
		"ephemeral must let auto-setup create temporal_visibility; nothing else does")
}

// TestAutoSetupEnvConformance exercises the render-time check exactly as the
// Deploy hook invokes it: from ConfigMap values, inlined Secret values, and
// external Secret references combined.
func TestAutoSetupEnvConformance(t *testing.T) {
	ref := func() map[string]*builderv0.KubernetesSecretKeyReference {
		return map[string]*builderv0.KubernetesSecretKeyReference{}
	}
	withRefs := func(names ...string) map[string]*builderv0.KubernetesSecretKeyReference {
		refs := ref()
		for _, name := range names {
			refs[name] = &builderv0.KubernetesSecretKeyReference{Name: "temporal-postgres", Key: name}
		}
		return refs
	}

	t.Run("restricted with the discrete secret references is complete", func(t *testing.T) {
		err := validateAutoSetupEnv(
			temporalServerConfig(true),
			nil,
			withRefs("POSTGRES_SEEDS", "DB_PORT", "POSTGRES_USER", "POSTGRES_PWD"),
		)
		require.NoError(t, err)
	})

	t.Run("connection-url-only references leave the entrypoint contract unmet", func(t *testing.T) {
		err := validateAutoSetupEnv(
			temporalServerConfig(true),
			nil,
			withRefs("TEMPORAL_RO_CONNECTION", "TEMPORAL_RW_CONNECTION"),
		)
		require.Error(t, err)
		for _, missing := range []string{"POSTGRES_SEEDS", "DB_PORT", "POSTGRES_USER", "POSTGRES_PWD"} {
			require.Contains(t, err.Error(), missing)
		}
	})

	t.Run("restricted references missing the port silently misconnect and must fail", func(t *testing.T) {
		err := validateAutoSetupEnv(
			temporalServerConfig(true),
			nil,
			withRefs("POSTGRES_SEEDS", "POSTGRES_USER", "POSTGRES_PWD"),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DB_PORT")
	})

	t.Run("non-restricted discrete render is complete", func(t *testing.T) {
		configMap := append(temporalServerConfig(false),
			resources.Env("POSTGRES_SEEDS", "10.0.0.5"),
			resources.Env("DB_PORT", 5432),
		)
		secrets := []*resources.EnvironmentVariable{
			resources.Env("POSTGRES_USER", "temporal"),
			resources.Env("POSTGRES_PWD", "secret"),
		}
		require.NoError(t, validateAutoSetupEnv(configMap, secrets, nil))
	})
}

func TestDeploymentSecretWiringByProfile(t *testing.T) {
	restricted := builderv0.KubernetesOutputProfile_KUBERNETES_OUTPUT_PROFILE_RESTRICTED_PORTABLE_V1
	ephemeral := builderv0.KubernetesOutputProfile_KUBERNETES_OUTPUT_PROFILE_EPHEMERAL_LOCAL_APPLY_V1
	refs := map[string]*builderv0.KubernetesSecretKeyReference{
		"POSTGRES_USER": {Name: "temporal-postgres", Key: "user"},
	}

	t.Run("restricted references external secret without emitting one", func(t *testing.T) {
		dir := renderDeployment(t, services.DeploymentParameters{SecretReferences: refs}, restricted)
		deploymentYAML := readFile(t, dir, "base/deployment.yaml")
		require.Contains(t, deploymentYAML, "secretKeyRef")
		require.Contains(t, deploymentYAML, "name: \"temporal-postgres\"")
		require.NotContains(t, deploymentYAML, "envFrom:\n            - secretRef")
		require.False(t, treeHasSecretResource(t, dir), "restricted output must not emit a Secret resource")
		require.NotContains(t, readFile(t, dir, "overlays/test/kustomization.yaml"), "secret.yaml")
	})

	t.Run("restricted with no references emits no dangling env key", func(t *testing.T) {
		dir := renderDeployment(t, services.DeploymentParameters{}, restricted)
		deploymentYAML := readFile(t, dir, "base/deployment.yaml")
		for line := range strings.SplitSeq(deploymentYAML, "\n") {
			require.NotEqual(t, "env:", strings.TrimSpace(line), "no secret references must not render a bare env: key")
		}
		require.NotContains(t, deploymentYAML, "secretKeyRef")
		require.False(t, treeHasSecretResource(t, dir))
	})

	t.Run("ephemeral inlines a Secret and mounts it via envFrom", func(t *testing.T) {
		params := services.DeploymentParameters{
			SecretMap:        services.EnvironmentMap{"POSTGRES_USER": "cG9zdGdyZXM="},
			SecretReferences: refs,
		}
		dir := renderDeployment(t, params, ephemeral)
		deploymentYAML := readFile(t, dir, "base/deployment.yaml")
		require.Contains(t, deploymentYAML, "secretRef")
		require.NotContains(t, deploymentYAML, "secretKeyRef")
		require.True(t, treeHasSecretResource(t, dir), "ephemeral output must inline a Secret resource")
		require.Contains(t, readFile(t, dir, "overlays/test/kustomization.yaml"), "secret.yaml")
	})
}
