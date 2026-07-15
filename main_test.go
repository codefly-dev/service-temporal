package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codefly-dev/core/network"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/sdk"
	"github.com/codefly-dev/core/shared"
	"github.com/codefly-dev/core/wool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"

	temporalclient "go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// ── Unit tests ───────────────────────────────────────────────────────────────

func TestParsePostgresConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		connStr  string
		wantHost string
		wantPort int
		wantUser string
		wantPass string
		wantErr  bool
	}{
		{
			name:     "standard",
			connStr:  "postgresql://myuser:mypass@localhost:5432/mydb?sslmode=disable",
			wantHost: "localhost",
			wantPort: 5432,
			wantUser: "myuser",
			wantPass: "mypass",
		},
		{
			name:     "escaped credentials and ipv6",
			connStr:  "postgresql://my%40user:p%3A%40ss@[::1]:6432/mydb",
			wantHost: "::1",
			wantPort: 6432,
			wantUser: "my@user",
			wantPass: "p:@ss",
		},
		{
			name:    "missing @ sign",
			connStr: "postgresql://localhost:5432/mydb",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, user, pass, err := parsePostgresConnectionString(tt.connStr)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantPort, port)
			assert.Equal(t, tt.wantUser, user)
			assert.Equal(t, tt.wantPass, pass)
		})
	}
}

// ── Integration test ─────────────────────────────────────────────────────────
// 1. cli.WithDependencies starts postgres (temporal's dependency)
// 2. We start temporal ourselves via NewRuntime() with the postgres connection
// 3. Connect temporal client, execute workflow, verify

func TestTemporalFullStack(t *testing.T) {
	ctx := context.Background()
	wool.SetGlobalLogLevel(wool.DEBUG)

	// cd into the temporal service dir so codefly finds the workspace
	workspaceDir, err := filepath.Abs("testdata/workspace/services/temporal")
	require.NoError(t, err)
	origDir, _ := os.Getwd()
	defer os.Chdir(origDir)
	require.NoError(t, os.Chdir(workspaceDir))

	// ── 1. Start dependencies (postgres) via codefly ──
	// --exclude-root means codefly starts ONLY the dependencies, not temporal itself.
	// We start temporal ourselves below.
	deps, err := sdk.WithDependencies(ctx, sdk.WithDebug(), sdk.WithTimeout(90*time.Second))
	require.NoError(t, err, "codefly must start postgres")
	defer func() { _ = deps.Destroy(ctx) }()

	// Get the postgres connection using the resources helper (not hardcoded env vars)
	pgEnvKey := resources.ServiceSecretConfigurationKeyFromUnique("temporal-test/postgres", "postgres", "connection")
	pgConn := os.Getenv(pgEnvKey)
	require.NotEmpty(t, pgConn, "postgres connection must be injected by codefly (env key: %s)", pgEnvKey)
	t.Logf("Postgres: %s", pgConn)

	// ── 2. Start temporal via our own runtime ──
	// Go back to the plugin dir for building
	require.NoError(t, os.Chdir(origDir))

	tmpDir := t.TempDir()
	workspace := &resources.Workspace{Name: "test"}
	env := resources.LocalEnvironment()

	temporalServiceName := fmt.Sprintf("temporal-%d", time.Now().UnixMilli())
	temporalService := &resources.Service{Name: temporalServiceName, Version: "0.0.0"}
	require.NoError(t, temporalService.SaveAtDir(ctx, filepath.Join(tmpDir, "mod", temporalServiceName)))

	temporalIdentity := &basev0.ServiceIdentity{
		Name:                temporalServiceName,
		Version:             temporalService.Version,
		Module:              "mod",
		Workspace:           workspace.Name,
		WorkspacePath:       tmpDir,
		RelativeToWorkspace: fmt.Sprintf("mod/%s", temporalServiceName),
	}

	builder := NewBuilder()
	_, err = builder.Load(ctx, &builderv0.LoadRequest{
		Identity:     temporalIdentity,
		CreationMode: &builderv0.CreationMode{Communicate: false},
	})
	require.NoError(t, err)
	_, err = builder.Create(ctx, &builderv0.CreateRequest{})
	require.NoError(t, err)

	runtime := NewRuntime()
	_, err = runtime.Load(ctx, &runtimev0.LoadRequest{
		Identity:     temporalIdentity,
		Environment:  shared.Must(env.Proto()),
		DisableCatch: true,
	})
	require.NoError(t, err)

	networkMgr, err := network.NewRuntimeManager(ctx, nil)
	require.NoError(t, err)
	networkMgr.WithTemporaryPorts()

	networkMappings, err := networkMgr.GenerateNetworkMappings(ctx, env, workspace, runtime.Identity, runtime.Endpoints)
	require.NoError(t, err)

	// Pass the REAL postgres connection from codefly as a dependency
	depConfs := []*basev0.Configuration{{
		Origin:         "temporal-test/postgres",
		RuntimeContext: resources.NewRuntimeContextNative(),
		Infos: []*basev0.ConfigurationInformation{{
			Name: "postgres",
			ConfigurationValues: []*basev0.ConfigurationValue{
				{Key: "connection", Value: pgConn, Secret: true},
			},
		}},
	}}

	temporalInitResp, err := runtime.Init(ctx, &runtimev0.InitRequest{
		RuntimeContext:             resources.NewRuntimeContextNative(),
		ProposedNetworkMappings:    networkMappings,
		DependenciesConfigurations: depConfs,
	})
	require.NoError(t, err)
	defer func() { _, _ = runtime.Destroy(ctx, &runtimev0.DestroyRequest{}) }()

	// Start the embedded Temporal server
	_, err = runtime.Start(ctx, &runtimev0.StartRequest{})
	require.NoError(t, err)
	t.Logf("Temporal running on port %d", runtime.grpcPort)

	// Extract gRPC connection
	temporalConf, err := resources.ExtractConfiguration(temporalInitResp.RuntimeConfigurations, resources.NewRuntimeContextNative())
	require.NoError(t, err)
	grpcAddr, err := resources.GetConfigurationValue(ctx, temporalConf, "grpc", "connection")
	require.NoError(t, err)
	require.NotEmpty(t, grpcAddr)
	t.Logf("Temporal gRPC: %s", grpcAddr)

	// ── 3. Connect and execute a workflow ──
	c, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  grpcAddr,
		Namespace: "default",
	})
	require.NoError(t, err)
	defer c.Close()

	taskQueue := "plugin-test"
	w := temporalworker.New(c, taskQueue, temporalworker.Options{})
	w.RegisterWorkflow(pingWorkflow)
	go func() { _ = w.Run(temporalworker.InterruptCh()) }()
	defer w.Stop()

	time.Sleep(1 * time.Second)

	run, err := c.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        fmt.Sprintf("ping-%d", time.Now().UnixNano()),
		TaskQueue: taskQueue,
	}, pingWorkflow, "hello")
	require.NoError(t, err)

	var result string
	err = run.Get(ctx, &result)
	require.NoError(t, err)
	assert.Equal(t, "pong: hello", result)
	t.Logf("SUCCESS: %s", result)
}

func pingWorkflow(ctx workflow.Context, input string) (string, error) {
	return "pong: " + input, nil
}
