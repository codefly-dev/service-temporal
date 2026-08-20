package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
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
	if _, err := exec.LookPath("codefly"); err != nil {
		t.Skip("codefly CLI not available; skipping full-stack integration test")
	}

	ctx := context.Background()
	wool.SetGlobalLogLevel(wool.DEBUG)

	pgConn := startTestPostgres(ctx, t)
	grpcAddr := bootTemporal(ctx, t, pgConn)

	runPingWorkflow(ctx, t, grpcAddr)
	assertSchemaCurrent(ctx, t, pgConn)
}

// TestTemporalSchemaUpgrade proves the runtime migrates a database created by a
// previous release. It seeds temporal / temporal_visibility with the base
// snapshots and the "1.18" schema_version marker that older code always wrote
// (regardless of the actual applied structure), then boots the server. The
// pinned server enforces temporal schema 1.19, so a boot that skipped the
// upgrade would fail its compatibility check and the ping workflow would never
// complete.
func TestTemporalSchemaUpgrade(t *testing.T) {
	if _, err := exec.LookPath("codefly"); err != nil {
		t.Skip("codefly CLI not available; skipping full-stack integration test")
	}

	ctx := context.Background()
	wool.SetGlobalLogLevel(wool.DEBUG)

	pgConn := startTestPostgres(ctx, t)
	seedLegacySchema(ctx, t, pgConn)

	grpcAddr := bootTemporal(ctx, t, pgConn)

	runPingWorkflow(ctx, t, grpcAddr)
	assertSchemaCurrent(ctx, t, pgConn)
}

// startTestPostgres starts the postgres dependency via codefly and returns its
// owner connection string. Cleanup restores the working directory and tears the
// dependency down.
func startTestPostgres(ctx context.Context, t *testing.T) string {
	// cd into the temporal service dir so codefly finds the workspace
	workspaceDir, err := filepath.Abs("testdata/workspace/services/temporal")
	require.NoError(t, err)
	origDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(workspaceDir))
	t.Cleanup(func() { _ = os.Chdir(origDir) })

	// --exclude-root means codefly starts ONLY the dependencies, not temporal
	// itself. We start temporal ourselves via the runtime.
	//
	// The timeout must cover a cold start of the postgres dependency: during a
	// release the CLI pulls the postgres image at test time, which alone can
	// exceed the previous 90s budget on a loaded CI runner.
	deps, err := sdk.WithDependencies(ctx, sdk.WithDebug(), sdk.WithTimeout(3*time.Minute))
	require.NoError(t, err, "codefly must start postgres")
	t.Cleanup(func() { _ = deps.Destroy(ctx) })

	// Get the postgres connection using the resources helper (not hardcoded env vars)
	pgEnvKey := resources.ServiceSecretConfigurationKeyFromUnique("temporal-test/postgres", "postgres", "owner-connection")
	pgConn := os.Getenv(pgEnvKey)
	require.NotEmpty(t, pgConn, "postgres connection must be injected by codefly (env key: %s)", pgEnvKey)
	t.Logf("Postgres: %s", pgConn)

	// Back to the plugin dir so the runtime can build the service.
	require.NoError(t, os.Chdir(origDir))
	return pgConn
}

// bootTemporal builds and starts the embedded Temporal server against the given
// postgres connection and returns its gRPC address.
func bootTemporal(ctx context.Context, t *testing.T, pgConn string) string {
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
	_, err := builder.Load(ctx, &builderv0.LoadRequest{
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

	networkMappings, err := networkMgr.GenerateNetworkMappings(ctx, env, workspace, runtime.Identity, runtime.Endpoints, resources.NewRuntimeContextNative())
	require.NoError(t, err)

	// Pass the REAL postgres connection from codefly as a dependency
	depConfs := []*basev0.Configuration{{
		Origin:         "temporal-test/postgres",
		RuntimeContext: resources.NewRuntimeContextNative(),
		Infos: []*basev0.ConfigurationInformation{{
			Name: "postgres",
			ConfigurationValues: []*basev0.ConfigurationValue{
				{Key: "owner-connection", Value: pgConn, Secret: true},
			},
		}},
	}}

	temporalInitResp, err := runtime.Init(ctx, &runtimev0.InitRequest{
		RuntimeContext:             resources.NewRuntimeContextNative(),
		ProposedNetworkMappings:    networkMappings,
		DependenciesConfigurations: depConfs,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = runtime.Destroy(ctx, &runtimev0.DestroyRequest{}) })

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
	return grpcAddr
}

// runPingWorkflow connects a worker + client and executes a workflow end to end.
// It only succeeds if the server actually came up and is serving.
func runPingWorkflow(ctx context.Context, t *testing.T, grpcAddr string) {
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

// assertSchemaCurrent verifies both databases carry the structure and recorded
// version the pinned server requires — the checks that would have caught a
// schema declared at a version its content does not match.
func assertSchemaCurrent(ctx context.Context, t *testing.T, pgConn string) {
	host, port, user, password, err := parsePostgresConnectionString(pgConn)
	require.NoError(t, err)
	openDB := func(dbName string) *sql.DB {
		db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable", user, password, host, port, dbName))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	tdb := openDB("temporal")
	var chasm bool
	require.NoError(t, tdb.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='current_chasm_executions')").Scan(&chasm))
	assert.True(t, chasm, "temporal must have current_chasm_executions table (schema v1.19)")
	var temporalVer string
	require.NoError(t, tdb.QueryRowContext(ctx,
		"SELECT curr_version FROM schema_version WHERE db_name='temporal'").Scan(&temporalVer))
	assert.Equal(t, temporalSchemaVersion, temporalVer, "temporal schema_version must be recorded as current")

	vdb := openDB("temporal_visibility")
	var externalPayloadCol bool
	require.NoError(t, vdb.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='executions_visibility' AND lower(column_name)=lower('TemporalExternalPayloadSizeBytes'))").Scan(&externalPayloadCol))
	assert.True(t, externalPayloadCol, "visibility must have TemporalExternalPayloadSizeBytes column (schema v1.14)")
	var visibilityVer string
	require.NoError(t, vdb.QueryRowContext(ctx,
		"SELECT curr_version FROM schema_version WHERE db_name='temporal_visibility'").Scan(&visibilityVer))
	assert.Equal(t, visibilitySchemaVersion, visibilityVer, "visibility schema_version must be recorded as current")
}

// seedLegacySchema reproduces a database created by a previous release: the base
// snapshots applied with the schema_version marker hardcoded to "1.18" for both
// databases, which is what the prior code wrote regardless of actual structure.
//
// The `temporal` database is provisioned and held open by the codefly postgres
// agent, so it is reset by clearing its public schema rather than dropping the
// database (dropping it out from under the agent races with its setup). The
// snapshots recreate the extension and functions they need, so a cleared schema
// is a sufficient blank slate.
func seedLegacySchema(ctx context.Context, t *testing.T, pgConn string) {
	host, port, user, password, err := parsePostgresConnectionString(pgConn)
	require.NoError(t, err)

	admin, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s:%s@%s:%d/postgres?sslmode=disable", user, password, host, port))
	require.NoError(t, err)
	defer admin.Close()

	seed := func(dbName, schemaSQL string) {
		var exists bool
		require.NoError(t, admin.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists))
		if !exists {
			_, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
			require.NoError(t, err)
		}

		db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable", user, password, host, port, dbName))
		require.NoError(t, err)
		defer db.Close()

		// Clear any prior structure (e.g. from an earlier test in this run) so
		// the legacy snapshot applies onto a blank slate.
		_, err = db.ExecContext(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, schemaSQL)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `
			CREATE TABLE schema_version (
				version_partition INT NOT NULL,
				db_name VARCHAR(255) NOT NULL,
				creation_time TIMESTAMP,
				curr_version VARCHAR(64),
				min_compatible_version VARCHAR(64),
				PRIMARY KEY (version_partition, db_name)
			)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO schema_version (version_partition, db_name, creation_time, curr_version, min_compatible_version) VALUES (0, $1, NOW(), '1.18', '1.0')",
			dbName)
		require.NoError(t, err)
	}

	seed("temporal", temporalSchemaSQL)
	seed("temporal_visibility", visibilitySchemaSQL)
	t.Logf("seeded legacy schema (v1.18 marker) for temporal and temporal_visibility")
}

func pingWorkflow(ctx workflow.Context, input string) (string, error) {
	return "pong: " + input, nil
}
