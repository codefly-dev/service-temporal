package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/shared"
	_ "github.com/lib/pq"

	"github.com/codefly-dev/core/agents/services"

	"go.temporal.io/server/temporal"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Runtime struct {
	services.RuntimeServer
	*Service

	// The embedded Temporal server
	temporalServer temporal.Server

	// Postgres connection details (parsed from codefly dependency)
	postgresHost     string
	postgresPort     int
	postgresUser     string
	postgresPassword string

	// Assigned ports from codefly
	grpcPort uint16
	httpPort uint16
}

func NewRuntime() *Runtime {
	return &Runtime{
		Service: NewService(),
	}
}

func (s *Runtime) Load(ctx context.Context, req *runtimev0.LoadRequest) (*runtimev0.LoadResponse, error) {
	defer s.Wool.Catch()

	return s.Runtime.LoadService(ctx, req, services.RuntimeLoad{
		Settings:     s.Settings,
		Requirements: requirements,
		ResolveEndpoints: func(ctx context.Context, endpoints []*basev0.Endpoint) error {
			grpcEndpoint, err := resources.FindTCPEndpointWithName(ctx, "grpc", endpoints)
			if err != nil {
				return s.Wool.Wrapf(err, "finding grpc endpoint")
			}
			httpEndpoint, err := resources.FindTCPEndpointWithName(ctx, "http", endpoints)
			if err != nil {
				return s.Wool.Wrapf(err, "finding http endpoint")
			}
			s.grpcEndpoint = grpcEndpoint
			s.httpEndpoint = httpEndpoint
			return nil
		},
	})
}

func (s *Runtime) CreateConnectionConfigurationInformation(_ context.Context, endpoint *basev0.Endpoint, instance *basev0.NetworkInstance) *basev0.ConfigurationInformation {
	var connection string
	switch endpoint.Name {
	case "grpc":
		connection = fmt.Sprintf("%s:%d", instance.Hostname, instance.Port)
	case "http":
		connection = fmt.Sprintf("http://%s:%d", instance.Hostname, instance.Port)
	}

	return &basev0.ConfigurationInformation{
		Name: endpoint.Name,
		ConfigurationValues: []*basev0.ConfigurationValue{
			{Key: "connection", Value: connection, Secret: false},
		},
	}
}

func (s *Runtime) CreateConnectionsConfiguration(runtimeContext *basev0.RuntimeContext, infos []*basev0.ConfigurationInformation) *basev0.Configuration {
	return &basev0.Configuration{
		Origin:         s.Base.Unique(),
		RuntimeContext: runtimeContext,
		Infos:          infos,
	}
}

// resolvePostgresFromDependencies extracts Postgres connection details from
// codefly dependency configurations (postgres service).
func (s *Runtime) resolvePostgresFromDependencies(confs []*basev0.Configuration) error {
	for _, conf := range confs {
		for _, info := range conf.Infos {
			if info.Name == "postgres" {
				for _, cv := range info.ConfigurationValues {
					if cv.Key == "owner-connection" {
						host, port, user, pass, err := parsePostgresConnectionString(cv.Value)
						if err != nil {
							return fmt.Errorf("cannot parse postgres connection string: %w", err)
						}
						s.postgresHost = host
						s.postgresPort = port
						s.postgresUser = user
						s.postgresPassword = pass
						s.Infof("resolved Postgres from dependency: %s:%d", host, port)
						return nil
					}
				}
			}
		}
	}
	return fmt.Errorf("no postgres owner-connection migration capability found: add a postgres service as a dependency")
}

// ensureDatabase creates a database if it does not already exist.
func (s *Runtime) ensureDatabase(ctx context.Context, dbName string) error {
	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%d/postgres?sslmode=disable",
		s.postgresUser, s.postgresPassword, s.postgresHost, s.postgresPort)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("cannot connect to postgres for database creation: %w", err)
	}
	defer db.Close()

	// Check if database exists
	var exists bool
	err = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("cannot check if database %s exists: %w", dbName, err)
	}

	if !exists {
		// CREATE DATABASE cannot run inside a transaction
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil {
			return fmt.Errorf("cannot create database %s: %w", dbName, err)
		}
		s.Infof("created database: %s", dbName)
	} else {
		s.Infof("database already exists: %s", dbName)
	}
	return nil
}

func (s *Runtime) Init(ctx context.Context, req *runtimev0.InitRequest) (*runtimev0.InitResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	s.Runtime.LogInitRequest(req)

	s.NetworkMappings = req.ProposedNetworkMappings

	// gRPC endpoint mapping
	grpcInstanceNative, err := resources.FindNetworkInstanceInNetworkMappings(ctx, req.ProposedNetworkMappings, s.grpcEndpoint, resources.NewNativeNetworkAccess())
	if err != nil {
		return s.Runtime.InitError(err)
	}

	// HTTP endpoint mapping
	httpInstanceNative, err := resources.FindNetworkInstanceInNetworkMappings(ctx, req.ProposedNetworkMappings, s.httpEndpoint, resources.NewNativeNetworkAccess())
	if err != nil {
		return s.Runtime.InitError(err)
	}

	s.grpcPort = uint16(grpcInstanceNative.Port)
	s.httpPort = uint16(httpInstanceNative.Port)

	s.Infof("Temporal gRPC will run on localhost:%d", s.grpcPort)
	s.Infof("Temporal HTTP will run on localhost:%d", s.httpPort)

	// Resolve Postgres connection from dependency configurations
	// Filter for the appropriate runtime context (like go-grpc does)
	if req.DependenciesConfigurations == nil || len(req.DependenciesConfigurations) == 0 {
		return s.Runtime.InitErrorf(fmt.Errorf("no dependencies configurations"), "temporal requires postgres dependency")
	}

	err = s.resolvePostgresFromDependencies(req.DependenciesConfigurations)
	if err != nil {
		return s.Runtime.InitErrorf(err, "resolving postgres dependency")
	}

	// Build network mappings and runtime configurations
	grpcMapping, err := resources.FindNetworkMapping(ctx, req.ProposedNetworkMappings, s.grpcEndpoint)
	if err != nil {
		return s.Runtime.InitError(err)
	}

	httpMapping, err := resources.FindNetworkMapping(ctx, req.ProposedNetworkMappings, s.httpEndpoint)
	if err != nil {
		return s.Runtime.InitError(err)
	}

	// Build configurations for each runtime context so dependents get connection info
	informations := make(map[string][]*basev0.ConfigurationInformation)

	for _, inst := range grpcMapping.Instances {
		rc := resources.RuntimeContextFromInstance(inst)
		informations[rc.Kind] = append(informations[rc.Kind], s.CreateConnectionConfigurationInformation(ctx, s.grpcEndpoint, inst))
	}
	for _, inst := range httpMapping.Instances {
		rc := resources.RuntimeContextFromInstance(inst)
		informations[rc.Kind] = append(informations[rc.Kind], s.CreateConnectionConfigurationInformation(ctx, s.httpEndpoint, inst))
	}
	for _, runtimeContext := range []*basev0.RuntimeContext{resources.NewRuntimeContextNative(), resources.NewRuntimeContextContainer()} {
		infos := informations[runtimeContext.Kind]
		if len(infos) > 0 {
			conf := s.CreateConnectionsConfiguration(runtimeContext, infos)
			s.Runtime.RuntimeConfigurations = append(s.Runtime.RuntimeConfigurations, conf)
		}
	}

	return s.Runtime.InitResponse()
}

func (s *Runtime) startTemporalServer(ctx context.Context) error {
	// Ensure temporal databases exist and schemas are applied
	err := s.ensureDatabase(ctx, "temporal")
	if err != nil {
		return fmt.Errorf("cannot ensure temporal database: %w", err)
	}
	err = s.ensureDatabase(ctx, "temporal_visibility")
	if err != nil {
		return fmt.Errorf("cannot ensure temporal_visibility database: %w", err)
	}

	// Apply schema migrations (idempotent — checks if schema_version table exists)
	if err := s.ensureSchema(ctx, "temporal", temporalSchemaSQL); err != nil {
		return fmt.Errorf("cannot apply temporal schema: %w", err)
	}
	if err := s.ensureSchema(ctx, "temporal_visibility", visibilitySchemaSQL); err != nil {
		return fmt.Errorf("cannot apply visibility schema: %w", err)
	}

	// Build Temporal server configuration
	sqlConnectAddr := fmt.Sprintf("%s:%d", s.postgresHost, s.postgresPort)

	defaultStoreConfig := config.SQL{
		PluginName:   postgresql.PluginName,
		ConnectAddr:  sqlConnectAddr,
		User:         s.postgresUser,
		Password:     s.postgresPassword,
		DatabaseName: "temporal",
		ConnectAttributes: map[string]string{
			"sslmode": "disable",
		},
		MaxConns: 20,
	}

	visibilityStoreConfig := config.SQL{
		PluginName:   postgresql.PluginName,
		ConnectAddr:  sqlConnectAddr,
		User:         s.postgresUser,
		Password:     s.postgresPassword,
		DatabaseName: "temporal_visibility",
		ConnectAttributes: map[string]string{
			"sslmode": "disable",
		},
		MaxConns: 20,
	}

	temporalConfig := &config.Config{
		Global: config.Global{
			Membership: config.Membership{
				MaxJoinDuration:  30 * time.Second,
				BroadcastAddress: "127.0.0.1",
			},
		},
		Persistence: config.Persistence{
			DefaultStore:     "default",
			VisibilityStore:  "visibility",
			NumHistoryShards: 1,
			DataStores: map[string]config.DataStore{
				"default": {
					SQL: &defaultStoreConfig,
				},
				"visibility": {
					SQL: &visibilityStoreConfig,
				},
			},
		},
		ClusterMetadata: &cluster.Config{
			EnableGlobalNamespace:    false,
			FailoverVersionIncrement: 10,
			MasterClusterName:        "active",
			CurrentClusterName:       "active",
			ClusterInformation: map[string]cluster.ClusterInformation{
				"active": {
					Enabled:                true,
					InitialFailoverVersion: 1,
					RPCAddress:             fmt.Sprintf("127.0.0.1:%d", s.grpcPort),
				},
			},
		},
		DCRedirectionPolicy: config.DCRedirectionPolicy{
			Policy: "noop",
		},
		Services: map[string]config.Service{
			"frontend": {
				RPC: config.RPC{
					GRPCPort:        int(s.grpcPort),
					MembershipPort:  getFreePort(),
					BindOnLocalHost: true,
				},
			},
			"history": {
				RPC: config.RPC{
					GRPCPort:        getFreePort(),
					MembershipPort:  getFreePort(),
					BindOnLocalHost: true,
				},
			},
			"matching": {
				RPC: config.RPC{
					GRPCPort:        getFreePort(),
					MembershipPort:  getFreePort(),
					BindOnLocalHost: true,
				},
			},
			"worker": {
				RPC: config.RPC{
					GRPCPort:        getFreePort(),
					MembershipPort:  getFreePort(),
					BindOnLocalHost: true,
				},
			},
		},
		Archival: config.Archival{
			History: config.HistoryArchival{
				State: "disabled",
			},
			Visibility: config.VisibilityArchival{
				State: "disabled",
			},
		},
		NamespaceDefaults: config.NamespaceDefaults{
			Archival: config.ArchivalNamespaceDefaults{
				History: config.HistoryArchivalNamespaceDefaults{
					State: "disabled",
				},
				Visibility: config.VisibilityArchivalNamespaceDefaults{
					State: "disabled",
				},
			},
		},
	}

	logger := log.NewZapLogger(log.BuildZapLogger(log.Config{
		Stdout: true,
		Level:  "info",
	}))

	serverOpts := []temporal.ServerOption{
		temporal.WithConfig(temporalConfig),
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithLogger(logger),
	}

	server, err := temporal.NewServer(serverOpts...)
	if err != nil {
		return fmt.Errorf("cannot create temporal server: %w", err)
	}

	s.temporalServer = server

	// Start the server in a goroutine (non-blocking)
	go func() {
		if startErr := server.Start(); startErr != nil {
			s.Wool.Error("temporal server failed")
		}
	}()

	return nil
}

func (s *Runtime) WaitForReady(ctx context.Context) error {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	addr := fmt.Sprintf("localhost:%d", s.grpcPort)

	// Use a real gRPC client to check Temporal is actually serving,
	// not just accepting TCP connections. Same pattern as postgres ping loop.
	return shared.Retry(2*time.Second, 30, func() error {
		conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr != nil {
			return dialErr
		}
		defer conn.Close()

		client := workflowservice.NewWorkflowServiceClient(conn)
		_, err := client.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
		return err
	})
}

func (s *Runtime) Start(ctx context.Context, req *runtimev0.StartRequest) (*runtimev0.StartResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	s.Infof("starting embedded Temporal server...")

	err := s.startTemporalServer(ctx)
	if err != nil {
		return s.Runtime.StartError(err)
	}

	s.Infof("waiting for Temporal to be ready...")

	err = s.WaitForReady(ctx)
	if err != nil {
		return s.Runtime.StartError(err)
	}

	s.Infof("Temporal server is ready on port %d", s.grpcPort)

	// Register the "default" namespace so clients can use it immediately
	if err := s.ensureDefaultNamespace(ctx); err != nil {
		return s.Runtime.StartError(fmt.Errorf("cannot register default namespace: %w", err))
	}

	return s.Runtime.StartResponse()
}

func (s *Runtime) Information(ctx context.Context, req *runtimev0.InformationRequest) (*runtimev0.InformationResponse, error) {
	return s.Runtime.InformationResponse(ctx, req)
}

func (s *Runtime) Stop(ctx context.Context, req *runtimev0.StopRequest) (*runtimev0.StopResponse, error) {
	defer s.Wool.Catch()

	if s.temporalServer != nil {
		s.Infof("stopping embedded Temporal server...")
		s.temporalServer.Stop()
		s.temporalServer = nil
	}

	return s.Runtime.StopResponse()
}

func (s *Runtime) Destroy(ctx context.Context, req *runtimev0.DestroyRequest) (*runtimev0.DestroyResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	if s.temporalServer != nil {
		s.temporalServer.Stop()
		s.temporalServer = nil
	}
	return s.Runtime.DestroyResponse()
}

func (s *Runtime) Test(ctx context.Context, req *runtimev0.TestRequest) (*runtimev0.TestResponse, error) {
	return s.Runtime.TestResponse()
}

// parsePostgresConnectionString extracts host, port, user, password from a
// PostgreSQL connection string of the form:
// postgresql://user:password@host:port/dbname?sslmode=disable
func parsePostgresConnectionString(connStr string) (host string, port int, user, password string, err error) {
	parsed, err := url.Parse(connStr)
	if err != nil {
		return "", 0, "", "", fmt.Errorf("parse postgres connection string: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", 0, "", "", fmt.Errorf("unsupported postgres connection scheme %q", parsed.Scheme)
	}
	if parsed.User == nil {
		return "", 0, "", "", fmt.Errorf("postgres connection string has no user information")
	}
	host = parsed.Hostname()
	if host == "" {
		return "", 0, "", "", fmt.Errorf("postgres connection string has no host")
	}
	port = 5432
	if rawPort := parsed.Port(); rawPort != "" {
		port, err = strconv.Atoi(rawPort)
		if err != nil || port < 1 || port > 65535 {
			return "", 0, "", "", fmt.Errorf("invalid postgres port %q", rawPort)
		}
	}
	user = parsed.User.Username()
	password, _ = parsed.User.Password()
	return host, port, user, password, nil
}

// ensureDefaultNamespace registers the "default" namespace if it doesn't exist.
func (s *Runtime) ensureDefaultNamespace(ctx context.Context) error {
	addr := fmt.Sprintf("localhost:%d", s.grpcPort)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("cannot dial temporal: %w", err)
	}
	defer conn.Close()

	client := workflowservice.NewWorkflowServiceClient(conn)

	// Check if the namespace already exists
	return shared.Retry(2*time.Second, 15, func() error {
		_, err := client.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: "default",
		})
		if err == nil {
			s.Infof("namespace 'default' already exists")
			return nil
		}

		// Try to register it
		_, regErr := client.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
			Namespace:                        "default",
			WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		})
		if regErr != nil {
			return fmt.Errorf("register namespace: %w", regErr)
		}
		s.Infof("registered namespace 'default'")
		return nil
	})
}

// allocatedPorts tracks ports already handed out by getFreePort to avoid
// TOCTOU races where multiple calls return the same port before Temporal binds.
// allocatedPortsMu guards it — Init allocates several internal ports and the
// map was read+written with no synchronization, a data race under -race and a
// real duplicate-port risk if allocations ever run concurrently.
var (
	allocatedPorts   = make(map[int]bool)
	allocatedPortsMu sync.Mutex
)

// getFreePort returns a free TCP port on localhost in the low range (< 32768).
// Temporal stores RPC ports as SMALLINT in PostgreSQL, which has a max of 32767.
// Each call returns a unique port not previously returned in this process.
func getFreePort() int {
	// Hold the lock across the whole scan so the check-and-reserve is atomic;
	// getFreePort is called a handful of times at Init, not on a hot path.
	allocatedPortsMu.Lock()
	defer allocatedPortsMu.Unlock()
	// Start at 11000 to avoid colliding with codefly's REST server (10001)
	// and other services commonly using the 10000 range.
	for port := 11000; port < 32000; port++ {
		if allocatedPorts[port] {
			continue
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			l.Close()
			allocatedPorts[port] = true
			return port
		}
	}
	return 0
}

// Ensure the postgresql plugin is registered.
var _ = postgresql.PluginName

// Embedded schema SQL for Temporal's PostgreSQL databases.
// These are copied from go.temporal.io/server/schema/postgresql/v12/.
//
//go:embed schema/temporal.sql
var temporalSchemaSQL string

//go:embed schema/visibility.sql
var visibilitySchemaSQL string

// ensureSchema applies the base schema to a database if schema_version doesn't exist yet.
// Idempotent: if schema_version already exists, it's a no-op.
func (s *Runtime) ensureSchema(ctx context.Context, dbName string, schemaSQL string) error {
	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		s.postgresUser, s.postgresPassword, s.postgresHost, s.postgresPort, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", dbName, err)
	}
	defer db.Close()

	// Check if schema_version table exists (Temporal's migration marker)
	var exists bool
	err = db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='schema_version')").Scan(&exists)
	if err != nil {
		return fmt.Errorf("check schema_version in %s: %w", dbName, err)
	}

	if exists {
		s.Infof("schema already applied for %s", dbName)
		return nil
	}

	// Apply the base schema
	s.Infof("applying schema to %s...", dbName)
	_, err = db.ExecContext(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("apply schema to %s: %w", dbName, err)
	}

	// Insert schema_version so Temporal knows the schema version
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_version (
			version_partition INT NOT NULL,
			db_name VARCHAR(255) NOT NULL,
			creation_time TIMESTAMP,
			curr_version VARCHAR(64),
			min_compatible_version VARCHAR(64),
			PRIMARY KEY (version_partition, db_name)
		)`)
	if err != nil {
		return fmt.Errorf("create schema_version in %s: %w", dbName, err)
	}

	_, err = db.ExecContext(ctx,
		"INSERT INTO schema_version (version_partition, db_name, creation_time, curr_version, min_compatible_version) VALUES (0, $1, NOW(), '1.18', '1.0') ON CONFLICT DO NOTHING",
		dbName)
	if err != nil {
		return fmt.Errorf("insert schema_version in %s: %w", dbName, err)
	}

	s.Infof("schema applied to %s", dbName)
	return nil
}
