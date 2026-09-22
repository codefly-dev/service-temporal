package main

import (
	"io/fs"
	"os"
	"testing"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	agentv0 "github.com/codefly-dev/core/generated/go/codefly/services/agent/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"github.com/stretchr/testify/require"
)

func TestManagedImageSyncAndBuild(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.CopyFS(root, os.DirFS("testdata/workspace")))
	builder := NewBuilder()
	loaded, err := builder.Load(t.Context(), &builderv0.LoadRequest{
		Identity: &basev0.ServiceIdentity{
			Name: "temporal", Version: "0.0.0", Module: "temporal-test",
			Workspace: "temporal-test", WorkspacePath: root,
			RelativeToWorkspace: "services/temporal",
		},
	})
	require.NoError(t, err)
	require.Equal(t, builderv0.LoadStatus_READY, loaded.GetState().GetState())
	before := snapshotFiles(t, root)
	for _, dryRun := range []bool{true, false} {
		response, err := builder.Sync(t.Context(), &builderv0.SyncRequest{DryRun: dryRun})
		require.NoError(t, err)
		require.Equal(t, builderv0.SyncStatus_SUCCESS, response.GetState().GetState())
		require.Empty(t, response.GetChangedFiles())
		require.Equal(t, before, snapshotFiles(t, root))
	}
	response, err := builder.Build(t.Context(), &builderv0.BuildRequest{})
	require.NoError(t, err)
	require.Equal(t, builderv0.BuildStatus_SUCCESS, response.GetState().GetState())
	require.Equal(t, []string{image.FullName()}, response.GetResult().GetDockerBuildResult().GetImages())
	require.NotEmpty(t, image.Digest)
	require.Equal(t, before, snapshotFiles(t, root))

	info, err := builder.GetAgentInformation(t.Context(), &agentv0.AgentInformationRequest{})
	require.NoError(t, err)
	require.True(t, info.GetValidation().GetSync().GetSupported())
	require.True(t, info.GetValidation().GetArtifactBuild().GetSupported())
	// There is no native source to lint or compile in this managed-image service.
	require.False(t, info.GetValidation().GetLint().GetSupported())
	require.False(t, info.GetValidation().GetCompile().GetSupported())
}

func TestManagedImageOperationsRequireLoadedService(t *testing.T) {
	builder := NewBuilder()
	sync, err := builder.Sync(t.Context(), &builderv0.SyncRequest{DryRun: true})
	require.NoError(t, err)
	require.Equal(t, builderv0.SyncStatus_ERROR, sync.GetState().GetState())
	build, err := builder.Build(t.Context(), &builderv0.BuildRequest{})
	require.NoError(t, err)
	require.Equal(t, builderv0.BuildStatus_ERROR, build.GetState().GetState())
}

func snapshotFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	filesystem := os.DirFS(root)
	require.NoError(t, fs.WalkDir(filesystem, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		contents, err := fs.ReadFile(filesystem, path)
		files[path] = string(contents)
		return err
	}))
	return files
}
