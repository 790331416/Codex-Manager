# Build, Release, and Script Guide

## Local development

### Frontend
```bash
pnpm -C apps install
pnpm -C apps run dev
pnpm -C apps run test
pnpm -C apps run test:ui
pnpm -C apps run build
```

### Rust
```bash
cargo test --workspace
cargo build -p codexmanager-service --release
cargo build -p codexmanager-web --release
cargo build -p codexmanager-start --release

# Bundle frontend static assets into codexmanager-web (single-binary mode)
pnpm -C apps run build
cargo build -p codexmanager-web --release --features embedded-ui
```

## Tauri packaging

### Windows
```powershell
pwsh -NoLogo -NoProfile -File scripts/rebuild.ps1 -Bundle nsis -CleanDist -Portable
```

### Linux / macOS
```bash
./scripts/rebuild-linux.sh --bundles "appimage,deb" --clean-dist
./scripts/rebuild-macos.sh --bundles "dmg" --clean-dist
```

## GitHub Actions
The unified release workflow is `.github/workflows/release-all.yml`. It is triggered through `workflow_dispatch` and does not run automatically.

### `release-all.yml`
- Purpose: publish Desktop + Service artifacts for all supported platforms in one run
- Build targets: `Windows`, `macOS (dmg)`, `Linux`
- The frontend `dist` is built once, then reused by the packaging jobs for each platform
- Inputs:
  - `tag`: required
  - `ref`: defaults to `main`
  - `prerelease`: defaults to `auto`, options: `auto | true | false`
- Behavior: packages and publishes artifacts only; it no longer includes server-side test gates

## Release artifacts

### Desktop
- Windows: `CodexManager_<version>_x64-setup.exe`, `CodexManager-portable.exe`
- macOS: `CodexManager_<version>_aarch64.dmg`, `CodexManager_<version>_x64.dmg`
- Linux: `CodexManager_<version>_amd64.AppImage`, `CodexManager_<version>_amd64.deb`, `CodexManager-linux-portable.zip`

### Service
- Windows: `CodexManager-service-windows-x86_64.zip`
- macOS: `CodexManager-service-macos-arm64.zip`, `CodexManager-service-macos-x64.zip`
- Linux: `CodexManager-service-linux-x86_64.zip`
- Linux (web test bundle): `CodexManager-web-linux-x86_64.zip`

### Release type
- When `prerelease=auto`, tags containing `-` are published as pre-releases.
- When `prerelease=auto`, tags without `-` are published as stable releases.
- When `prerelease=true|false`, the tag-based auto-detection is overridden.
- Rerunning the same `tag` updates the Release metadata using the current input values.
- GitHub still attaches `Source code (zip/tar.gz)` automatically.

## `scripts/rebuild.ps1`
By default, this script packages Windows builds locally. In `-AllPlatforms` mode, it triggers the GitHub release workflow.

### Common examples
```powershell
# Local Windows build
pwsh -NoLogo -NoProfile -File scripts/rebuild.ps1 -Bundle nsis -CleanDist -Portable

# Trigger the release workflow (and download artifacts)
pwsh -NoLogo -NoProfile -File scripts/rebuild.ps1 `
  -AllPlatforms `
  -GitRef main `
  -ReleaseTag v0.1.9 `
  -GithubToken <token>

# Force a pre-release
pwsh -NoLogo -NoProfile -File scripts/rebuild.ps1 `
  -AllPlatforms -GitRef main -ReleaseTag v0.1.9-beta.1 -GithubToken <token> -Prerelease true
```

### Main parameters
- `-Bundle nsis|msi`: defaults to `nsis`
- `-NoBundle`: compile only, do not create an installer
- `-CleanDist`: clean `apps/out` before building
- `-Portable`: also output a portable build
- `-PortableDir <path>`: portable build output directory, default `portable/`
- `-AllPlatforms`: trigger the release workflow
- `-GithubToken <token>`: GitHub token; if omitted, the script tries `GITHUB_TOKEN` / `GH_TOKEN`
- `-WorkflowFile <name>`: defaults to `release-all.yml`
- `-GitRef <ref>`: workflow build ref; defaults to the current branch or current tag
- `-ReleaseTag <tag>`: release tag; recommended explicitly when using `-AllPlatforms`
- `-Prerelease <auto|true|false>`: defaults to `auto`
- `-DownloadArtifacts <bool>`: defaults to `true`
- `-ArtifactsDir <path>`: artifact download directory, default `artifacts/`
- `-PollIntervalSec <n>`: polling interval, default `10`
- `-TimeoutMin <n>`: timeout in minutes, default `60`
- `-DryRun`: print the execution plan only

## `scripts/bump-version.ps1`
```powershell
pwsh -NoLogo -NoProfile -File scripts/bump-version.ps1 -Version 0.1.9
```

This updates:
- the workspace version in the root `Cargo.toml`
- `apps/src-tauri/Cargo.toml`
- `apps/src-tauri/tauri.conf.json`

## Protocol regression probe
```powershell
pwsh -NoLogo -NoProfile -File scripts/tests/gateway_regression_suite.ps1 `
  -Base http://localhost:45760 -ApiKey <key> -Model gpt-5.3-codex
```

It runs the following in sequence:
- `chat_tools_hit_probe.ps1`
- `chat_tools_hit_probe.ps1 -Stream`
- `codex_stream_probe.ps1`

## Related documents
- Release and artifacts: [Release and Artifacts](release-and-artifacts.md)
- Script responsibility matrix: [Script and Release Responsibility Matrix](../report/script-and-release-responsibility-matrix.md)
