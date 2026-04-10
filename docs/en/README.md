# Documentation Index

`docs/` is the official long-form documentation directory for CodexManager.

Its purpose is simple:
- Keep governance notes, release guides, and operating manuals inside the repository.
- Make it easy for new contributors to find the right document without relying on tribal knowledge.

## Scope
- Root `README.md` / `README.en.md`: project overview and quick start.
- Root `CHANGELOG.md`: version history and unreleased changes.
- `report/*`: operations, troubleshooting, compatibility notes, and FAQs.
- `release/*`: build, packaging, release, and artifact documentation.

## Start here
- For the latest release notes, see [CHANGELOG.md](CHANGELOG.md).
- If you are not sure which document to open first, use the table below.

## Sponsors

Thanks to the following sponsors for supporting CodexManager.

<table>
  <tr>
    <td align="center" valign="middle" width="180">
      <a href="https://www.aixiamo.com/">
        <img src="../../assets/images/sponsors/aixiamo.ico" alt="XiaoMo AI Shop" width="88" />
      </a>
    </td>
    <td valign="top">
      <strong>XiaoMo AI Shop (MoDuanXia)</strong> provides stable GPT and Gemini membership top-up services for CodexManager users, with self-service purchase and activation. Register on the <a href="https://www.aixiamo.com/">official site</a> and enter <code>cliproxyapi</code> during recharge to get a dedicated discount.
    </td>
  </tr>
  <tr>
    <td align="center" valign="middle" width="180">
      <a href="https://gzxsy.vip/">
        <img src="../../assets/images/sponsors/xingsiyan.jpg" alt="Xing Si Yan Gateway" width="120" />
      </a>
    </td>
    <td valign="top">
      <strong>Xing Si Yan Gateway</strong> offers stable relay and supporting services for Claude Code, Codex, Gemini, and similar model-access scenarios. It is a strong fit for developers and teams that need reliable APIs, fast onboarding, and steady delivery support. Visit the <a href="https://gzxsy.vip/">official site</a> for the latest plans.
    </td>
  </tr>
</table>

Other sponsors: PackyCode, AICodeMirror, BmoPlus, LingtrueAPI, Poixe AI

## Ecosystem Pairing

### OpenCowork

- Repository: [AIDotNet/OpenCowork](https://github.com/AIDotNet/OpenCowork)
- Recommended pairing: use OpenCowork for local file operations, multi-agent execution, workplace messaging, and desktop automation, while CodexManager handles Codex account management, usage tracking, platform keys, and the local gateway entry point.
- Best for: teams that want to separate the execution workspace and office integration from account-pool management and gateway access.
- A simple way to think about it: **OpenCowork executes in the real workspace, CodexManager manages accounts and gateway access.**

## Quick navigation
| What you need | Open this document |
| --- | --- |
| First launch, deployment, Docker, macOS allowlisting | [Runtime and Deployment Guide](report/runtime-and-deployment-guide.md) |
| Environment variables, database, ports, proxy, listen address | [Environment and Runtime Configuration](report/environment-and-runtime-config.md) |
| Account routing, import errors, challenge interception | [FAQ and Account Routing Rules](report/faq-and-account-routing-rules.md) |
| Why background jobs skip or disable accounts | [Background Task Account Skip Notes](report/background-task-account-skip-notes.md) |
| Minimum plugin marketplace integration | [Plugin Center Minimal Integration](report/plugin-center-minimal-integration.md) |
| Internal commands and integration surfaces | [System Internal Interface Inventory](report/system-internal-interface-inventory.md) |
| Local build, packaging, and release scripts | [Build, Release, and Script Guide](release/build-release-and-scripts.md) |

## Directory guide

### `release/`
Release notes, rollback notes, artifact descriptions, and packaging guides.

### `report/`
Operational guides, troubleshooting notes, compatibility reports, and FAQs.

## Recommended reading

### Operations
| Document | Summary |
| --- | --- |
| [Runtime and Deployment Guide](report/runtime-and-deployment-guide.md) | Desktop first launch, Service edition, Docker, and macOS first-run handling |
| [Environment and Runtime Configuration](report/environment-and-runtime-config.md) | Runtime configuration, defaults, and environment variables |
| [FAQ and Account Routing Rules](report/faq-and-account-routing-rules.md) | Common account-routing issues and troubleshooting tips |
| [Gateway vs Official Codex Params](report/gateway-vs-codex-official-params.md) | Current outbound parameter differences compared with official Codex |
| [Background Task Account Skip Notes](report/background-task-account-skip-notes.md) | Why background jobs skip, cool down, or disable accounts |
| [Minimal Troubleshooting Guide](report/minimal-troubleshooting-guide.md) | Fast checks for the most common startup and relay issues |
| [Plugin Center Minimal Integration](report/plugin-center-minimal-integration.md) | Minimum fields and interfaces required for plugin marketplace access |
| [Gateway vs Codex Headers and Params](report/gateway-vs-codex-headers-and-params.md) | Header and request parameter differences between the gateway and Codex |
| [Plugin Center Integration and Interfaces](report/plugin-center-integration-and-interfaces.md) | Marketplace modes, RPC/Tauri commands, manifest fields, and Rhai interfaces |
| [System Internal Interface Inventory](report/system-internal-interface-inventory.md) | Internal commands, RPC endpoints, and built-in plugin functions |

### Build and release
| Document | Summary |
| --- | --- |
| [Build, Release, and Script Guide](release/build-release-and-scripts.md) | Local builds, script parameters, and GitHub workflow entry points |
| [Release and Artifacts](release/release-and-artifacts.md) | Release artifacts, naming, and publication rules |
| [Script and Release Responsibility Matrix](report/script-and-release-responsibility-matrix.md) | Which script or workflow is responsible for which task |

## Contribution rules

### Commit documentation when it
- remains useful for future contributors,
- affects development, testing, release, or troubleshooting,
- or serves as a long-term source of truth.

### Do not commit documentation when it is
- a temporary draft,
- personal working notes,
- a disposable intermediate file,
- or a local-only experiment record.

## Ignored patterns
- `docs/**/*.tmp.md`
- `docs/**/*.local.md`

Do not use those suffixes for formal documentation.

## Naming

```text
Long-lived documents: topic.md
One-off reports: yyyyMMddHHmmssfff_topic.md
```

## Maintenance notes
- Add important governance material under `docs/` instead of expanding the README indefinitely.
- Keep version history in `CHANGELOG.md`.
- Keep architecture notes in `ARCHITECTURE.md`.
- Keep collaboration rules in `CONTRIBUTING.md`.
- Put unreleased change details in `CHANGELOG.md`; keep the README focused on navigation and summary.

## Contact
- Telegram group: [CodexManager TG group](https://t.me/+OdpFa9GvjxhjMDhl)
