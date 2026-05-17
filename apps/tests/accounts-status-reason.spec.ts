import { expect, test } from "@playwright/test";

const SETTINGS_SNAPSHOT = {
  updateAutoCheck: true,
  closeToTrayOnClose: false,
  closeToTraySupported: false,
  lowTransparency: false,
  lightweightModeOnCloseToTray: false,
  codexCliGuideDismissed: true,
  webAccessPasswordConfigured: false,
  locale: "zh-CN",
  localeOptions: ["zh-CN", "en"],
  serviceAddr: "localhost:48760",
  serviceListenMode: "loopback",
  serviceListenModeOptions: ["loopback", "all_interfaces"],
  routeStrategy: "ordered",
  routeStrategyOptions: ["ordered", "balanced"],
  freeAccountMaxModel: "auto",
  freeAccountMaxModelOptions: ["auto", "gpt-5"],
  modelForwardRules: "",
  accountMaxInflight: 1,
  gatewayOriginator: "codex-cli",
  gatewayOriginatorDefault: "codex-cli",
  gatewayUserAgentVersion: "1.0.0",
  gatewayUserAgentVersionDefault: "1.0.0",
  gatewayResidencyRequirement: "",
  gatewayResidencyRequirementOptions: ["", "us"],
  pluginMarketMode: "builtin",
  pluginMarketSourceUrl: "",
  upstreamProxyUrl: "",
  upstreamStreamTimeoutMs: 600000,
  upstreamTotalTimeoutMs: 0,
  sseKeepaliveIntervalMs: 15000,
  backgroundTasks: {
    usagePollingEnabled: true,
    usagePollIntervalSecs: 600,
    gatewayKeepaliveEnabled: true,
    gatewayKeepaliveIntervalSecs: 180,
    tokenRefreshPollingEnabled: true,
    tokenRefreshPollIntervalSecs: 60,
    usageRefreshWorkers: 4,
    httpWorkerFactor: 4,
    httpWorkerMin: 8,
    httpStreamWorkerFactor: 1,
    httpStreamWorkerMin: 2,
  },
  envOverrides: {},
  envOverrideCatalog: [],
  envOverrideReservedKeys: [],
  envOverrideUnsupportedKeys: [],
  theme: "tech",
  appearancePreset: "classic",
};

test("accounts page shows unavailable status reason and raw reason code", async ({
  page,
}) => {
  await page.route("**/api/runtime**", async (route) => {
    await route.fulfill({
      contentType: "application/json; charset=utf-8",
      body: JSON.stringify({
        mode: "web-gateway",
        rpcBaseUrl: "/api/rpc",
        canManageService: false,
        canSelfUpdate: false,
        canCloseToTray: false,
        canOpenLocalDir: false,
        canUseBrowserFileImport: true,
        canUseBrowserDownloadExport: true,
      }),
    });
  });

  await page.route("**/api/rpc**", async (route) => {
    const payload = route.request().postDataJSON();
    const method = typeof payload?.method === "string" ? payload.method : "";
    const id = payload?.id ?? 1;

    const ok = (result: unknown) =>
      route.fulfill({
        contentType: "application/json; charset=utf-8",
        body: JSON.stringify({
          jsonrpc: "2.0",
          id,
          result,
        }),
      });

    if (method === "appSettings/get") {
      await ok(SETTINGS_SNAPSHOT);
      return;
    }
    if (method === "initialize") {
      await ok({
        version: "0.3.1",
        userAgent: "codex_cli_rs/0.1.19",
        codexHome: "/tmp/.codex",
        platformFamily: "unix",
        platformOs: "macos",
      });
      return;
    }
    if (method === "accountManager/session/current") {
      await ok({
        mode: "none",
        currentUser: null,
        role: "system_admin",
        permissions: ["system:admin"],
        distributionEnabled: false,
      });
      return;
    }
    if (method === "account/list") {
      await ok({
        items: [
          {
            id: "acct-refresh-reused",
            label: "angiemooreja@hotmail.com",
            plan_type: "plus",
            status: "unavailable",
            status_reason: "refresh_token_invalid:refresh_token_reused",
            sort: 0,
          },
        ],
        total: 1,
        page: 1,
        pageSize: 20,
      });
      return;
    }
    if (method === "account/usage/list") {
      await ok([]);
      return;
    }

    await route.fulfill({
      status: 500,
      contentType: "application/json; charset=utf-8",
      body: JSON.stringify({
        jsonrpc: "2.0",
        id,
        error: {
          code: -32000,
          message: `Unhandled RPC method in test: ${method}`,
        },
      }),
    });
  });

  await page.goto("/accounts/");

  await expect(page.getByRole("heading", { name: "OpenAI 账号池" })).toBeVisible();
  const reasonText = page.getByText("Refresh Token 已被重复使用，需要重新登录");
  await expect(reasonText).toBeVisible();

  await reasonText.hover();
  await expect(
    page.getByText("refresh_token_invalid:refresh_token_reused"),
  ).toBeVisible();
});
