export const CCSWITCH_PROVIDER_IMPORT_BASE = "ccswitch://v1/import";

export interface CcSwitchProviderImportOptions {
  app?: "claude" | "codex" | "gemini" | "opencode" | "openclaw";
  name: string;
  endpoint: string;
  apiKey: string;
  model?: string | null;
  homepage?: string | null;
  notes?: string | null;
  enabled?: boolean;
}

function normalizeText(value: string | null | undefined): string {
  return String(value || "").trim();
}

function replaceLoopbackHost(host: string): string {
  return host === "0.0.0.0" || host === "::" || host === "[::]"
    ? "localhost"
    : host;
}

function appendV1Path(url: URL): string {
  const path = url.pathname.replace(/\/+$/, "");
  url.pathname = path.endsWith("/v1") ? path : `${path || ""}/v1`;
  url.search = "";
  url.hash = "";
  return url.toString().replace(/\/$/, "");
}

export function normalizeCodexManagerGatewayEndpoint(serviceAddr?: string | null): string {
  const raw = normalizeText(serviceAddr) || "localhost:48760";
  const value = raw.replace(/^https?:\/\//i, "");
  const target = value.split("/")[0] || "localhost:48760";

  try {
    const url = new URL(`http://${target}`);
    url.hostname = replaceLoopbackHost(url.hostname);
    return appendV1Path(url);
  } catch {
    return "http://localhost:48760/v1";
  }
}

export function buildCcSwitchProviderName(name?: string | null, id?: string | null): string {
  const label = normalizeText(name) || normalizeText(id) || "Platform Key";
  return label.toLowerCase().startsWith("codexmanager")
    ? label
    : `CodexManager - ${label}`;
}

export function buildCcSwitchProviderImportUrl(
  options: CcSwitchProviderImportOptions,
): string {
  const params = new URLSearchParams({
    resource: "provider",
    app: options.app || "codex",
    name: normalizeText(options.name) || "CodexManager",
    endpoint: normalizeText(options.endpoint),
    apiKey: normalizeText(options.apiKey),
  });

  const model = normalizeText(options.model);
  const homepage = normalizeText(options.homepage);
  const notes = normalizeText(options.notes);

  if (model) params.set("model", model);
  if (homepage) params.set("homepage", homepage);
  if (notes) params.set("notes", notes);
  if (typeof options.enabled === "boolean") {
    params.set("enabled", String(options.enabled));
  }

  return `${CCSWITCH_PROVIDER_IMPORT_BASE}?${params.toString()}`;
}
