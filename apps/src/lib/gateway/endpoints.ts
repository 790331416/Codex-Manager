"use client";

import type { RuntimeMode } from "@/types";

const DEFAULT_GATEWAY_ADDR = "localhost:48760";

type ResolveGatewayOriginOptions = {
  browserOrigin?: string | null;
  runtimeMode?: RuntimeMode | string | null;
  serviceAddr?: string | null;
};

export function normalizeGatewayOrigin(value: string | null | undefined): string {
  const normalized = String(value || "").trim().replace(/\/+$/, "");
  if (!normalized) {
    return "";
  }
  if (/^https?:\/\//i.test(normalized)) {
    return normalized;
  }
  return `http://${normalized}`;
}

export function resolveGatewayOrigin({
  browserOrigin,
  runtimeMode,
  serviceAddr,
}: ResolveGatewayOriginOptions): string {
  const webOrigin =
    runtimeMode === "web-gateway" ? normalizeGatewayOrigin(browserOrigin) : "";
  if (webOrigin) {
    return webOrigin;
  }

  return normalizeGatewayOrigin(serviceAddr) || normalizeGatewayOrigin(DEFAULT_GATEWAY_ADDR);
}

export function buildOpenAiGatewayEndpoint(origin: string): string {
  const normalized = normalizeGatewayOrigin(origin);
  if (!normalized) {
    return "";
  }
  return /\/v1$/i.test(normalized) ? normalized : `${normalized}/v1`;
}

function buildGatewayRootEndpoint(origin: string): string {
  const normalized = normalizeGatewayOrigin(origin);
  if (!normalized) {
    return "";
  }
  return normalized.replace(/\/(?:v1|v1alpha|v1beta)$/i, "");
}

export function buildClaudeCodeGatewayEndpoint(origin: string): string {
  return buildGatewayRootEndpoint(origin);
}

export function buildGeminiGatewayEndpoint(origin: string): string {
  return buildGatewayRootEndpoint(origin);
}
