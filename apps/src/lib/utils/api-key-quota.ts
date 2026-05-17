"use client";

export type QuotaLimitUnit = "k" | "m";

export const QUOTA_LIMIT_REFERENCE_PRICE_USD_PER_1K_TOKENS = 0.01;

const QUOTA_LIMIT_UNIT_MULTIPLIERS: Record<QuotaLimitUnit, number> = {
  k: 1_000,
  m: 1_000_000,
};

function trimTrailingZeros(value: string): string {
  return value.replace(/\.0+$/, "").replace(/(\.\d*[1-9])0+$/, "$1");
}

export function sanitizeQuotaLimitValue(value: string): string {
  const normalized = value.replace(/[^\d.]/g, "");
  const parts = normalized.split(".");
  if (parts.length <= 1) {
    return normalized;
  }
  return `${parts[0]}.${parts.slice(1).join("")}`;
}

export function parseQuotaLimitTokens(
  value: string,
  unit: QuotaLimitUnit,
): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  const multiplier = QUOTA_LIMIT_UNIT_MULTIPLIERS[unit];
  const tokens = Math.floor(parsed * multiplier);
  return tokens > 0 ? tokens : null;
}

export function resolveQuotaLimitUnit(tokens?: number | null): QuotaLimitUnit {
  const normalized =
    typeof tokens === "number" && Number.isFinite(tokens) ? tokens : 0;
  return normalized >= QUOTA_LIMIT_UNIT_MULTIPLIERS.m ? "m" : "k";
}

export function formatQuotaLimitValue(
  tokens: number | null | undefined,
  unit = resolveQuotaLimitUnit(tokens),
): string {
  if (typeof tokens !== "number" || !Number.isFinite(tokens) || tokens <= 0) {
    return "";
  }
  const multiplier = QUOTA_LIMIT_UNIT_MULTIPLIERS[unit];
  const fractionDigits = unit === "m" ? 6 : 3;
  return trimTrailingZeros((tokens / multiplier).toFixed(fractionDigits));
}

export function estimateQuotaLimitUsd(tokens: number | null | undefined): number {
  if (typeof tokens !== "number" || !Number.isFinite(tokens) || tokens <= 0) {
    return 0;
  }
  return (tokens / 1_000) * QUOTA_LIMIT_REFERENCE_PRICE_USD_PER_1K_TOKENS;
}

export function formatQuotaLimitUsd(value: number | null | undefined): string {
  const normalized =
    typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: normalized > 0 && normalized < 1 ? 4 : 2,
  }).format(normalized);
}
