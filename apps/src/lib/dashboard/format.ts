"use client";

import { formatCompactNumber } from "@/lib/utils/usage";

export function formatPercent(value: number | null | undefined): string {
  return value == null ? "--" : `${Math.max(0, Math.round(value))}%`;
}

export function formatCompactTokenAmount(value: number | null | undefined): string {
  const normalized =
    typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
  if (normalized < 1000) {
    return normalized.toLocaleString("zh-CN", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }
  return formatCompactNumber(normalized, "0.00", 2, true);
}
