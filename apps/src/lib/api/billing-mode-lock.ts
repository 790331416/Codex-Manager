export interface BillingModeLockView {
  accountModeLocked: boolean;
  distributionLocked: boolean;
  reasons: string[];
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asBoolean(value: unknown, fallback = false): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

export function readBillingModeLock(value: unknown): BillingModeLockView {
  const source = asRecord(value);
  const rawReasons = Array.isArray(source.reasons) ? source.reasons : [];
  return {
    accountModeLocked: asBoolean(source.accountModeLocked, false),
    distributionLocked: asBoolean(source.distributionLocked, false),
    reasons: rawReasons.map((item) => asString(item)).filter(Boolean),
  };
}
