import type {
  ChatgptAuthTokensRefreshAllItem,
  ChatgptAuthTokensRefreshAllResult,
  ChatgptAuthTokensRefreshResult,
  CurrentAccessTokenAccount,
  CurrentAccessTokenAccountReadResult,
  LoginStatusResult,
} from "../../types";

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function readStringField(payload: unknown, key: string, fallback = ""): string {
  const source = asRecord(payload);
  const value = source?.[key];
  return typeof value === "string" ? value.trim() : fallback;
}

function readBooleanField(payload: unknown, key: string, fallback = false): boolean {
  const source = asRecord(payload);
  const value = source?.[key];
  return typeof value === "boolean" ? value : fallback;
}

function readNullableBooleanField(payload: unknown, key: string): boolean | null {
  const source = asRecord(payload);
  const value = source?.[key];
  return typeof value === "boolean" ? value : null;
}

function readNullableNumberField(payload: unknown, key: string): number | null {
  const source = asRecord(payload);
  const value = source?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readNumberField(payload: unknown, key: string, fallback = 0): number {
  const source = asRecord(payload);
  const value = source?.[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function readNullableStringField(payload: unknown, key: string): string | null {
  const value = readStringField(payload, key);
  return value ? value : null;
}

export function readLoginStatusResult(payload: unknown): LoginStatusResult {
  return {
    status: readStringField(payload, "status"),
    error: readStringField(payload, "error"),
  };
}

export function readCurrentAccessTokenAccount(
  payload: unknown
): CurrentAccessTokenAccount | null {
  const source = asRecord(payload);
  if (!source) {
    return null;
  }

  return {
    type: readStringField(source, "type"),
    accountId: readStringField(source, "accountId"),
    email: readStringField(source, "email"),
    planType: readStringField(source, "planType"),
    planTypeRaw: readNullableStringField(source, "planTypeRaw"),
    hasSubscription: readNullableBooleanField(source, "hasSubscription"),
    subscriptionPlan: readNullableStringField(source, "subscriptionPlan"),
    subscriptionExpiresAt: readNullableNumberField(source, "subscriptionExpiresAt"),
    subscriptionRenewsAt: readNullableNumberField(source, "subscriptionRenewsAt"),
    chatgptAccountId: readNullableStringField(source, "chatgptAccountId"),
    workspaceId: readNullableStringField(source, "workspaceId"),
    status: readStringField(source, "status"),
  };
}

export function readCurrentAccessTokenAccountReadResult(
  payload: unknown
): CurrentAccessTokenAccountReadResult {
  const source = asRecord(payload);
  return {
    account: readCurrentAccessTokenAccount(source?.account),
    requiresOpenaiAuth: readBooleanField(payload, "requiresOpenaiAuth"),
  };
}

export function readChatgptAuthTokensRefreshResult(
  payload: unknown
): ChatgptAuthTokensRefreshResult {
  return {
    accessToken: readStringField(payload, "accessToken"),
    chatgptAccountId: readStringField(payload, "chatgptAccountId"),
    chatgptPlanType: readNullableStringField(payload, "chatgptPlanType"),
    hasSubscription: readNullableBooleanField(payload, "hasSubscription"),
    subscriptionPlan: readNullableStringField(payload, "subscriptionPlan"),
    subscriptionExpiresAt: readNullableNumberField(payload, "subscriptionExpiresAt"),
    subscriptionRenewsAt: readNullableNumberField(payload, "subscriptionRenewsAt"),
  };
}

function readChatgptAuthTokensRefreshAllItem(
  payload: unknown
): ChatgptAuthTokensRefreshAllItem {
  return {
    accountId: readStringField(payload, "accountId"),
    accountName: readStringField(payload, "accountName"),
    ok: readBooleanField(payload, "ok"),
    message: readNullableStringField(payload, "message"),
  };
}

export function readChatgptAuthTokensRefreshAllResult(
  payload: unknown
): ChatgptAuthTokensRefreshAllResult {
  const source = asRecord(payload);
  const rawResults = Array.isArray(source?.results) ? source.results : [];
  return {
    requested: readNumberField(payload, "requested"),
    succeeded: readNumberField(payload, "succeeded"),
    failed: readNumberField(payload, "failed"),
    skipped: readNumberField(payload, "skipped"),
    results: rawResults.map(readChatgptAuthTokensRefreshAllItem),
  };
}
