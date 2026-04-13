import type { RequestOptions } from "../utils/request";
import { unwrapRpcPayload } from "./transport-errors";

export type JsonRpcFetcher = (
  url: string,
  init?: RequestInit,
  options?: RequestOptions
) => Promise<Response>;

export function buildJsonRpcRequestBody(
  method: string,
  params: Record<string, unknown> = {}
): string {
  return JSON.stringify({
    jsonrpc: "2.0",
    id: Date.now(),
    method,
    params,
  });
}

export async function postJsonRpc<T>(
  fetcher: JsonRpcFetcher,
  url: string,
  rpcMethod: string,
  params: Record<string, unknown> = {},
  options: RequestOptions = {}
): Promise<T> {
  const response = await fetcher(
    url,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: buildJsonRpcRequestBody(rpcMethod, params),
    },
    options
  );

  if (!response.ok) {
    throw new Error(`RPC 请求失败（HTTP ${response.status}）`);
  }

  return unwrapRpcPayload<T>((await response.json()) as unknown);
}
