import { isTauriRuntime } from "./transport";

export const USAGE_REFRESH_COMPLETED_EVENT = "usage-refresh-completed";

export interface UsageRefreshCompletedPayload {
  source?: string;
  processed?: number;
  total?: number;
  completedAt?: number;
  completed_at?: number;
}

export type UsageRefreshCompletedHandler = (
  payload: UsageRefreshCompletedPayload
) => void;

type Unlisten = () => void;

function readUsageRefreshEventPayload(event: Event): UsageRefreshCompletedPayload {
  if (event instanceof CustomEvent && typeof event.detail === "object" && event.detail) {
    return event.detail as UsageRefreshCompletedPayload;
  }
  return {};
}

export async function listenUsageRefreshCompleted(
  handler: UsageRefreshCompletedHandler
): Promise<Unlisten> {
  if (typeof window === "undefined") {
    return () => {};
  }

  const handleWindowEvent = (event: Event) => {
    handler(readUsageRefreshEventPayload(event));
  };
  window.addEventListener(USAGE_REFRESH_COMPLETED_EVENT, handleWindowEvent);

  let unlistenTauri: Unlisten | null = null;
  if (isTauriRuntime()) {
    const { listen } = await import("@tauri-apps/api/event");
    unlistenTauri = await listen<UsageRefreshCompletedPayload>(
      USAGE_REFRESH_COMPLETED_EVENT,
      (event) => {
        handler(event.payload || {});
      },
    );
  }

  return () => {
    window.removeEventListener(USAGE_REFRESH_COMPLETED_EVENT, handleWindowEvent);
    unlistenTauri?.();
  };
}
