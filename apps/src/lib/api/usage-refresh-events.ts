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

function readUsageRefreshMessagePayload(event: MessageEvent): UsageRefreshCompletedPayload {
  if (typeof event.data !== "string" || !event.data.trim()) {
    return {};
  }
  try {
    const payload = JSON.parse(event.data);
    return typeof payload === "object" && payload
      ? (payload as UsageRefreshCompletedPayload)
      : {};
  } catch {
    return {};
  }
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

  let eventSource: EventSource | null = null;
  let handleEventSourceEvent: ((event: MessageEvent) => void) | null = null;
  if (
    !isTauriRuntime() &&
    typeof EventSource !== "undefined" &&
    window.location.protocol.startsWith("http")
  ) {
    eventSource = new EventSource("/api/events/usage-refresh");
    handleEventSourceEvent = (event: MessageEvent) => {
      handler(readUsageRefreshMessagePayload(event));
    };
    eventSource.addEventListener(
      USAGE_REFRESH_COMPLETED_EVENT,
      handleEventSourceEvent as EventListener
    );
  }

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
    if (eventSource && handleEventSourceEvent) {
      eventSource.removeEventListener(
        USAGE_REFRESH_COMPLETED_EVENT,
        handleEventSourceEvent as EventListener
      );
    }
    eventSource?.close();
    unlistenTauri?.();
  };
}
