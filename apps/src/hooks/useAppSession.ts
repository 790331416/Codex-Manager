"use client";

import { useQuery } from "@tanstack/react-query";
import { appClient } from "@/lib/api/app-client";
import type { AppRole, AppSessionResult } from "@/types";

export const APP_SESSION_QUERY_KEY = ["account-manager", "session", "current"] as const;

export function isAdminRole(role: AppRole | string | null | undefined): boolean {
  return role === "admin" || role === "system_admin";
}

export function resolveSessionRole(
  session: AppSessionResult | null | undefined,
  isLoading = false,
  forceSystemAdmin = false,
): AppRole {
  if (forceSystemAdmin) return "system_admin";
  return session?.role ?? (isLoading ? "system_admin" : "member");
}

export function useAppSession() {
  return useQuery<AppSessionResult>({
    queryKey: APP_SESSION_QUERY_KEY,
    queryFn: () => appClient.getCurrentSession(),
    staleTime: 30_000,
    retry: 1,
  });
}
