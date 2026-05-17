"use client";

import { useQuery } from "@tanstack/react-query";
import { useDeferredDesktopActivation } from "@/hooks/useDeferredDesktopActivation";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { useLocalDayRange } from "@/hooks/useLocalDayRange";
import { dashboardClient } from "@/lib/api/dashboard-client";
import { useAppStore } from "@/lib/store/useAppStore";
import type { MemberDashboardSummary } from "@/types";

export const MEMBER_DASHBOARD_SUMMARY_QUERY_KEY = [
  "dashboard",
  "member-summary",
] as const;

export function useMemberDashboardSummary(enabled = true) {
  const serviceStatus = useAppStore((state) => state.serviceStatus);
  const localDayRange = useLocalDayRange();
  const isPageActive = useDesktopPageActive("/");
  const isServiceReady = serviceStatus.connected;
  const isQueryEnabled = useDeferredDesktopActivation(
    enabled && isServiceReady && isPageActive,
  );

  const query = useQuery<MemberDashboardSummary>({
    queryKey: [
      ...MEMBER_DASHBOARD_SUMMARY_QUERY_KEY,
      serviceStatus.addr,
      localDayRange.dayStartTs,
    ],
    queryFn: () =>
      dashboardClient.getMemberSummary({
        dayStartTs: localDayRange.dayStartTs,
        dayEndTs: localDayRange.dayEndTs,
      }),
    enabled: isQueryEnabled,
    retry: 1,
    staleTime: 30_000,
  });

  return {
    ...query,
    isServiceReady,
  };
}
