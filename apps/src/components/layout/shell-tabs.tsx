"use client";

import { X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getTopLevelRouteLabel } from "@/lib/app-shell/top-level-routes";
import { useAppStore } from "@/lib/store/useAppStore";
import { cn } from "@/lib/utils";
import { useI18n } from "@/lib/i18n/provider";
import { resolveSessionRole, useAppSession } from "@/hooks/useAppSession";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";

const ROOT_ROUTE_PATH = "/";

export function ShellTabs() {
  const { t } = useI18n();
  const currentShellPath = useAppStore((state) => state.currentShellPath);
  const openShellTabs = useAppStore((state) => state.openShellTabs);
  const navigateShellPath = useAppStore((state) => state.navigateShellPath);
  const closeShellTab = useAppStore((state) => state.closeShellTab);
  const { isDesktopRuntime } = useRuntimeCapabilities();
  const { data: session, isLoading: isSessionLoading } = useAppSession();
  const role = resolveSessionRole(session, isSessionLoading, isDesktopRuntime);
  const routeAccess = { role, mode: session?.mode ?? null };

  if (openShellTabs.length <= 1) {
    return null;
  }

  return (
    <div className="sticky top-0 z-10 -mx-6 -mt-6 border-b bg-background px-6 py-3">
      <div className="flex flex-wrap items-center gap-2">
        {openShellTabs.map((path) => {
          const isActive = path === currentShellPath;
          const label = t(getTopLevelRouteLabel(path, routeAccess));
          const canClose = path !== ROOT_ROUTE_PATH;

          return (
            <div
              key={path}
              className={cn(
                "group flex items-center rounded-full border px-3 py-1.5 text-sm transition-colors duration-200",
                isActive
                  ? "border-primary/40 bg-primary/10 text-foreground shadow-sm"
                  : "border-border/70 bg-card/60 text-muted-foreground hover:bg-accent/70 hover:text-foreground",
              )}
            >
              <Button
                type="button"
                variant="ghost"
                className="min-w-0 truncate"
                onClick={() => navigateShellPath(path)}
              >
                {label}
              </Button>

              {canClose ? (
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-xs"
                  aria-label={t("关闭 {label}", { label })}
                  className={cn(
                    "ml-2 rounded-full transition-colors duration-150",
                    isActive
                      ? "text-foreground/70 hover:bg-primary/15 hover:text-foreground"
                      : "text-muted-foreground/70 hover:bg-accent hover:text-foreground",
                  )}
                  onClick={(event) => {
                    event.stopPropagation();
                    closeShellTab(path);
                  }}
                >
                  <X />
                </Button>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}
