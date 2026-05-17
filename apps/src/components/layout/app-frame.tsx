"use client";

import { useEffect } from "react";
import { usePathname } from "next/navigation";
import { Header } from "@/components/layout/header";
import { PageKeepAliveViewport } from "@/components/layout/page-keep-alive-viewport";
import { RouteTransitionOverlay } from "@/components/layout/route-transition-overlay";
import { Sidebar } from "@/components/layout/sidebar";
import { normalizeRoutePath } from "@/lib/utils/static-routes";

const TRAY_PREVIEW_PATH = "/tray-preview";

export function isTrayPreviewPath(pathname: string): boolean {
  return normalizeRoutePath(pathname) === TRAY_PREVIEW_PATH;
}

export function AppFrame({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const isTrayPreview = isTrayPreviewPath(pathname);

  useEffect(() => {
    document.documentElement.classList.toggle("tray-preview-mode", isTrayPreview);
    document.body.classList.remove("tray-preview-mode");
    return () => {
      document.documentElement.classList.remove("tray-preview-mode");
      document.body.classList.remove("tray-preview-mode");
    };
  }, [isTrayPreview]);

  if (isTrayPreview) {
    return <main className="h-screen overflow-hidden bg-transparent">{children}</main>;
  }

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar />
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <Header />
        <main className="relative min-w-0 flex-1 overflow-y-auto p-6 no-scrollbar">
          <RouteTransitionOverlay />
          <PageKeepAliveViewport initialChildren={children} />
        </main>
      </div>
    </div>
  );
}
