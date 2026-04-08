"use client";

import { usePathname, useRouter } from "next/navigation";
import { 
  LayoutDashboard, 
  Users, 
  Key, 
  Database,
  Puzzle,
  FileText, 
  Settings, 
  ChevronLeft, 
  ChevronRight
} from "lucide-react";
import { cn } from "@/lib/utils";
import { normalizeRoutePath } from "@/lib/utils/static-routes";
import { Button } from "@/components/ui/button";
import { useRuntimeCapabilities } from "@/hooks/useRuntimeCapabilities";
import { useAppStore } from "@/lib/store/useAppStore";
import { useI18n } from "@/lib/i18n/provider";
import {
  memo,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent,
} from "react";

const NAV_ITEMS = [
  { label: "仪表盘", href: "/", icon: LayoutDashboard },
  { label: "账号管理", href: "/accounts/", icon: Users },
  { label: "聚合API", href: "/aggregate-api/", icon: Database },
  { label: "平台密钥", href: "/apikeys/", icon: Key },
  { label: "插件中心", href: "/plugins/", icon: Puzzle },
  { label: "请求日志", href: "/logs/", icon: FileText },
  { label: "设置", href: "/settings/", icon: Settings },
];
const DESKTOP_NAVIGATION_FALLBACK_MS = 2_500;
const DESKTOP_ROUTE_WARMUP_TIMEOUT_MS = 4_000;

const NavItem = memo(({
  item,
  isActive,
  isSidebarOpen,
  onNavigate,
  onPrefetch,
  itemName,
}: {
  item: typeof NAV_ITEMS[0],
  isActive: boolean,
  isSidebarOpen: boolean,
  onNavigate: (href: string, event: MouseEvent<HTMLAnchorElement>) => void,
  onPrefetch: (href: string) => void,
  itemName: string,
}) => (
  <a
    href={item.href}
    onClick={(event) => onNavigate(item.href, event)}
    onPointerDown={() => onPrefetch(item.href)}
    onMouseEnter={() => onPrefetch(item.href)}
    onFocus={() => onPrefetch(item.href)}
    className={cn(
      "flex items-center gap-3 rounded-lg px-3 py-2 transition-all duration-200 hover:bg-accent hover:text-accent-foreground",
      isActive ? "bg-accent text-accent-foreground" : "text-muted-foreground"
    )}
    >
    <item.icon className="h-4 w-4 shrink-0" />
    {isSidebarOpen && <span className="text-sm truncate">{itemName}</span>}
  </a>
));

NavItem.displayName = "NavItem";

/**
 * 函数 `Sidebar`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-02
 *
 * # 参数
 * 无
 *
 * # 返回
 * 返回函数执行结果
 */
export function Sidebar() {
  const { t } = useI18n();
  const pathname = usePathname();
  const router = useRouter();
  const {
    isSidebarOpen,
    toggleSidebar,
    setPendingRoutePath,
  } = useAppStore();
  const { isDesktopRuntime } = useRuntimeCapabilities();
  const normalizedPathname = normalizeRoutePath(pathname);
  const [optimisticPathname, setOptimisticPathname] = useState<string | null>(null);
  const desktopNavigationFallbackTimerRef = useRef<number | null>(null);
  const prefetchedRouteSetRef = useRef<Set<string>>(new Set());
  const activePathname = optimisticPathname || normalizedPathname;

  const prefetchRoute = useCallback(
    (href: string) => {
      const normalizedHref = normalizeRoutePath(href);
      if (!normalizedHref || normalizedHref === normalizedPathname) {
        return;
      }
      if (prefetchedRouteSetRef.current.has(normalizedHref)) {
        return;
      }
      prefetchedRouteSetRef.current.add(normalizedHref);
      router.prefetch(href);
    },
    [normalizedPathname, router],
  );

  const handleNavigate = useCallback(
    (href: string, event: MouseEvent<HTMLAnchorElement>) => {
      const nextPath = normalizeRoutePath(href);
      if (nextPath === normalizedPathname) {
        event.preventDefault();
        setPendingRoutePath("");
        return;
      }

      event.preventDefault();
      setOptimisticPathname(nextPath);
      if (isDesktopRuntime) {
        setPendingRoutePath(nextPath);
      }
      if (isDesktopRuntime) {
        const currentPath = normalizeRoutePath(window.location.pathname);

        router.push(href);

        if (process.env.NODE_ENV === "production") {
          if (desktopNavigationFallbackTimerRef.current !== null) {
            window.clearTimeout(desktopNavigationFallbackTimerRef.current);
          }

          desktopNavigationFallbackTimerRef.current = window.setTimeout(() => {
            desktopNavigationFallbackTimerRef.current = null;
            if (normalizeRoutePath(window.location.pathname) === currentPath) {
              window.location.assign(href);
            }
          }, DESKTOP_NAVIGATION_FALLBACK_MS);
        }
        return;
      }

      router.push(href);
    },
    [isDesktopRuntime, normalizedPathname, router, setPendingRoutePath],
  );

  useEffect(() => {
    setOptimisticPathname(null);
    if (desktopNavigationFallbackTimerRef.current !== null) {
      window.clearTimeout(desktopNavigationFallbackTimerRef.current);
      desktopNavigationFallbackTimerRef.current = null;
    }
  }, [normalizedPathname]);

  useEffect(() => {
    return () => {
      if (desktopNavigationFallbackTimerRef.current !== null) {
        window.clearTimeout(desktopNavigationFallbackTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const runtime = globalThis as typeof globalThis & {
      requestIdleCallback?: (
        callback: IdleRequestCallback,
        options?: IdleRequestOptions,
      ) => number;
      cancelIdleCallback?: (handle: number) => void;
    };
    const controllers: AbortController[] = [];

    /**
     * 函数 `warmRouteDocument`
     *
     * 作者: gaohongshun
     *
     * 时间: 2026-04-02
     *
     * # 参数
     * - href: 参数 href
     *
     * # 返回
     * 返回函数执行结果
     */
    const warmRouteDocument = async (href: string) => {
      const normalizedHref = normalizeRoutePath(href);
      if (!normalizedHref || normalizedHref === normalizedPathname) {
        return;
      }

      const controller = new AbortController();
      const timeoutId = window.setTimeout(
        () => controller.abort(),
        DESKTOP_ROUTE_WARMUP_TIMEOUT_MS
      );
      controllers.push(controller);
      try {
        await fetch(href, {
          method: "GET",
          credentials: "same-origin",
          cache: "force-cache",
          signal: controller.signal,
          headers: {
            "x-codexmanager-route-warmup": "1",
          },
        });
      } catch {
        // 中文注释：路由文档预热失败时静默回退，不影响正常导航。
      } finally {
        window.clearTimeout(timeoutId);
        const index = controllers.indexOf(controller);
        if (index >= 0) {
          controllers.splice(index, 1);
        }
      }
    };

    /**
     * 函数 `prefetchRoutes`
     *
     * 作者: gaohongshun
     *
     * 时间: 2026-04-02
     *
     * # 参数
     * 无
     *
     * # 返回
     * 返回函数执行结果
     */
    const prefetchRoutes = () => {
      for (const item of NAV_ITEMS) {
        prefetchRoute(item.href);
        if (isDesktopRuntime) {
          void warmRouteDocument(item.href);
        }
      }
    };

    if (isDesktopRuntime) {
      return () => {
        for (const controller of controllers) {
          controller.abort();
        }
      };
    }

    if (runtime.requestIdleCallback) {
      const idleId = runtime.requestIdleCallback(() => prefetchRoutes(), {
        timeout: 1200,
      });
      return () => {
        runtime.cancelIdleCallback?.(idleId);
        for (const controller of controllers) {
          controller.abort();
        }
      };
    }

    const timer = globalThis.setTimeout(prefetchRoutes, 120);
    return () => {
      globalThis.clearTimeout(timer);
      for (const controller of controllers) {
        controller.abort();
      }
    };
  }, [isDesktopRuntime, normalizedPathname, prefetchRoute]);

  const renderedItems = useMemo(() => 
    NAV_ITEMS.map((item) => (
      <NavItem 
        key={item.href} 
        item={item} 
        itemName={t(item.label)}
        isActive={normalizeRoutePath(item.href) === activePathname} 
        isSidebarOpen={isSidebarOpen}
        onNavigate={handleNavigate}
        onPrefetch={prefetchRoute}
      />
    )),
    [activePathname, handleNavigate, isSidebarOpen, prefetchRoute]
  );

  return (
    <div
      className={cn(
        "relative z-20 flex shrink-0 flex-col glass-sidebar transition-[width] duration-300 ease-in-out",
        isSidebarOpen ? "w-64" : "w-16"
      )}
    >
      <div className="flex h-16 items-center px-4 border-b shrink-0">
        <div className="flex items-center gap-2 overflow-hidden">
          <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-primary text-primary-foreground">
            <span className="text-sm font-bold">CM</span>
          </div>
          {isSidebarOpen && (
            <div className="flex flex-col overflow-hidden animate-in fade-in duration-300">
              <span className="text-sm font-bold truncate">CodexManager</span>
              <span className="text-xs text-muted-foreground truncate opacity-70">{t("账号池 · 用量管理")}</span>
            </div>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto py-4">
        <nav className="grid gap-1 px-2">
          {renderedItems}
        </nav>
      </div>

      <div className="border-t p-2 shrink-0">
        <Button
          variant="ghost"
          size="icon"
          className="w-full justify-start gap-3 px-3 h-10"
          onClick={toggleSidebar}
        >
          {isSidebarOpen ? (
            <>
              <ChevronLeft className="h-4 w-4 shrink-0" />
              <span className="text-sm">{t("收起侧边栏")}</span>
            </>
          ) : (
            <ChevronRight className="h-4 w-4 shrink-0" />
          )}
        </Button>
      </div>
    </div>
  );
}
