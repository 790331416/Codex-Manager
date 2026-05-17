"use client";

import { useEffect, useState } from "react";
import { ChevronRight, ShieldAlert } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useI18n } from "@/lib/i18n/provider";

const DISCLAIMER_ITEMS = [
  "本项目仅用于学习与开发目的。",
  "使用者必须遵守相关平台的服务条款，例如 OpenAI、Anthropic。",
  "作者不提供或分发任何账号、API Key 或代理服务，也不对本软件的具体使用方式负责。",
  "请勿使用本项目绕过速率限制或服务限制。",
] as const;

const DISCLAIMER_ROTATE_INTERVAL_MS = 3200;

/**
 * 函数 `DisclaimerTicker`
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
export function DisclaimerTicker() {
  const { t } = useI18n();
  const [activeIndex, setActiveIndex] = useState(0);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    const timer = window.setInterval(() => {
      setActiveIndex((current) => (current + 1) % DISCLAIMER_ITEMS.length);
    }, DISCLAIMER_ROTATE_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <>
      <Button
        type="button"
        variant="outline"
        className="group flex w-full max-w-[220px] items-center gap-2 rounded-full border border-border/60 bg-card/35 px-2 py-1.5 text-left shadow-sm transition-colors hover:bg-card/55 xl:max-w-[260px] 2xl:max-w-[340px]"
        onClick={() => setOpen(true)}
        title={t("免责声明")}
      >
        <div className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-muted/70 text-muted-foreground">
          <ShieldAlert className="h-3 w-3" />
        </div>
        <div className="min-w-0 flex-1 leading-none">
          <div className="mb-0.5 text-[10px] font-medium text-muted-foreground/80">
            {t("免责声明")}
          </div>
          <div className="truncate text-[11px] text-muted-foreground/90">
            {t(DISCLAIMER_ITEMS[activeIndex])}
          </div>
        </div>
        <div className="flex shrink-0 items-center gap-1 whitespace-nowrap text-[10px] text-muted-foreground/70 transition-colors group-hover:text-muted-foreground">
          <span className="hidden xl:inline">{t("详情")}</span>
          <ChevronRight data-icon="inline-end" />
        </div>
      </Button>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-xl">
          <DialogHeader>
            <DialogTitle>{t("免责声明")}</DialogTitle>
            <DialogDescription>
              {t("以下内容与 README 保持一致，适合作为使用前的统一提示。")}
            </DialogDescription>
          </DialogHeader>
          <ul className="space-y-2 pl-5 text-sm leading-6 text-muted-foreground">
            {DISCLAIMER_ITEMS.map((item) => (
              <li key={item}>{t(item)}</li>
            ))}
          </ul>
          <DialogFooter>
            <Button onClick={() => setOpen(false)}>{t("我知道了")}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
