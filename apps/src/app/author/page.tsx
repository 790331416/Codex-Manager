"use client";

import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { appClient } from "@/lib/api/app-client";
import { useI18n } from "@/lib/i18n/provider";
import { ExternalLink, HeartHandshake, Info, Send } from "lucide-react";
import { toast } from "sonner";

const AUTHOR_WECHAT_ID = "ProsperGao";
const AUTHOR_TELEGRAM_GROUP_URL = "https://t.me/+OdpFa9GvjxhjMDhl";
const AUTHOR_SUPPORT_IMAGES = [
  {
    key: "alipay",
    title: "支付宝赞助码",
    description: "如果这个项目帮你省了时间，可以请作者喝杯咖啡。",
    src: "/author-alipay.jpg",
  },
  {
    key: "wechat-pay",
    title: "微信赞助码",
    description: "项目持续维护、修问题和做适配，欢迎随缘支持。",
    src: "/author-wechat-pay.jpg",
  },
] as const;

export default function AuthorPage() {
  const { t } = useI18n();
  const handleOpenTelegramGroup = async () => {
    try {
      await appClient.openInBrowser(AUTHOR_TELEGRAM_GROUP_URL);
    } catch (error) {
      toast.error(
        t("打开 TG 群聊失败：{message}", {
          message: error instanceof Error ? error.message : t("未知错误"),
        }),
      );
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-2 text-primary">
          <Info className="h-4 w-4" />
          <span className="text-xs font-medium uppercase tracking-[0.24em]">
            {t("About Author")}
          </span>
        </div>
        <div>
          <h2 className="text-xl font-bold tracking-tight">{t("关于作者")}</h2>
        </div>
      </div>

      <Card className="glass-card border-none shadow-md">
        <CardHeader className="gap-3">
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <HeartHandshake className="h-4 w-4 text-primary" />
              <CardTitle className="text-base">{t("赞助支持")}</CardTitle>
            </div>
            <Badge variant="secondary">{t("Support")}</Badge>
          </div>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2">
          {AUTHOR_SUPPORT_IMAGES.map((item) => (
            <div
              key={item.key}
              className="rounded-3xl border border-border/50 bg-background/40 p-5"
            >
              <div className="space-y-1">
                <h3 className="text-sm font-semibold text-foreground">
                  {t(item.title)}
                </h3>
                <p className="text-xs leading-6 text-muted-foreground">
                  {t(item.description)}
                </p>
              </div>
              <div className="mt-4 overflow-hidden rounded-3xl border border-border/50 bg-white p-3">
                <img
                  src={item.src}
                  alt={item.title}
                  className="mx-auto aspect-square w-full max-w-[220px] rounded-2xl object-cover"
                />
              </div>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card className="glass-card border-none shadow-md">
        <CardHeader className="gap-3">
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <Send className="h-4 w-4 text-primary" />
              <CardTitle className="text-base">{t("联系方式")}</CardTitle>
            </div>
            <Badge variant="secondary">{t("持续维护中")}</Badge>
          </div>
          <CardDescription>
            {t("需要反馈问题或进一步沟通时，可以通过微信或 TG 群联系作者。")}
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2">
          <div className="rounded-3xl border border-border/50 bg-background/40 p-5">
            <p className="text-xs uppercase tracking-[0.2em] text-muted-foreground">
              {t("WeChat")}
            </p>
            <p className="mt-3 text-2xl font-semibold tracking-tight text-foreground">
              {AUTHOR_WECHAT_ID}
            </p>
            <p className="mt-3 text-xs leading-6 text-muted-foreground">
              {t("扫码可直接添加作者微信，也可以手动搜索上面的微信号。")}
            </p>
            <div className="mt-4 overflow-hidden rounded-3xl border border-border/50 bg-white p-3">
              <img
                src="/author-wechat.jpg"
                alt="作者微信二维码"
                className="mx-auto aspect-square w-full max-w-[180px] rounded-2xl object-cover"
              />
            </div>
          </div>

          <div className="rounded-3xl border border-border/50 bg-background/40 p-5">
            <p className="text-xs uppercase tracking-[0.2em] text-muted-foreground">
              Telegram
            </p>
            <button
              type="button"
              onClick={() => {
                void handleOpenTelegramGroup();
              }}
              className="mt-3 inline-flex items-center gap-2 text-sm font-semibold text-primary transition-opacity hover:opacity-80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
            >
              {t("加入 TG 群聊")}
              <ExternalLink className="h-4 w-4" />
            </button>
            <p className="mt-3 text-xs leading-6 text-muted-foreground">
              {t("README 里维护的官方群链接，打开后即可加入讨论。")}
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
