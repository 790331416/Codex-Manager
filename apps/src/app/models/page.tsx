"use client";

import { useEffect, useMemo, useState } from "react";
import {
  MoreVertical,
  PencilLine,
  Plus,
  RefreshCw,
  Search,
  Trash2,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ConfirmDialog } from "@/components/modals/confirm-dialog";
import { ModelCatalogModal } from "@/components/modals/model-catalog-modal";
import { useDesktopPageActive } from "@/hooks/useDesktopPageActive";
import { useManagedModels } from "@/hooks/useManagedModels";
import { usePageTransitionReady } from "@/hooks/usePageTransitionReady";
import { useI18n } from "@/lib/i18n/provider";
import { formatTsFromSeconds } from "@/lib/utils/usage";

type ModelFilter = "all" | "api" | "custom" | "edited";

function MiniStatBadge({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="inline-flex items-center gap-2 rounded-full border border-border/60 bg-background/45 px-3 py-1.5 text-xs text-muted-foreground">
      <span>{label}</span>
      <span className="font-semibold text-foreground">{value}</span>
    </div>
  );
}

export default function ModelsPage() {
  const { t } = useI18n();
  const {
    models,
    isLoading,
    isServiceReady,
    refreshRemote,
    saveModel,
    deleteModel,
    isRefreshing,
    isSaving,
    isDeleting,
  } = useManagedModels();
  const isPageActive = useDesktopPageActive("/models/");
  usePageTransitionReady("/models/", !isServiceReady || !isLoading);

  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<ModelFilter>("all");
  const [modalOpen, setModalOpen] = useState(false);
  const [editingSlug, setEditingSlug] = useState<string | null>(null);
  const [deleteSlug, setDeleteSlug] = useState<string | null>(null);

  useEffect(() => {
    if (isPageActive) return;
    setModalOpen(false);
    setEditingSlug(null);
    setDeleteSlug(null);
  }, [isPageActive]);

  const editingModel = useMemo(
    () => models.find((item) => item.slug === editingSlug) || null,
    [editingSlug, models]
  );

  const nextSortIndex = useMemo(
    () => models.reduce((maxValue, item) => Math.max(maxValue, item.sortIndex), -1) + 1,
    [models]
  );

  const stats = useMemo(
    () => ({
      total: models.length,
      apiEnabled: models.filter((item) => item.supportedInApi).length,
      custom: models.filter((item) => item.sourceKind === "custom").length,
      edited: models.filter((item) => item.userEdited).length,
    }),
    [models]
  );

  const filteredModels = useMemo(() => {
    const keyword = search.trim().toLowerCase();
    return models.filter((model) => {
      const matchesKeyword =
        !keyword ||
        model.slug.toLowerCase().includes(keyword) ||
        model.displayName.toLowerCase().includes(keyword) ||
        String(model.description || "").toLowerCase().includes(keyword);
      if (!matchesKeyword) return false;

      switch (filter) {
        case "api":
          return model.supportedInApi;
        case "custom":
          return model.sourceKind === "custom";
        case "edited":
          return model.userEdited;
        default:
          return true;
      }
    });
  }, [filter, models, search]);

  const currentFilterLabel = useMemo(() => {
    switch (filter) {
      case "api":
        return t("仅 API 可用");
      case "custom":
        return t("仅自定义");
      case "edited":
        return t("仅本地覆写");
      default:
        return t("全部模型");
    }
  }, [filter, t]);

  return (
    <>
      <div className="space-y-3 animate-in fade-in duration-500">
        <div className="space-y-2">
          <div className="space-y-2">
            <Badge className="w-fit rounded-full bg-primary/10 px-3 py-1 text-primary">
              {t("模型目录")}
            </Badge>
            <div className="space-y-1">
              <h1 className="text-3xl font-semibold tracking-tight">{t("模型管理")}</h1>
              <p className="max-w-4xl text-sm leading-6 text-muted-foreground">
                {t("这里维护本地结构化模型目录。默认绑定模型会优先展示 supportedInApi=true 的模型，而 Codex CLI 仍会拿到完整目录。")}
              </p>
            </div>
            <div className="flex flex-wrap gap-2 text-xs">
              <Badge variant="secondary" className="rounded-full px-3 py-1">
                {t("完整目录会同步到 Codex CLI")}
              </Badge>
              <Badge variant="secondary" className="rounded-full px-3 py-1">
                {t("默认绑定优先展示 API 可用模型")}
              </Badge>
              <Badge variant="secondary" className="rounded-full px-3 py-1">
                {t("远端刷新可与本地覆写共存")}
              </Badge>
            </div>
          </div>
        </div>

        <Card className="glass-card border-none shadow-md backdrop-blur-md">
          <CardHeader className="pb-3">
            <div className="flex flex-col gap-3">
              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div>
                  <CardTitle>{t("模型目录明细")}</CardTitle>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {t("按 slug、显示名称或描述快速定位，并结合来源与覆写状态查看当前目录。")}
                  </p>
                </div>
                <div className="flex flex-wrap gap-2 lg:justify-end">
                  <Button
                    variant="outline"
                    onClick={() => void refreshRemote()}
                    disabled={isRefreshing}
                  >
                    <RefreshCw className={`mr-2 h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
                    {t("远端并入")}
                  </Button>
                  <Button
                    onClick={() => {
                      setEditingSlug(null);
                      setModalOpen(true);
                    }}
                  >
                    <Plus className="mr-2 h-4 w-4" />
                    {t("新增自定义模型")}
                  </Button>
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <MiniStatBadge label={t("模型总数")} value={`${stats.total}`} />
                <MiniStatBadge label={t("API 可用")} value={`${stats.apiEnabled}`} />
                <MiniStatBadge label={t("自定义模型")} value={`${stats.custom}`} />
                <MiniStatBadge label={t("本地覆写")} value={`${stats.edited}`} />
                <Badge variant="secondary" className="rounded-full px-3 py-1">
                  {t("当前筛选")} {currentFilterLabel}
                </Badge>
                <Badge variant="secondary" className="rounded-full px-3 py-1">
                  {t("共 {count} 条", { count: filteredModels.length })}
                </Badge>
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <div className="flex h-10 items-center gap-2 rounded-xl border border-border/60 bg-background/35 px-3">
                  <Search className="h-4 w-4 text-muted-foreground" />
                  <Input
                    value={search}
                    onChange={(event) => setSearch(event.target.value)}
                    placeholder={t("搜索 slug、显示名称或描述")}
                    className="h-full border-none bg-transparent px-0 shadow-none focus-visible:ring-0"
                  />
                </div>
                <Select value={filter} onValueChange={(value) => setFilter(value as ModelFilter)}>
                  <SelectTrigger className="h-10 w-full rounded-xl px-3">
                    <SelectValue>{currentFilterLabel}</SelectValue>
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">{t("全部模型")}</SelectItem>
                    <SelectItem value="api">{t("仅 API 可用")}</SelectItem>
                    <SelectItem value="custom">{t("仅自定义")}</SelectItem>
                    <SelectItem value="edited">{t("仅本地覆写")}</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="text-xs text-muted-foreground">
                {t("保存后会自动同步到 `~/.codex/models_cache.json`；如需让 `/model` 立即看到最新模型与说明，仍需重启正在运行中的 Codex 会话。")}
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {!isServiceReady ? (
              <div className="rounded-2xl border border-dashed border-border/70 bg-background/35 px-6 py-10 text-sm text-muted-foreground">
                {t("服务未连接，当前无法读取模型目录。")}
              </div>
            ) : isLoading ? (
              <div className="space-y-3">
                {Array.from({ length: 6 }).map((_, index) => (
                  <Skeleton key={`models-skeleton-${index}`} className="h-12 w-full rounded-xl" />
                ))}
              </div>
            ) : filteredModels.length === 0 ? (
              <div className="rounded-2xl border border-dashed border-border/70 bg-background/35 px-6 py-10 text-sm text-muted-foreground">
                {t("没有匹配的模型。你可以调整筛选条件，或直接新增一个自定义模型。")}
              </div>
            ) : (
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>{t("模型")}</TableHead>
                      <TableHead>{t("来源")}</TableHead>
                      <TableHead>{t("API")}</TableHead>
                      <TableHead>{t("可见性")}</TableHead>
                      <TableHead>{t("推理等级")}</TableHead>
                      <TableHead>{t("更新时间")}</TableHead>
                      <TableHead className="table-sticky-action-head w-[88px] text-right">
                        {t("操作")}
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredModels.map((model) => (
                      <TableRow key={model.slug}>
                        <TableCell className="min-w-[280px]">
                          <div className="space-y-1">
                            <div className="flex items-center gap-2">
                              <span className="font-medium">{model.displayName || model.slug}</span>
                              <Badge variant="secondary" className="font-mono text-[11px]">
                                {model.slug}
                              </Badge>
                            </div>
                            <p className="text-xs text-muted-foreground">
                              {model.description || t("未填写描述")}
                            </p>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex flex-wrap gap-2">
                            <Badge
                              variant={model.sourceKind === "custom" ? "default" : "secondary"}
                            >
                              {model.sourceKind === "custom" ? t("自定义") : t("远端")}
                            </Badge>
                            {model.userEdited ? (
                              <Badge className="bg-primary/10 text-primary">{t("已覆写")}</Badge>
                            ) : null}
                          </div>
                        </TableCell>
                        <TableCell>
                          {model.supportedInApi ? (
                            <Badge className="bg-emerald-500/10 text-emerald-600">
                              {t("可用")}
                            </Badge>
                          ) : (
                            <Badge variant="outline">{t("隐藏")}</Badge>
                          )}
                        </TableCell>
                        <TableCell>
                          {model.visibility === "list" ? (
                            <Badge className="bg-primary/10 text-primary">list</Badge>
                          ) : model.visibility === "hide" ? (
                            <Badge variant="outline">hide</Badge>
                          ) : (
                            <Badge variant="secondary">{t("未设置")}</Badge>
                          )}
                        </TableCell>
                        <TableCell className="text-sm text-muted-foreground">
                          {model.supportedReasoningLevels.length > 0
                            ? model.supportedReasoningLevels.map((item) => item.effort).join(" / ")
                            : model.defaultReasoningLevel || t("未配置")}
                        </TableCell>
                        <TableCell className="text-sm text-muted-foreground">
                          {formatTsFromSeconds(model.updatedAt, t("未同步"))}
                        </TableCell>
                        <TableCell className="table-sticky-action-cell text-right">
                          <DropdownMenu>
                            <DropdownMenuTrigger render={<span />} nativeButton={false}>
                              <Button variant="ghost" size="icon" aria-label={t("模型操作")}>
                                <MoreVertical className="h-4 w-4" />
                              </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                              <DropdownMenuItem
                                onClick={() => {
                                  setEditingSlug(model.slug);
                                  setModalOpen(true);
                                }}
                              >
                                <PencilLine className="h-4 w-4" />
                                {t("编辑模型")}
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                variant="destructive"
                                onClick={() => setDeleteSlug(model.slug)}
                              >
                                <Trash2 className="h-4 w-4" />
                                {t("删除模型")}
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <ModelCatalogModal
        open={modalOpen}
        onOpenChange={setModalOpen}
        model={editingModel}
        nextSortIndex={nextSortIndex}
        isSaving={isSaving}
        onSave={saveModel}
      />

      <ConfirmDialog
        open={Boolean(deleteSlug)}
        onOpenChange={(open) => {
          if (!open) {
            setDeleteSlug(null);
          }
        }}
        title={t("删除模型")}
        description={
          deleteSlug
            ? t("确定要删除模型 {slug} 吗？如果后续执行远端刷新，远端模型可能会再次并入本地目录。", {
                slug: deleteSlug,
              })
            : ""
        }
        confirmText={isDeleting ? t("删除中...") : t("删除")}
        confirmVariant="destructive"
        onConfirm={() => {
          if (deleteSlug) {
            void deleteModel(deleteSlug).then((ok) => {
              if (ok) {
                setDeleteSlug(null);
              }
            });
          }
        }}
      />
    </>
  );
}
