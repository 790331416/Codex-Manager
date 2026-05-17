"use client";

import { useEffect, useMemo, useState } from "react";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button, buttonVariants } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ManagedModelPayload } from "@/lib/api/account-client";
import { useI18n } from "@/lib/i18n/provider";
import { ManagedModelInfo } from "@/types";

interface ModelCatalogModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  model?: ManagedModelInfo | null;
  nextSortIndex: number;
  isSaving?: boolean;
  onSave: (payload: ManagedModelPayload) => Promise<ManagedModelInfo | null>;
}

interface ModelCatalogDraft {
  slug: string;
  displayName: string;
  description: string;
  sourceKind: string;
  userEdited: boolean;
  supportedInApi: boolean;
  sortIndex: string;
  priority: string;
  visibility: string;
  defaultReasoningLevel: string;
  advancedJson: string;
}

const EDITABLE_ADVANCED_KEYS = [
  "supportedReasoningLevels",
  "shellType",
  "additionalSpeedTiers",
  "availabilityNux",
  "upgrade",
  "baseInstructions",
  "modelMessages",
  "supportsReasoningSummaries",
  "defaultReasoningSummary",
  "supportVerbosity",
  "defaultVerbosity",
  "applyPatchToolType",
  "webSearchToolType",
  "truncationPolicy",
  "supportsParallelToolCalls",
  "supportsImageDetailOriginal",
  "contextWindow",
  "autoCompactTokenLimit",
  "effectiveContextWindowPercent",
  "experimentalSupportedTools",
  "inputModalities",
  "minimalClientVersion",
  "supportsSearchTool",
  "availableInPlans",
];

const UNSET_SELECT_VALUE = "__unset__";

const VISIBILITY_OPTIONS = [
  { value: "list", label: "list" },
  { value: "hide", label: "hide" },
] as const;

const REASONING_EFFORT_OPTIONS = [
  { value: "low", label: "low" },
  { value: "medium", label: "medium" },
  { value: "high", label: "high" },
  { value: "xhigh", label: "xhigh" },
] as const;

function normalizeOptionalSelectValue(value: string | null): string {
  if (!value || value === UNSET_SELECT_VALUE) {
    return "";
  }
  if (value === "hidden") {
    return "hide";
  }
  return value;
}

function normalizeVisibilityValue(value: string | null | undefined): string {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "";
  }
  if (normalized === "hidden") {
    return "hide";
  }
  return normalized;
}

function toPrettyJson(value: unknown): string {
  if (
    !value ||
    (typeof value === "object" &&
      !Array.isArray(value) &&
      Object.keys(value).length === 0)
  ) {
    return "";
  }
  if (Array.isArray(value) && value.length === 0) {
    return "";
  }
  return JSON.stringify(value, null, 2);
}

function parseOptionalNumber(text: string, label: string): number {
  const parsed = Number(text.trim() || "0");
  if (!Number.isFinite(parsed)) {
    throw new Error(`${label} 必须是数字`);
  }
  return parsed;
}

function parseJsonObject(text: string, label: string): Record<string, unknown> {
  const trimmed = text.trim();
  if (!trimmed) {
    return {};
  }
  try {
    const parsed = JSON.parse(trimmed);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("必须是对象");
    }
    return parsed as Record<string, unknown>;
  } catch (error) {
    throw new Error(
      `${label} 不是有效 JSON 对象: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

function buildAdvancedJson(model: ManagedModelInfo | null | undefined): string {
  if (!model) {
    return toPrettyJson({
      inputModalities: ["text", "image"],
      supportedReasoningLevels: [],
      additionalSpeedTiers: [],
      experimentalSupportedTools: [],
      availableInPlans: [],
    });
  }

  const advanced = Object.fromEntries(
    Object.entries(model).filter(([key]) =>
      EDITABLE_ADVANCED_KEYS.includes(key),
    ),
  );
  const extra = Object.fromEntries(
    Object.entries(model).filter(
      ([key]) =>
        ![
          "slug",
          "displayName",
          "description",
          "sourceKind",
          "userEdited",
          "supportedInApi",
          "sortIndex",
          "updatedAt",
          "priority",
          "visibility",
          "defaultReasoningLevel",
          ...EDITABLE_ADVANCED_KEYS,
        ].includes(key),
    ),
  );
  return toPrettyJson({ ...advanced, ...extra });
}

function buildDraft(
  model: ManagedModelInfo | null | undefined,
  nextSortIndex: number,
): ModelCatalogDraft {
  return {
    slug: model?.slug || "",
    displayName: model?.displayName || "",
    description: model?.description || "",
    sourceKind: model?.sourceKind || "custom",
    userEdited: model?.userEdited ?? true,
    supportedInApi: model?.supportedInApi ?? false,
    sortIndex: String(model?.sortIndex ?? nextSortIndex),
    priority: String(model?.priority ?? 0),
    visibility: normalizeVisibilityValue(model?.visibility),
    defaultReasoningLevel: model?.defaultReasoningLevel || "",
    advancedJson: buildAdvancedJson(model),
  };
}

function buildDefaultModel(
  nextSortIndex: number,
  updatedAt: number,
): ManagedModelInfo {
  return {
    slug: "",
    displayName: "",
    description: null,
    defaultReasoningLevel: null,
    supportedReasoningLevels: [],
    shellType: null,
    visibility: null,
    supportedInApi: false,
    priority: 0,
    additionalSpeedTiers: [],
    availabilityNux: null,
    upgrade: null,
    baseInstructions: null,
    modelMessages: null,
    supportsReasoningSummaries: null,
    defaultReasoningSummary: null,
    supportVerbosity: null,
    defaultVerbosity: null,
    applyPatchToolType: null,
    webSearchToolType: null,
    truncationPolicy: null,
    supportsParallelToolCalls: null,
    supportsImageDetailOriginal: null,
    contextWindow: null,
    autoCompactTokenLimit: null,
    effectiveContextWindowPercent: null,
    experimentalSupportedTools: [],
    inputModalities: ["text", "image"],
    minimalClientVersion: null,
    supportsSearchTool: null,
    availableInPlans: [],
    sourceKind: "custom",
    userEdited: true,
    sortIndex: nextSortIndex,
    updatedAt,
  };
}

export function ModelCatalogModal({
  open,
  onOpenChange,
  model,
  nextSortIndex,
  isSaving = false,
  onSave,
}: ModelCatalogModalProps) {
  const { t } = useI18n();
  const [draft, setDraft] = useState<ModelCatalogDraft>(() =>
    buildDraft(model, nextSortIndex),
  );

  useEffect(() => {
    if (!open) return;
    const frameId = window.requestAnimationFrame(() => {
      setDraft(buildDraft(model, nextSortIndex));
    });
    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [model, nextSortIndex, open]);

  const title = useMemo(
    () => (model ? t("编辑模型") : t("新增模型")),
    [model, t],
  );

  const updateDraft = <K extends keyof ModelCatalogDraft>(
    key: K,
    value: ModelCatalogDraft[K],
  ) => {
    setDraft((current) => ({ ...current, [key]: value }));
  };

  const handleSave = async () => {
    const slug = draft.slug.trim();
    if (!slug) {
      throw new Error("模型 slug 不能为空");
    }

    const advancedFields = parseJsonObject(draft.advancedJson, "高级 JSON");
    const nextModel: ManagedModelInfo = {
      ...buildDefaultModel(nextSortIndex, model?.updatedAt ?? 0),
      ...advancedFields,
      slug,
      displayName: draft.displayName.trim() || slug,
      description: draft.description.trim() || null,
      sourceKind: draft.sourceKind,
      userEdited: draft.userEdited,
      supportedInApi: draft.supportedInApi,
      sortIndex: parseOptionalNumber(draft.sortIndex, "排序权重"),
      priority: parseOptionalNumber(draft.priority, "Priority"),
      visibility: draft.visibility.trim() || null,
      defaultReasoningLevel: draft.defaultReasoningLevel.trim() || null,
      updatedAt: model?.updatedAt ?? 0,
    };

    const saved = await onSave({
      previousSlug: model?.slug || null,
      sourceKind: nextModel.sourceKind,
      userEdited: nextModel.userEdited,
      sortIndex: nextModel.sortIndex,
      model: nextModel,
    });
    if (saved) {
      onOpenChange(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="glass-card p-0 md:max-w-[750px] xl:max-w-[1360px]">
        <div className="max-h-[84vh] overflow-y-auto p-6">
          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
            <DialogDescription>
              {t(
                "核心字段单独编辑，其余官方 `/models` 参数请直接在高级 JSON 中维护。",
              )}
            </DialogDescription>
          </DialogHeader>

          <div className="mt-6 grid gap-6">
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="model-slug">Slug</Label>
                <Input
                  id="model-slug"
                  value={draft.slug}
                  onChange={(event) => updateDraft("slug", event.target.value)}
                  placeholder="gpt-5.4"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="model-display-name">{t("显示名称")}</Label>
                <Input
                  id="model-display-name"
                  value={draft.displayName}
                  onChange={(event) =>
                    updateDraft("displayName", event.target.value)
                  }
                  placeholder="GPT-5.4"
                />
              </div>
              <div className="space-y-2 md:col-span-2">
                <Label htmlFor="model-description">{t("描述")}</Label>
                <Textarea
                  id="model-description"
                  rows={3}
                  value={draft.description}
                  onChange={(event) =>
                    updateDraft("description", event.target.value)
                  }
                />
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label>{t("来源类型")}</Label>
                <Select
                  value={draft.sourceKind}
                  onValueChange={(value) =>
                    updateDraft("sourceKind", value || "custom")
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                    <SelectItem value="custom">{t("自定义")}</SelectItem>
                    <SelectItem value="remote">{t("远端同步")}</SelectItem>
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="model-sort-index">{t("排序权重")}</Label>
                <Input
                  id="model-sort-index"
                  type="number"
                  value={draft.sortIndex}
                  onChange={(event) =>
                    updateDraft("sortIndex", event.target.value)
                  }
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="model-priority">{t("Priority")}</Label>
                <Input
                  id="model-priority"
                  type="number"
                  value={draft.priority}
                  onChange={(event) =>
                    updateDraft("priority", event.target.value)
                  }
                />
              </div>
              <div className="space-y-2">
                <Label>{t("可见性")}</Label>
                <Select
                  value={draft.visibility.trim() || UNSET_SELECT_VALUE}
                  onValueChange={(value) =>
                    updateDraft("visibility", normalizeOptionalSelectValue(value))
                  }
                >
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder={t("未设置")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                    <SelectItem value={UNSET_SELECT_VALUE}>{t("未设置")}</SelectItem>
                    {VISIBILITY_OPTIONS.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2 md:col-span-2">
                <Label>{t("默认推理等级")}</Label>
                <Select
                  value={draft.defaultReasoningLevel.trim() || UNSET_SELECT_VALUE}
                  onValueChange={(value) =>
                    updateDraft(
                      "defaultReasoningLevel",
                      normalizeOptionalSelectValue(value),
                    )
                  }
                >
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder={t("未设置")} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                    <SelectItem value={UNSET_SELECT_VALUE}>{t("未设置")}</SelectItem>
                    {REASONING_EFFORT_OPTIONS.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <Card size="sm">
                <CardContent className="flex items-center justify-between gap-4">
                  <div className="space-y-1">
                    <p className="text-sm font-medium">{t("可用于 API")}</p>
                    <p className="text-xs text-muted-foreground">
                      {t("开启后必须至少配置一个启用映射，否则只能保存为草稿。")}
                    </p>
                  </div>
                  <Switch
                    checked={draft.supportedInApi}
                    onCheckedChange={(checked) =>
                      updateDraft("supportedInApi", checked)
                    }
                  />
                </CardContent>
              </Card>
              <Card size="sm">
                <CardContent className="flex items-center justify-between gap-4">
                  <div className="space-y-1">
                    <p className="text-sm font-medium">{t("保留本地覆写")}</p>
                    <p className="text-xs text-muted-foreground">
                      {t("开启后，远端刷新不会直接覆盖当前本地版本。")}
                    </p>
                  </div>
                  <Switch
                    checked={draft.userEdited}
                    onCheckedChange={(checked) =>
                      updateDraft("userEdited", checked)
                    }
                  />
                </CardContent>
              </Card>
            </div>

            <div className="space-y-2">
              <Label htmlFor="model-advanced-json">{t("高级 JSON")}</Label>
              <Textarea
                id="model-advanced-json"
                rows={20}
                value={draft.advancedJson}
                onChange={(event) =>
                  updateDraft("advancedJson", event.target.value)
                }
                placeholder='{"inputModalities":["text","image"],"supportedReasoningLevels":[{"effort":"medium","description":"balanced"}]}'
              />
              <p className="text-xs text-muted-foreground">
                {t(
                  "在这里维护所有复杂字段，包括 supportedReasoningLevels、truncationPolicy、inputModalities、availableInPlans 以及任意扩展字段。",
                )}
              </p>
            </div>
          </div>
        </div>

        <div className="border-t border-border/50 px-5 py-3">
          <DialogFooter>
            <DialogClose
              className={buttonVariants({ variant: "ghost" })}
              type="button"
            >
              {t("取消")}
            </DialogClose>
            <Button
              onClick={() => {
                void handleSave();
              }}
              disabled={isSaving}
            >
              {isSaving ? t("保存中...") : t("保存模型")}
            </Button>
          </DialogFooter>
        </div>
      </DialogContent>
    </Dialog>
  );
}
