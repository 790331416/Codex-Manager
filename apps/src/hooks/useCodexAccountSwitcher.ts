"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { appClient } from "@/lib/api/app-client";
import { getAppErrorMessage, isTauriRuntime } from "@/lib/api/transport";
import { useI18n } from "@/lib/i18n/provider";

export function useCodexAccountSwitcher() {
  const { t } = useI18n();
  const queryClient = useQueryClient();
  const supported = isTauriRuntime();

  const mutation = useMutation({
    mutationFn: async (accountId: string) => appClient.switchCodexAccount(accountId),
    onSuccess: async (result) => {
      toast.success(result.message || "切号成功");
      await queryClient.invalidateQueries({ queryKey: ["startup-snapshot"] });
    },
    onError: (error) => {
      toast.error(`${t("切号失败")}: ${getAppErrorMessage(error)}`);
    },
  });

  return {
    canSwitchCodexAccount: supported,
    switchCodexAccount: mutation.mutate,
    isSwitchingCodexAccount: mutation.isPending,
    switchingCodexAccountId: mutation.isPending ? mutation.variables ?? null : null,
  };
}
