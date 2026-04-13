"use client";

import { useEffect, useState } from "react";
import { getLocalDayRange, type LocalDayRange } from "@/lib/utils/time";

/**
 * 函数 `useLocalDayRange`
 *
 * 作者: gaohongshun
 *
 * 时间: 2026-04-13
 *
 * # 参数
 * 无
 *
 * # 返回
 * 返回当前浏览器本地时区对应的当天时间范围
 */
export function useLocalDayRange(): LocalDayRange {
  const [dayRange, setDayRange] = useState<LocalDayRange>(() => getLocalDayRange());

  useEffect(() => {
    const refresh = () => {
      setDayRange((current) => {
        const next = getLocalDayRange();
        if (
          current.dayStartTs === next.dayStartTs &&
          current.dayEndTs === next.dayEndTs &&
          current.timeZone === next.timeZone
        ) {
          return current;
        }
        return next;
      });
    };

    refresh();
    const intervalId = window.setInterval(refresh, 60_000);
    return () => window.clearInterval(intervalId);
  }, []);

  return dayRange;
}
