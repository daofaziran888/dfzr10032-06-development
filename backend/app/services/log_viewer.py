"""采集日志业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "log_viewer"
REQUIRED_FIELDS = ["日志编号", "关联任务", "日志级别"]
STATUS_ORDER = ["未处理", "处理中", "已处理", "已忽略"]
ACTION_RULES = {"开始处理": "处理中", "标记已处理": "已处理", "忽略日志": "已忽略"}
NEGATIVE_ACTIONS = ["忽略日志"]


class LogViewerService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        extra: dict[str, str] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("日志编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if extra:
            for field, val in extra.items():
                rows = [row for row in rows if val in str(row.get(field, ""))]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def append_system_log(
        self,
        *,
        log_code: str,
        task: str,
        level: str,
        content: str,
        status: str = "未处理",
        stack: str = "",
        operator: str = "采集调度器",
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """登记一条由系统动作产生的日志（批次包切分等），保留业务侧附带的核对字段。"""
        rows = store.rows(MODULE)
        entry: dict[str, Any] = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "日志编号": log_code,
            "关联任务": task,
            "日志级别": level,
            "记录时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "日志内容": content,
            "错误堆栈": stack,
            "处理状态": status,
            "处理人": operator,
            "status": status,
            "pending": status in ("未处理", "处理中"),
            "abnormal": level == "错误",
        }
        if extra:
            entry.update(extra)
        rows.append(entry)
        return entry

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"日志条目 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集日志可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"日志条目已{action}"
