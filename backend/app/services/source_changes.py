"""源端变更记录：按游标位置生成可打包、可离线回放的变更明细。

平台里的增量游标只保存「当前水位」，没有真实源库可连；这里按来源标识和游标区间
生成稳定的示例变更记录，保证同一游标位置每次读到的内容一致，批次包才可复现。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

SHELF_SOURCE = "货架变更"
_CHANGE_TYPES = ("盘点差异", "上架", "下架", "移位")
_BASE_TIME = datetime(2026, 11, 20, 8, 0, 0)


class SourceChangeService:
    """只读的源端变更记录提供方。"""

    def records(self, source: str, start: int, end: int) -> list[dict[str, Any]]:
        """返回闭区间 [start, end] 内的全部变更记录，游标位置连续且唯一。"""
        if start > end:
            return []
        if source == SHELF_SOURCE:
            return [self._shelf_record(source, cursor) for cursor in range(start, end + 1)]
        if source == "订单变更":
            return [self._order_record(source, cursor) for cursor in range(start, end + 1)]
        if source == "商品变更":
            return [self._product_record(source, cursor) for cursor in range(start, end + 1)]
        return [self._generic_record(source, cursor) for cursor in range(start, end + 1)]

    def shelf_codes(self, records: list[dict[str, Any]]) -> list[str]:
        """按记录顺序取出货架编号；非货架来源返回空列表。"""
        return [
            str(record["货架编号"])
            for record in records
            if str(record.get("货架编号") or "").strip()
        ]

    def _shelf_record(self, source: str, cursor: int) -> dict[str, Any]:
        change_type = _CHANGE_TYPES[cursor % len(_CHANGE_TYPES)]
        return {
            "游标位置": cursor,
            "来源标识": source,
            "货架编号": f"SH-{cursor:04d}",
            "库区": f"A{(cursor - 1) // 100 + 1:02d}",
            "变更类型": change_type,
            "变更时间": self._changed_at(cursor),
            "变更摘要": f"货架 SH-{cursor:04d} {change_type}",
        }

    def _order_record(self, source: str, cursor: int) -> dict[str, Any]:
        return {
            "游标位置": cursor,
            "来源标识": source,
            "订单编号": f"ORD-{cursor:06d}",
            "变更类型": _CHANGE_TYPES[cursor % len(_CHANGE_TYPES)],
            "变更时间": self._changed_at(cursor),
        }

    def _product_record(self, source: str, cursor: int) -> dict[str, Any]:
        return {
            "游标位置": cursor,
            "来源标识": source,
            "商品编号": f"SKU-{cursor:06d}",
            "变更类型": _CHANGE_TYPES[cursor % len(_CHANGE_TYPES)],
            "变更时间": self._changed_at(cursor),
        }

    def _generic_record(self, source: str, cursor: int) -> dict[str, Any]:
        return {
            "游标位置": cursor,
            "来源标识": source,
            "记录编号": f"REC-{cursor:06d}",
            "变更类型": _CHANGE_TYPES[cursor % len(_CHANGE_TYPES)],
            "变更时间": self._changed_at(cursor),
        }

    @staticmethod
    def _changed_at(cursor: int) -> str:
        return (_BASE_TIME + timedelta(minutes=cursor % 720)).isoformat(timespec="seconds")
