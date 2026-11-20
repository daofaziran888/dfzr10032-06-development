"""增量批次包：把游标区间内的源端变更切成分片，原子发布可离线回放的包。

回放人员的核对口径都依赖下面几条不变量：

* **索引最后发布**：所有分片写完、记录数与游标区间都对得上，才原子写入 ``index.json``。
  没有索引的目录一律视为未交付，下载与回放接口会拒绝，避免把缺失的游标位置误读成
  「没有变更」。
* **每次尝试独立落盘**：重试新建 ``attempts/attempt-000N``，上一次的中间片留在原地供
  采集日志核对，但绝不会被本次索引引用，也不会覆盖已交付的索引。
* **分片口径冻结**：一次切分只用一个分片条数。切分途中口径被改动（现场把「每片 10 条」
  改为「每片 6 条」）就判本次尝试失败，不会产出 10/6 混排的索引；重试按新口径整包重切。
* **日志按尝试登记**：每次尝试都在采集日志留下实际覆盖的游标区间和逐片货架区间；未交付
  的尝试只能用于核对，不能作为回放输入。
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import threading
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import settings
from app.services.log_viewer import LogViewerService
from app.services.source_changes import SourceChangeService
from app.store import store

DELIVERED = "已交付"
UNDELIVERED = "未交付"
SPLITTING = "切分中"
FAILED = "失败"

PACKAGE_PREFIX = "PKG"
_BATCH_NO_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_DEFAULT_SHARD_SIZE = 20
_MAX_SHARD_SIZE = 1000


class BatchPackageError(Exception):
    """批次包操作失败：message 直接给回放人员看，status 用作 HTTP 状态码。"""

    def __init__(self, message: str, *, status: int = 400, code: str = "batch_error") -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code


class BatchWriteError(RuntimeError):
    """分片写入失败。"""


class ShardSizeDrift(RuntimeError):
    """切分途中分片口径发生变化。"""

    def __init__(self, snapshot: int, live: int) -> None:
        self.snapshot = snapshot
        self.live = live
        super().__init__(
            f"分片口径在切分过程中发生变化：本次尝试按每片 {snapshot} 条切分，现场已改为每片 {live} 条"
        )


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _atomic_write_json(path: Path, payload: Any) -> None:
    """同目录临时文件 + os.replace，读到的要么是旧版本要么是完整新版本。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _read_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _optional_int(raw: Any) -> int | None:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise BatchPackageError(f"「{raw}」不是有效的整数") from exc


def _format_range(start: int, end: int) -> str:
    return f"[{start}, {end}]"


class BatchPackageService:
    def __init__(
        self,
        root: str | Path | None = None,
        *,
        logs: LogViewerService | None = None,
        sources: SourceChangeService | None = None,
    ) -> None:
        self._root = Path(root) if root is not None else None
        self.logs = logs or LogViewerService()
        self.sources = sources or SourceChangeService()
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ 基础
    @property
    def root(self) -> Path:
        root = self._root or Path(settings.package_root)
        root.mkdir(parents=True, exist_ok=True)
        return root

    @property
    def stale_attempts_policy(self) -> str:
        """未交付尝试的中间片的处置口径：保留供核对，永不进入索引。"""
        return "未交付尝试的分片保留在 attempts/ 下供采集日志核对，不会进入任何索引，也不能作为回放输入"

    def _package_dir(self, batch_no: str) -> Path:
        if not _BATCH_NO_RE.match(batch_no):
            raise BatchPackageError(f"批次包编号「{batch_no}」不合法", status=400)
        root = self.root.resolve()
        path = (root / batch_no).resolve()
        if path != root and root not in path.parents:
            raise BatchPackageError("批次包路径越界", status=400)
        return path

    def _meta_path(self, batch_no: str) -> Path:
        return self._package_dir(batch_no) / "package.json"

    def _load_package(self, batch_no: str) -> dict[str, Any]:
        path = self._meta_path(batch_no)
        if not path.exists():
            raise BatchPackageError(f"增量批次包 {batch_no} 不存在", status=404, code="not_found")
        meta = _read_json(path)
        if not isinstance(meta, dict) or "batch_no" not in meta:
            raise BatchPackageError(
                f"增量批次包 {batch_no} 的元数据损坏", status=500, code="meta_invalid"
            )
        return meta

    def _save_package(self, meta: dict[str, Any]) -> None:
        _atomic_write_json(self._meta_path(meta["batch_no"]), meta)

    def _new_batch_no(self) -> str:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        return f"{PACKAGE_PREFIX}-{stamp}-{uuid.uuid4().hex[:6].upper()}"

    # ------------------------------------------------------------------ 创建/重试
    def create_batch(self, values: dict[str, Any]) -> dict[str, Any]:
        """创建批次包；请求里带已有批次包编号时，按该包追加一次尝试（重试）。"""
        with self._lock:
            batch_no = str(values.get("批次包编号") or "").strip()
            if batch_no:
                meta = self._load_package(batch_no)
                return self._start_attempt(meta, values)

            source = str(values.get("来源标识") or "").strip()
            if not source:
                raise BatchPackageError("缺少必填字段：来源标识")
            start, end = self._parse_range(values)
            cursor_entry, cursor_code = self._resolve_cursor(values)
            shard_size = self._resolve_shard_size(values, cursor_entry)
            follows_cursor = self._size_follows_cursor(values)

            batch_no = self._new_batch_no()
            meta = {
                "batch_no": batch_no,
                "source": source,
                "cursor_code": cursor_code,
                "cursor_id": cursor_entry.get("id") if cursor_entry else None,
                "cursor_start": start,
                "cursor_end": end,
                "status": SPLITTING,
                "delivered_attempt": None,
                "attempts": [],
                "created_at": _now(),
                "updated_at": _now(),
            }
            meta["attempts"].append(self._new_attempt_meta(1, shard_size, start, end))
            self._save_package(meta)
            return self._run_attempt(
                meta, 1, values, source, start, end, cursor_entry, shard_size, follows_cursor
            )

    def retry_batch(self, batch_no: str, values: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            meta = self._load_package(batch_no)
            return self._start_attempt(meta, values)

    def _start_attempt(self, meta: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
        batch_no = meta["batch_no"]
        source = str(values.get("来源标识") or meta.get("source") or "").strip()
        if source != meta["source"]:
            raise BatchPackageError(
                f"重试不能更换来源：批次包是「{meta['source']}」，请求里是「{source}」"
            )
        start, end = self._parse_range(
            {
                "起始游标": values.get("起始游标", meta["cursor_start"]),
                "结束游标": values.get("结束游标", meta["cursor_end"]),
            }
        )
        if (start, end) != (meta["cursor_start"], meta["cursor_end"]):
            raise BatchPackageError(
                f"重试不能改变游标范围：批次包覆盖 {_format_range(meta['cursor_start'], meta['cursor_end'])}，"
                f"请求里是 {_format_range(start, end)}"
            )
        cursor_entry, _ = self._resolve_cursor(
            {"游标编号": values.get("游标编号") or meta.get("cursor_code")},
            allow_missing=True,
        )
        shard_size = self._resolve_shard_size(values, cursor_entry)
        follows_cursor = self._size_follows_cursor(values)
        attempt_no = max((int(item["attempt_no"]) for item in meta["attempts"]), default=0) + 1
        meta["attempts"].append(self._new_attempt_meta(attempt_no, shard_size, start, end))
        if meta.get("delivered_attempt") is None:
            meta["status"] = SPLITTING
        meta["updated_at"] = _now()
        self._save_package(meta)
        return self._run_attempt(
            meta, attempt_no, values, source, start, end, cursor_entry, shard_size, follows_cursor
        )

    def _new_attempt_meta(self, attempt_no: int, shard_size: int, start: int, end: int) -> dict[str, Any]:
        return {
            "attempt_no": attempt_no,
            "status": SPLITTING,
            "shard_size": shard_size,
            "cursor_start": start,
            "cursor_end": end,
            "record_count": 0,
            "shards": [],
            "failure": "",
            "started_at": _now(),
            "finished_at": "",
            "deliverable": False,
            "index_file": "",
        }

    @staticmethod
    def _parse_range(values: dict[str, Any]) -> tuple[int, int]:
        start_raw = values.get("起始游标")
        end_raw = values.get("结束游标")
        if start_raw in (None, "") or end_raw in (None, ""):
            raise BatchPackageError("缺少必填字段：起始游标、结束游标")
        try:
            start = int(str(start_raw).strip())
            end = int(str(end_raw).strip())
        except (TypeError, ValueError) as exc:
            raise BatchPackageError("起始游标、结束游标必须是整数") from exc
        if start < 0:
            raise BatchPackageError("起始游标不能小于 0")
        if end < start:
            raise BatchPackageError(f"结束游标 {end} 不能小于起始游标 {start}")
        return start, end

    @staticmethod
    def _resolve_cursor(
        values: dict[str, Any], *, allow_missing: bool = False
    ) -> tuple[dict[str, Any] | None, str]:
        code = str(values.get("游标编号") or "").strip()
        if not code:
            return None, ""
        for row in store.rows("incremental"):
            if str(row.get("游标编号")) == code:
                return row, code
        if allow_missing:
            return None, code
        raise BatchPackageError(f"增量游标 {code} 不存在或已归档", status=404)

    @staticmethod
    def _resolve_shard_size(values: dict[str, Any], cursor_entry: dict[str, Any] | None) -> int:
        explicit = values.get("分片条数")
        if explicit not in (None, ""):
            try:
                size = int(str(explicit).strip())
            except (TypeError, ValueError) as exc:
                raise BatchPackageError(f"分片条数「{explicit}」不是有效的整数") from exc
            if size <= 0 or size > _MAX_SHARD_SIZE:
                raise BatchPackageError(f"分片条数必须在 1 到 {_MAX_SHARD_SIZE} 之间")
            return size
        # 未显式指定时跟随游标的批次大小；示例数据里的非数字占位值退回默认口径。
        raw = str((cursor_entry or {}).get("批次大小") or "").strip()
        try:
            size = int(raw)
        except ValueError:
            return _DEFAULT_SHARD_SIZE
        return size if 0 < size <= _MAX_SHARD_SIZE else _DEFAULT_SHARD_SIZE

    @staticmethod
    def _size_follows_cursor(values: dict[str, Any]) -> bool:
        """显式给了分片条数就以它为准，否则跟随游标的「批次大小」。"""
        return values.get("分片条数") in (None, "")

    # ------------------------------------------------------------------ 切分
    def _run_attempt(
        self,
        meta: dict[str, Any],
        attempt_no: int,
        values: dict[str, Any],
        source: str,
        start: int,
        end: int,
        cursor_entry: dict[str, Any] | None,
        shard_size: int,
        follows_cursor: bool,
    ) -> dict[str, Any]:
        batch_no = meta["batch_no"]
        # 这两个参数只用于本地演练「写入失败重试」和「切分途中改口径」，生产环境关闭。
        fail_after = _optional_int(values.get("模拟写入失败于第几片"))
        drift_size = _optional_int(values.get("模拟途中改为每片条数"))
        if not settings.allow_debug_fault:
            fail_after = None
            drift_size = None

        attempt_dir = self._package_dir(batch_no) / "attempts" / f"attempt-{attempt_no:04d}"
        shard_dir = attempt_dir / "shards"
        shard_dir.mkdir(parents=True, exist_ok=True)
        attempt_meta = next(item for item in meta["attempts"] if int(item["attempt_no"]) == attempt_no)
        _atomic_write_json(attempt_dir / "attempt.json", attempt_meta)

        log_row = self.logs.append_system_log(
            log_code=f"BATCH-{batch_no}-A{attempt_no:02d}",
            task=f"增量批次包 {batch_no}",
            level="信息",
            content=(
                f"增量批次包 {batch_no} 第 {attempt_no} 次切分开始：来源 {source}，"
                f"游标 {_format_range(start, end)}，按每片 {shard_size} 条切分"
            ),
            status="处理中",
            extra={
                "批次包编号": batch_no,
                "尝试序号": attempt_no,
                "切分口径": f"每片 {shard_size} 条",
                "分片条数": shard_size,
                "覆盖游标区间": _format_range(start, end),
                "实际覆盖段": "",
                "分片明细": [],
                "交付状态": UNDELIVERED,
                "可回放": "否",
                "索引文件": "",
            },
        )

        shards: list[dict[str, Any]] = []
        total = end - start + 1
        cursor = start
        shard_no = 0
        try:
            while cursor <= end:
                # 口径漂移检查放在每片开始前：已经写过的片不重写，本次尝试直接作废。
                live_size = self._live_shard_size(
                    cursor_entry, shard_size, follows_cursor, drift_size, shard_no
                )
                if live_size != shard_size:
                    raise ShardSizeDrift(shard_size, live_size)

                shard_no += 1
                shard_end = min(cursor + shard_size - 1, end)
                records = self.sources.records(source, cursor, shard_end)
                if len(records) != shard_end - cursor + 1:
                    raise BatchWriteError(
                        f"源端记录不完整：分片 {shard_no} 期望 {shard_end - cursor + 1} 条，"
                        f"实际 {len(records)} 条"
                    )

                shard_payload = {
                    "batch_no": batch_no,
                    "attempt_no": attempt_no,
                    "shard_no": shard_no,
                    "source": source,
                    "cursor_start": cursor,
                    "cursor_end": shard_end,
                    "record_count": len(records),
                    "records": records,
                }
                shard_path = shard_dir / f"shard-{shard_no:04d}.json"
                _atomic_write_json(shard_path, shard_payload)

                shelf_codes = self.sources.shelf_codes(records)
                shard_entry = {
                    "shard_no": shard_no,
                    "cursor_start": cursor,
                    "cursor_end": shard_end,
                    "record_count": len(records),
                    "shelf_start": shelf_codes[0] if shelf_codes else "",
                    "shelf_end": shelf_codes[-1] if shelf_codes else "",
                    "shelf_range": (
                        f"{shelf_codes[0]}~{shelf_codes[-1]}" if shelf_codes else ""
                    ),
                    "file": f"attempts/attempt-{attempt_no:04d}/shards/{shard_path.name}",
                    "sha256": _sha256(shard_path),
                }
                shards.append(shard_entry)

                # 每写完一片就同步尝试元数据和日志，进程中断也能留下实际覆盖段。
                attempt_meta["shards"] = list(shards)
                attempt_meta["record_count"] = sum(item["record_count"] for item in shards)
                _atomic_write_json(attempt_dir / "attempt.json", attempt_meta)
                log_row["分片明细"] = list(shards)
                log_row["实际覆盖段"] = _format_range(start, shard_end)
                log_row["日志内容"] = (
                    f"增量批次包 {batch_no} 第 {attempt_no} 次切分进行中：来源 {source}，"
                    f"游标 {_format_range(start, end)}，按每片 {shard_size} 条切分；"
                    f"已写入 {shard_no} 片，实际覆盖 {_format_range(start, shard_end)}"
                )

                if fail_after is not None and shard_no >= fail_after:
                    raise BatchWriteError(
                        f"模拟第 {fail_after} 片写入失败（写入未确认），本次切分未交付"
                    )
                cursor = shard_end + 1

            record_count = sum(item["record_count"] for item in shards)
            if record_count != total:
                raise BatchWriteError(
                    f"切分记录数不齐：应覆盖 {total} 条，实际 {record_count} 条"
                )

            index = {
                "batch_no": batch_no,
                "source": source,
                "cursor_code": meta.get("cursor_code", ""),
                "cursor_start": start,
                "cursor_end": end,
                "record_count": record_count,
                "shard_size": shard_size,
                "shard_count": len(shards),
                "granularity": {"shard_size": shard_size, "rule": f"每片 {shard_size} 条"},
                "attempt_no": attempt_no,
                "generated_at": _now(),
                "shards": shards,
            }
            # 索引最后写，且是原子替换：在此之前目录里没有可被回放读取的索引入口。
            _atomic_write_json(self._package_dir(batch_no) / "index.json", index)

            attempt_meta["status"] = DELIVERED
            attempt_meta["record_count"] = record_count
            attempt_meta["finished_at"] = _now()
            attempt_meta["deliverable"] = True
            attempt_meta["index_file"] = "index.json"
            _atomic_write_json(attempt_dir / "attempt.json", attempt_meta)

            meta["delivered_attempt"] = attempt_no
            meta["status"] = DELIVERED
            meta["updated_at"] = _now()
            self._save_package(meta)

            log_row["交付状态"] = DELIVERED
            log_row["可回放"] = "是"
            log_row["索引文件"] = "index.json"
            log_row["日志级别"] = "信息"
            log_row["日志内容"] = (
                f"增量批次包 {batch_no} 第 {attempt_no} 次切分完成：来源 {source}，"
                f"游标 {_format_range(start, end)}，按每片 {shard_size} 条切分，共 {len(shards)} 片、"
                f"{record_count} 条；索引已发布，可作为回放输入"
            )
            log_row["处理状态"] = "已处理"
            log_row["status"] = "已处理"
            log_row["pending"] = False
            log_row["abnormal"] = False

            return {
                "ok": True,
                "batch_no": batch_no,
                "attempt_no": attempt_no,
                "status": meta["status"],
                "message": (
                    f"增量批次包 {batch_no} 第 {attempt_no} 次切分已交付："
                    f"{len(shards)} 片、{record_count} 条，每片 {shard_size} 条"
                ),
                "index": index,
                "package": self.get_package(batch_no),
            }

        except (BatchWriteError, ShardSizeDrift, OSError) as exc:
            message = str(exc)
            attempt_meta["status"] = FAILED
            attempt_meta["failure"] = message
            attempt_meta["finished_at"] = _now()
            attempt_meta["deliverable"] = False
            _atomic_write_json(attempt_dir / "attempt.json", attempt_meta)

            # 已交付的旧索引保持不动；本次失败不产生新索引，旧索引的尝试号仍然有效。
            meta["status"] = DELIVERED if meta.get("delivered_attempt") is not None else UNDELIVERED
            meta["updated_at"] = _now()
            self._save_package(meta)

            log_row["交付状态"] = UNDELIVERED
            log_row["可回放"] = "否"
            log_row["日志级别"] = "错误"
            log_row["错误堆栈"] = message
            log_row["日志内容"] = (
                f"增量批次包 {batch_no} 第 {attempt_no} 次切分失败：来源 {source}，"
                f"游标 {_format_range(start, end)}，按每片 {shard_size} 条切分；"
                f"已写入 {len(shards)} 片，实际覆盖 "
                f"{log_row['实际覆盖段'] or '无'}；本次未交付，中间片不进入索引"
            )
            log_row["处理状态"] = "未处理"
            log_row["status"] = "未处理"
            log_row["pending"] = True
            log_row["abnormal"] = True

            return {
                "ok": False,
                "batch_no": batch_no,
                "attempt_no": attempt_no,
                "status": meta["status"],
                "message": message,
                "package": self.get_package(batch_no),
            }

    @staticmethod
    def _live_shard_size(
        cursor_entry: dict[str, Any] | None,
        snapshot: int,
        follows_cursor: bool,
        drift_size: int | None,
        shards_written: int,
    ) -> int:
        """模拟现场在切分途中修改分片条数；真实场景读的是游标当前的批次大小。"""
        if drift_size is not None and shards_written >= 1:
            return drift_size
        if cursor_entry is None or not follows_cursor:
            return snapshot
        raw = str(cursor_entry.get("批次大小") or "").strip()
        if not raw:
            return snapshot
        try:
            current = int(raw)
        except ValueError:
            return snapshot
        return current if current > 0 else snapshot

    # ------------------------------------------------------------------ 校验/读取
    def _validate_index(self, meta: dict[str, Any], index: dict[str, Any]) -> list[str]:
        problems: list[str] = []
        shards = sorted(index.get("shards") or [], key=lambda item: int(item["cursor_start"]))
        if not shards:
            return ["索引里没有任何分片"]
        if int(index.get("cursor_start", -1)) != int(meta["cursor_start"]):
            problems.append("索引起始游标与批次包不一致")
        if int(index.get("cursor_end", -1)) != int(meta["cursor_end"]):
            problems.append("索引结束游标与批次包不一致")
        if len(shards) != int(index.get("shard_count", -1)):
            problems.append("索引声明的分片数与实际条目不一致")

        expected = int(meta["cursor_start"])
        shard_size = int(index.get("shard_size") or 0)
        for position, shard in enumerate(shards, start=1):
            start = int(shard["cursor_start"])
            end = int(shard["cursor_end"])
            count = int(shard["record_count"])
            if start != expected:
                if start > expected:
                    problems.append(f"分片 {position} 之前缺少游标 {_format_range(expected, start - 1)}")
                else:
                    problems.append(f"分片 {position} 与上一片在游标 {start} 处重叠")
            if end < start:
                problems.append(f"分片 {position} 的起止游标倒置")
            if count != end - start + 1:
                problems.append(f"分片 {position} 记录数 {count} 与游标区间 {start}-{end} 不符")
            is_last = position == len(shards)
            if shard_size and not is_last and count != shard_size:
                problems.append(
                    f"分片 {position} 为 {count} 条，与索引声明的每片 {shard_size} 条不一致"
                )
            if shard_size and is_last and count > shard_size:
                problems.append(f"最后一片 {count} 条超过每片 {shard_size} 条")
            expected = max(expected, end + 1)

            file_rel = str(shard.get("file") or "")
            if not file_rel or ".." in Path(file_rel).parts or Path(file_rel).is_absolute():
                problems.append(f"分片 {position} 的文件路径不安全")
                continue
            file_path = self._package_dir(meta["batch_no"]) / file_rel
            if not file_path.exists():
                problems.append(f"分片 {position} 的文件缺失：{file_rel}")
                continue
            if shard.get("sha256") and _sha256(file_path) != shard["sha256"]:
                problems.append(f"分片 {position} 的文件校验和不符")
                continue
            try:
                payload = _read_json(file_path)
            except (OSError, json.JSONDecodeError):
                problems.append(f"分片 {position} 的文件无法读取")
                continue
            if int(payload.get("record_count", -1)) != count:
                problems.append(f"分片 {position} 文件内记录数与索引不一致")
            if int(payload.get("cursor_start", -1)) != start or int(payload.get("cursor_end", -1)) != end:
                problems.append(f"分片 {position} 文件内游标区间与索引不一致")

        if expected <= int(meta["cursor_end"]):
            problems.append(f"索引缺少游标 {_format_range(expected, meta['cursor_end'])}")
        return problems

    def delivered_index(self, batch_no: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """只有「索引存在且逐片校验通过」才返回；否则给出可读的拒绝原因。"""
        meta = self._load_package(batch_no)
        attempt_no = meta.get("delivered_attempt")
        if attempt_no is None:
            raise BatchPackageError(
                f"增量批次包 {batch_no} 尚未交付：没有成功完成的切分，缺少完整索引，"
                "不能作为回放输入（未交付尝试可在采集日志核对）",
                status=409,
                code="not_delivered",
            )
        index_path = self._package_dir(batch_no) / "index.json"
        if not index_path.exists():
            raise BatchPackageError(
                f"增量批次包 {batch_no} 缺少索引文件，不能作为回放输入",
                status=409,
                code="index_missing",
            )
        index = _read_json(index_path)
        if not isinstance(index, dict):
            raise BatchPackageError(
                f"增量批次包 {batch_no} 的索引格式不正确，不能作为回放输入",
                status=409,
                code="index_invalid",
            )
        if int(index.get("attempt_no", -1)) != int(attempt_no):
            raise BatchPackageError(
                f"增量批次包 {batch_no} 的索引与已交付尝试不一致，拒绝回放",
                status=409,
                code="index_stale",
            )
        problems = self._validate_index(meta, index)
        if problems:
            raise BatchPackageError(
                f"增量批次包 {batch_no} 的分片不齐备，不能作为回放输入：{'；'.join(problems[:3])}",
                status=409,
                code="incomplete_package",
            )
        return meta, index

    def get_package(self, batch_no: str) -> dict[str, Any]:
        meta = self._load_package(batch_no)
        delivered_attempt = meta.get("delivered_attempt")
        index: dict[str, Any] | None = None
        index_error = ""
        try:
            _, index = self.delivered_index(batch_no)
        except BatchPackageError as exc:
            index_error = exc.message

        attempts = []
        for attempt in sorted(meta["attempts"], key=lambda item: int(item["attempt_no"])):
            item = dict(attempt)
            item["可回放"] = index is not None and int(item["attempt_no"]) == int(delivered_attempt or -1)
            item["交付状态"] = DELIVERED if item["可回放"] else UNDELIVERED
            item["切分口径"] = f"每片 {item['shard_size']} 条"
            item["覆盖游标区间"] = _format_range(item["cursor_start"], item["cursor_end"])
            item["实际覆盖段"] = (
                _format_range(item["shards"][0]["cursor_start"], item["shards"][-1]["cursor_end"])
                if item.get("shards")
                else ""
            )
            attempts.append(item)

        return {
            "batch_no": meta["batch_no"],
            "source": meta["source"],
            "cursor_code": meta.get("cursor_code", ""),
            "cursor_start": meta["cursor_start"],
            "cursor_end": meta["cursor_end"],
            "status": meta["status"],
            "delivered_attempt": delivered_attempt,
            "deliverable": index is not None,
            "index_error": index_error,
            "created_at": meta.get("created_at", ""),
            "updated_at": meta.get("updated_at", ""),
            "index": index,
            "attempts": attempts,
            "logs": self.logs_for(batch_no),
        }

    def list_packages(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        source: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        packages: list[dict[str, Any]] = []
        for directory in sorted(self.root.iterdir()):
            if not directory.is_dir() or not (directory / "package.json").exists():
                continue
            try:
                packages.append(self.get_package(directory.name))
            except (BatchPackageError, OSError, json.JSONDecodeError):
                continue
        if keyword:
            packages = [
                item for item in packages
                if keyword in item["batch_no"] or keyword in str(item.get("source", ""))
            ]
        if status:
            packages = [item for item in packages if item["status"] == status]
        if source:
            packages = [item for item in packages if item["source"] == source]
        packages.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        total = len(packages)
        start = max(page - 1, 0) * size
        return packages[start:start + size], total

    def logs_for(self, batch_no: str) -> list[dict[str, Any]]:
        rows = store.rows("log_viewer")
        return [
            row for row in rows
            if str(row.get("批次包编号") or "") == batch_no
        ]

    # ------------------------------------------------------------------ 核对/回放
    def coverage(
        self,
        batch_no: str,
        start: int | None = None,
        end: int | None = None,
        *,
        attempt_no: int | None = None,
    ) -> dict[str, Any]:
        """按索引（或未交付尝试的日志）列出逐片区间，并核对相邻分片是否覆盖目标游标段。"""
        meta = self._load_package(batch_no)
        delivered = meta.get("delivered_attempt")
        if attempt_no is None:
            _, index = self.delivered_index(batch_no)
            shards = sorted(index["shards"], key=lambda item: int(item["cursor_start"]))
            shard_size = int(index["shard_size"])
            attempt = int(index["attempt_no"])
            deliverable = True
            package_range = (int(meta["cursor_start"]), int(meta["cursor_end"]))
        else:
            attempt_meta = next(
                (item for item in meta["attempts"] if int(item["attempt_no"]) == int(attempt_no)),
                None,
            )
            if attempt_meta is None:
                raise BatchPackageError(f"批次包 {batch_no} 没有第 {attempt_no} 次切分", status=404)
            shards = sorted(attempt_meta.get("shards") or [], key=lambda item: int(item["cursor_start"]))
            shard_size = int(attempt_meta["shard_size"])
            attempt = int(attempt_no)
            deliverable = bool(delivered is not None and int(delivered) == attempt)
            package_range = (int(attempt_meta["cursor_start"]), int(attempt_meta["cursor_end"]))
            if not shards:
                raise BatchPackageError(f"第 {attempt_no} 次切分没有写入任何分片", status=404)

        range_start = package_range[0] if start is None else int(start)
        range_end = package_range[1] if end is None else int(end)
        if range_end < range_start:
            raise BatchPackageError("核对范围的结束游标不能小于起始游标")

        gaps: list[list[int]] = []
        overlaps: list[list[int]] = []
        expected = int(shards[0]["cursor_start"])
        for shard in shards:
            shard_start = int(shard["cursor_start"])
            shard_end = int(shard["cursor_end"])
            if shard_start > expected:
                gaps.append([expected, shard_start - 1])
            elif shard_start < expected:
                overlaps.append([shard_start, min(shard_end, expected - 1)])
            expected = max(expected, shard_end + 1)

        in_range = [
            shard for shard in shards
            if int(shard["cursor_end"]) >= range_start and int(shard["cursor_start"]) <= range_end
        ]
        missing: list[list[int]] = []
        cursor = range_start
        for shard in in_range:
            shard_start = int(shard["cursor_start"])
            shard_end = int(shard["cursor_end"])
            if shard_start > cursor:
                missing.append([cursor, min(shard_start - 1, range_end)])
            cursor = max(cursor, shard_end + 1)
        if cursor <= range_end:
            missing.append([cursor, range_end])

        non_last = [int(shard["record_count"]) for shard in shards[:-1]]
        consistent = not non_last or (len(set(non_last)) == 1 and non_last[0] == shard_size)

        return {
            "batch_no": batch_no,
            "attempt_no": attempt,
            "deliverable": deliverable,
            "range": {"start": range_start, "end": range_end},
            "package_range": {"start": package_range[0], "end": package_range[1]},
            "shard_size": shard_size,
            "shard_count": len(shards),
            "granularity_consistent": consistent,
            "adjacent": not gaps and not overlaps,
            "covered": not missing and not gaps and not overlaps,
            "gaps": gaps,
            "overlaps": overlaps,
            "missing_in_range": missing,
            "shards": shards,
            "note": (
                ("相邻分片首尾相接，已覆盖待回放游标位置" if deliverable else
                 "本次切分未交付，下列区间只能用于核对，不能作为回放输入")
                if not gaps and not overlaps and not missing
                else "分片没有完整覆盖待回放游标位置，不能用于回放"
            ),
        }

    def replay(self, batch_no: str, cursor: int, attempt_no: int | None = None) -> dict[str, Any]:
        meta = self._load_package(batch_no)
        delivered_attempt = meta.get("delivered_attempt")
        if attempt_no is not None and int(attempt_no) != int(delivered_attempt or -1):
            attempt = next(
                (item for item in meta["attempts"] if int(item["attempt_no"]) == int(attempt_no)),
                None,
            )
            if attempt is None:
                raise BatchPackageError(f"批次包 {batch_no} 没有第 {attempt_no} 次切分", status=404)
            raise BatchPackageError(
                f"第 {attempt_no} 次切分未交付（{attempt.get('status')}），只能从采集日志核对，"
                "不能作为回放输入",
                status=409,
                code="attempt_not_deliverable",
            )

        meta, index = self.delivered_index(batch_no)
        cursor = int(cursor)
        shard = next(
            (
                item for item in index["shards"]
                if int(item["cursor_start"]) <= cursor <= int(item["cursor_end"])
            ),
            None,
        )
        if shard is None:
            raise BatchPackageError(
                f"游标位置 {cursor} 不在批次包 {batch_no} 覆盖的 "
                f"{_format_range(meta['cursor_start'], meta['cursor_end'])} 内",
                status=404,
                code="cursor_out_of_range",
            )

        payload = _read_json(self._package_dir(batch_no) / shard["file"])
        records = payload["records"]
        shelves = self.sources.shelf_codes(records)
        return {
            "batch_no": batch_no,
            "attempt_no": index["attempt_no"],
            "cursor": cursor,
            "shard_no": shard["shard_no"],
            "cursor_start": shard["cursor_start"],
            "cursor_end": shard["cursor_end"],
            "record_count": shard["record_count"],
            "shard_size": index["shard_size"],
            "shelf_range": shard.get("shelf_range", ""),
            "shelves": shelves,
            "records": records,
            "index_shard": shard,
            "note": (
                f"本次盘点按整片回放：分片 {shard['shard_no']} 覆盖游标 "
                f"[{shard['cursor_start']}, {shard['cursor_end']}]，"
                f"货架 {shard.get('shelf_range') or '—'}"
            ),
        }

    # ------------------------------------------------------------------ 下载
    def download(self, batch_no: str) -> bytes:
        """把已交付的包打成 zip；调用前必须通过完整校验。"""
        meta, index = self.delivered_index(batch_no)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("index.json", json.dumps(index, ensure_ascii=False, indent=2))
            archive.writestr(
                "package.json",
                json.dumps(
                    {
                        "batch_no": meta["batch_no"],
                        "source": meta["source"],
                        "cursor_start": meta["cursor_start"],
                        "cursor_end": meta["cursor_end"],
                        "attempt_no": index["attempt_no"],
                        "shard_size": index["shard_size"],
                        "shard_count": index["shard_count"],
                        "record_count": index["record_count"],
                        "delivered_at": meta.get("updated_at", ""),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
            )
            archive.writestr(
                "README.txt",
                "增量批次包：index.json 逐片列出起止游标与记录数，shards/ 下为变更明细。\n"
                "回放前请先核对索引分片首尾相接、记录数与游标区间一致。\n",
            )
            for shard in index["shards"]:
                path = self._package_dir(batch_no) / shard["file"]
                archive.writestr(f"shards/shard-{int(shard['shard_no']):04d}.json", path.read_bytes())
        return buffer.getvalue()

    def shard_file(self, batch_no: str, shard_no: int) -> dict[str, Any]:
        _, index = self.delivered_index(batch_no)
        shard = next(
            (item for item in index["shards"] if int(item["shard_no"]) == int(shard_no)),
            None,
        )
        if shard is None:
            raise BatchPackageError(f"批次包 {batch_no} 没有分片 {shard_no}", status=404)
        return _read_json(self._package_dir(batch_no) / shard["file"])

    def delete_package(self, batch_no: str) -> None:
        import shutil

        path = self._package_dir(batch_no)
        if not (path / "package.json").exists():
            raise BatchPackageError(f"增量批次包 {batch_no} 不存在", status=404, code="not_found")
        shutil.rmtree(path)


_service: BatchPackageService | None = None


def get_batch_service() -> BatchPackageService:
    global _service
    if _service is None:
        _service = BatchPackageService()
    return _service


def set_batch_service(service: BatchPackageService | None) -> None:
    """测试注入：替换默认单例（传 None 恢复）。"""
    global _service
    _service = service
