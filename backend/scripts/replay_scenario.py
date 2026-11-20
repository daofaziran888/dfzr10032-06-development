#!/usr/bin/env python3
"""复现「游标 180-209 货架变更回放包」的整段口径，供现场复核。

用法（在 backend 目录下）::

    python3 scripts/replay_scenario.py

脚本自带临时落盘目录，不会污染正式批次包目录。
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.services.batch_package import BatchPackageService, set_batch_service  # noqa: E402


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="shelf-replay-") as workdir:
        set_batch_service(BatchPackageService(Path(workdir) / "packages"))
        client = TestClient(app)

        section("1. 首次切分：每片 10 条，第二片写入失败")
        first = client.post(
            "/api/incremental/batches",
            json={
                "values": {
                    "来源标识": "货架变更",
                    "起始游标": 180,
                    "结束游标": 209,
                    "分片条数": 10,
                    "模拟写入失败于第几片": 2,
                }
            },
        ).json()
        batch_no = first["entry"]["batch_no"]
        print("批次包:", batch_no, "| 结果:", first["message"])
        print("可回放:", first["entry"]["deliverable"], "| 索引:", first["entry"]["index"])
        for attempt in first["entry"]["attempts"]:
            print(
                f"  第 {attempt['attempt_no']} 次：{attempt['status']}，"
                f"{attempt['切分口径']}，覆盖 {attempt['覆盖游标区间']}，"
                f"实际写入 {attempt['实际覆盖段'] or '无'}，可回放={attempt['可回放']}"
            )

        section("2. 首次切分未交付：下载/回放/索引都被拒绝")
        for path in ("index", "download"):
            response = client.get(f"/api/incremental/batches/{batch_no}/{path}")
            print(f"  GET /{path} ->", response.status_code, response.json().get("detail", "zip"))
        response = client.get(
            f"/api/incremental/batches/{batch_no}/replay", params={"cursor": 188}
        )
        print("  重放游标 188 ->", response.status_code, response.json()["detail"])

        section("3. 重试：现场改为每片 6 条，整包重切")
        retry = client.post(
            f"/api/incremental/batches/{batch_no}/retry", json={"values": {"分片条数": 6}}
        ).json()
        print("结果:", retry["message"])
        package = retry["entry"]
        print("已交付尝试:", package["delivered_attempt"], "| 索引口径:", package["index"]["shard_size"], "条/片")
        print("逐片索引:")
        for shard in package["index"]["shards"]:
            print(
                f"  分片 {shard['shard_no']}: 游标 [{shard['cursor_start']}, {shard['cursor_end']}] "
                f"{shard['record_count']} 条，货架 {shard['shelf_range']}"
            )

        section("4. 覆盖核对：相邻分片是否覆盖 180-209")
        coverage = client.get(
            f"/api/incremental/batches/{batch_no}/coverage", params={"start": 180, "end": 209}
        ).json()
        print(coverage["note"], "| 缺口:", coverage["gaps"], "| 缺失:", coverage["missing_in_range"])

        section("5. 整片回放：选取包含 188 的分片")
        replay = client.get(
            f"/api/incremental/batches/{batch_no}/replay", params={"cursor": 188}
        ).json()
        print(replay["note"])
        print("本次盘点会重放的货架:", "、".join(replay["shelves"]))

        section("6. 未交付的第一次切分只能核对，不能被选作回放")
        refused = client.get(
            f"/api/incremental/batches/{batch_no}/replay",
            params={"cursor": 188, "attempt": 1},
        )
        print("重放第 1 次 ->", refused.status_code, refused.json()["detail"])
        logged = client.get(
            f"/api/incremental/batches/{batch_no}/coverage",
            params={"start": 180, "end": 209, "attempt": 1},
        ).json()
        print("第 1 次日志核对:", logged["note"], "| 缺失段:", logged["missing_in_range"])

        section("7. 采集日志（按批次包登记每次切分）")
        detail = client.get(f"/api/incremental/batches/{batch_no}").json()
        for log in detail["logs"]:
            print(
                f"  {log['日志编号']}: {log['交付状态']}，{log['切分口径']}，"
                f"覆盖 {log['覆盖游标区间']}，实际 {log['实际覆盖段']}，"
                f"分片货架区间 {[shard['shelf_range'] for shard in log['分片明细']]}"
            )

        print("\n回放包内索引片段（下载接口返回的 index.json）：")
        print(json.dumps(package["index"], ensure_ascii=False, indent=2)[:600] + " ...")

        set_batch_service(None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
