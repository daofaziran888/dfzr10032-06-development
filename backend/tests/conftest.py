"""测试夹具：每个用例用独立的临时目录，互不污染批次包和采集日志。"""
from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.seed import SEED_ROWS  # noqa: E402
from app.services import batch_package as batch_module  # noqa: E402
from app.services.batch_package import BatchPackageService, set_batch_service  # noqa: E402
from app.store import store  # noqa: E402


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    """重置内存仓库与批次包目录，避免用例之间互相看到对方的日志和索引。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    service = BatchPackageService(tmp_path / "packages")
    set_batch_service(service)
    monkeypatch.setattr(
        batch_module, "settings", replace(batch_module.settings, allow_debug_fault=True)
    )
    yield service
    set_batch_service(None)
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
