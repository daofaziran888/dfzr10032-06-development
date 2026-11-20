"""运行配置：端口、跨域、运行环境与批次包落盘位置。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    app_name: str = "多源数据采集与RPA自动化平台"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 增量批次包的落盘目录；测试和演示可以用 APP_PACKAGE_ROOT 指到临时目录。
    package_root: str = field(
        default_factory=lambda: os.environ.get(
            "APP_PACKAGE_ROOT", str(_BACKEND_DIR / "var" / "packages")
        )
    )
    # 只在本地/测试环境开放「调试故障分片数」，用来演练写入失败后的重试口径。
    allow_debug_fault: bool = field(
        default_factory=lambda: os.environ.get("APP_ENV", "local") != "production"
    )


settings = Settings()
