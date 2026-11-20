"""增量批次包接口：切分打包、索引核对、下载与整片回放。

路由层只做参数整理和错误翻译，切分与校验规则都在 ``app.services.batch_package``。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.batch_package import BatchPackageError, get_batch_service
from app.services.source_changes import SourceChangeService

router = APIRouter(prefix="/api/incremental/batches", tags=["增量批次包"])

sources = SourceChangeService()


def _service():
    return get_batch_service()


def _fail(exc: BatchPackageError) -> HTTPException:
    return HTTPException(status_code=exc.status, detail=exc.message)


@router.get("/sources", response_model=dict)
def list_sources() -> dict[str, Any]:
    """可选来源与默认分片口径，供前端表单提示。"""
    return {
        "sources": ["货架变更", "订单变更", "商品变更"],
        "default_shard_size": 20,
        "package_root": str(_service().root),
        "note": "分片条数留空时跟随游标的批次大小；一次切分只使用一个口径。",
    }


@router.get("/preview", response_model=dict)
def preview_records(
    source: str = Query(..., description="来源标识，例如 货架变更"),
    start: int = Query(..., description="起始游标位置（含）"),
    end: int = Query(..., description="结束游标位置（含）"),
    size: int = Query(default=10, ge=1, le=200),
) -> dict[str, Any]:
    """只读预览源端变更，方便回放前确认游标区间里确实有货架记录。"""
    if end < start:
        raise HTTPException(status_code=400, detail="结束游标不能小于起始游标")
    if end - start + 1 > 500:
        raise HTTPException(status_code=400, detail="预览区间最多 500 个游标位置")
    records = sources.records(source, start, end)
    return {
        "source": source,
        "start": start,
        "end": end,
        "total": len(records),
        "records": records[:size],
    }


@router.get("", response_model=PageResult[dict])
def list_packages(
    keyword: str | None = Query(default=None, description="按批次包编号或来源检索"),
    status: str | None = Query(default=None, description="切分中、已交付、未交付"),
    source: str | None = Query(default=None, description="按来源标识过滤"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """批次包列表；未交付的包同样列出，但 deliverable=false 且没有索引。"""
    items, total = _service().list_packages(
        keyword=keyword, status=status, source=source, page=page, size=size
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("", response_model=ActionResult)
def create_package(payload: EntryPayload) -> ActionResult:
    """切分并打包。

    必填：来源标识、起始游标、结束游标。可选：游标编号（用于跟随批次大小）、
    分片条数、批次包编号（用于对该包重试）。
    """
    try:
        result = _service().create_batch(payload.values)
    except BatchPackageError as exc:
        raise _fail(exc) from exc
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=result["package"],
    )


@router.post("/{batch_no}/retry", response_model=ActionResult)
def retry_package(batch_no: str, payload: EntryPayload) -> ActionResult:
    """按新口径重新切分一次；上一次的中间片保留供核对，但不会进入本次索引。"""
    try:
        result = _service().retry_batch(batch_no, payload.values)
    except BatchPackageError as exc:
        raise _fail(exc) from exc
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=result["package"],
    )


@router.get("/{batch_no}", response_model=dict)
def get_package(batch_no: str) -> dict[str, Any]:
    """批次包详情：索引、每次尝试的口径与实际覆盖段、对应采集日志。"""
    try:
        return _service().get_package(batch_no)
    except BatchPackageError as exc:
        raise _fail(exc) from exc


@router.get("/{batch_no}/index", response_model=dict)
def get_index(batch_no: str) -> dict[str, Any]:
    """只返回已交付且校验通过的索引；未交付时 409 并说明原因。"""
    try:
        _, index = _service().delivered_index(batch_no)
    except BatchPackageError as exc:
        raise _fail(exc) from exc
    return index


@router.get("/{batch_no}/coverage", response_model=dict)
def get_coverage(
    batch_no: str,
    start: int | None = Query(default=None, description="待回放游标起点（含）"),
    end: int | None = Query(default=None, description="待回放游标终点（含）"),
    attempt: int | None = Query(default=None, description="核对某次尝试（含未交付）"),
) -> dict[str, Any]:
    """按索引逐片列出起止游标与记录数，并核对相邻分片是否覆盖待回放游标。"""
    try:
        return _service().coverage(batch_no, start, end, attempt_no=attempt)
    except BatchPackageError as exc:
        raise _fail(exc) from exc


@router.get("/{batch_no}/replay", response_model=dict)
def replay(
    batch_no: str,
    cursor: int = Query(..., description="要重放的游标位置"),
    attempt: int | None = Query(default=None, description="指定尝试，只能选已交付的那次"),
) -> dict[str, Any]:
    """选取包含该游标位置的分片整片重放；未交付的包或尝试一律拒绝。"""
    try:
        return _service().replay(batch_no, cursor, attempt)
    except BatchPackageError as exc:
        raise _fail(exc) from exc


@router.get("/{batch_no}/shards/{shard_no}", response_model=dict)
def get_shard(batch_no: str, shard_no: int) -> dict[str, Any]:
    """读取单个分片的完整明细（仅限已交付的包）。"""
    try:
        return _service().shard_file(batch_no, shard_no)
    except BatchPackageError as exc:
        raise _fail(exc) from exc


@router.get("/{batch_no}/download")
def download_package(batch_no: str) -> Response:
    """下载离线回放包（zip）；分片不齐备时拒绝导出半成品。"""
    try:
        body = _service().download(batch_no)
    except BatchPackageError as exc:
        raise _fail(exc) from exc
    return Response(
        content=body,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{batch_no}.zip"'},
    )
