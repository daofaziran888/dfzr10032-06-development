"""增量批次包的核心口径回归：原子交付、重试隔离、口径漂移与整片回放。"""
from __future__ import annotations

import json
import zipfile
from io import BytesIO

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store

SHELF = "货架变更"


@pytest.fixture()
def client():
    return TestClient(app)


def _create(client: TestClient, **values):
    return client.post("/api/incremental/batches", json={"values": values})


def _delivered(client: TestClient, **values) -> dict:
    response = _create(client, **values)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ok"], body["message"]
    return body["entry"]


# --------------------------------------------------------------------- 基础切分
def test_package_splits_range_and_index_lists_each_shard(client):
    package = _delivered(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10
    )
    assert package["status"] == "已交付"
    assert package["deliverable"] is True
    index = package["index"]
    assert (index["cursor_start"], index["cursor_end"]) == (180, 209)
    assert index["record_count"] == 30
    assert index["shard_size"] == 10
    assert [(s["cursor_start"], s["cursor_end"], s["record_count"]) for s in index["shards"]] == [
        (180, 189, 10),
        (190, 199, 10),
        (200, 209, 10),
    ]
    assert index["shards"][0]["shelf_range"] == "SH-0180~SH-0189"


def test_index_is_not_published_until_every_shard_is_written(client, isolated_state):
    response = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟写入失败于第几片=2,
    )
    body = response.json()
    assert body["ok"] is False
    package = body["entry"]
    assert package["deliverable"] is False
    assert package["delivered_attempt"] is None
    package_dir = isolated_state.root / package["batch_no"]
    assert not (package_dir / "index.json").exists()
    # 中间片确实写过，但不会被任何索引引用。
    assert (package_dir / "attempts/attempt-0001/shards/shard-0001.json").exists()
    assert (package_dir / "attempts/attempt-0001/shards/shard-0002.json").exists()


def test_undelivered_package_cannot_be_downloaded_or_replayed(client, isolated_state):
    body = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟写入失败于第几片=1,
    ).json()
    batch_no = body["entry"]["batch_no"]
    assert client.get(f"/api/incremental/batches/{batch_no}/index").status_code == 409
    assert client.get(f"/api/incremental/batches/{batch_no}/download").status_code == 409
    replay = client.get(f"/api/incremental/batches/{batch_no}/replay", params={"cursor": 188})
    assert replay.status_code == 409
    assert "未交付" in replay.json()["detail"]


def test_download_rejects_package_with_missing_shard_file(client, isolated_state):
    package = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10)
    shard_file = isolated_state.root / package["batch_no"] / package["index"]["shards"][1]["file"]
    shard_file.unlink()
    response = client.get(f"/api/incremental/batches/{package['batch_no']}/download")
    assert response.status_code == 409
    assert "分片不齐备" in response.json()["detail"]


def test_download_rejects_tampered_shard(client, isolated_state):
    package = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10)
    shard_file = isolated_state.root / package["batch_no"] / package["index"]["shards"][0]["file"]
    payload = json.loads(shard_file.read_text())
    payload["records"].pop()
    shard_file.write_text(json.dumps(payload))
    response = client.get(f"/api/incremental/batches/{package['batch_no']}/index")
    assert response.status_code == 409
    assert "校验和" in response.json()["detail"]


def test_download_zip_contains_index_and_shards(client):
    package = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=6)
    response = client.get(f"/api/incremental/batches/{package['batch_no']}/download")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(BytesIO(response.content)) as archive:
        names = set(archive.namelist())
        assert "index.json" in names
        assert "shards/shard-0005.json" in names
        index = json.loads(archive.read("index.json"))
        assert index["shard_count"] == 5


# --------------------------------------------------------------------- 重试
def test_retry_keeps_previous_middle_shards_out_of_the_new_index(client, isolated_state):
    first = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟写入失败于第几片=1,
    ).json()["entry"]
    batch_no = first["batch_no"]

    retry = client.post(
        f"/api/incremental/batches/{batch_no}/retry", json={"values": {"分片条数": 6}}
    )
    assert retry.status_code == 200, retry.text
    package = retry.json()["entry"]
    assert package["status"] == "已交付"
    assert package["delivered_attempt"] == 2
    index = package["index"]
    assert index["attempt_no"] == 2
    assert index["shard_size"] == 6
    assert [s["cursor_start"] for s in index["shards"]] == [180, 186, 192, 198, 204]
    assert all("attempt-0002" in s["file"] for s in index["shards"])
    # 第一次尝试的 10 条中间片仍在原地，但不属于索引。
    assert (isolated_state.root / batch_no / "attempts/attempt-0001/shards/shard-0001.json").exists()

    attempts = {item["attempt_no"]: item for item in package["attempts"]}
    assert attempts[1]["交付状态"] == "未交付"
    assert attempts[1]["可回放"] is False
    assert attempts[1]["实际覆盖段"] == "[180, 189]"
    assert attempts[2]["交付状态"] == "已交付"
    assert attempts[2]["可回放"] is True


def test_retry_rejects_changed_source_or_range(client):
    package = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10)
    other_source = client.post(
        f"/api/incremental/batches/{package['batch_no']}/retry",
        json={"values": {"来源标识": "订单变更"}},
    )
    assert other_source.status_code == 400
    assert "来源" in other_source.json()["detail"]

    other_range = client.post(
        f"/api/incremental/batches/{package['batch_no']}/retry",
        json={"values": {"结束游标": 210}},
    )
    assert other_range.status_code == 400
    assert "游标范围" in other_range.json()["detail"]


def test_retry_does_not_replace_delivered_index_when_it_fails(client):
    package = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10)
    batch_no = package["batch_no"]
    index_before = client.get(f"/api/incremental/batches/{batch_no}/index").json()

    failed = client.post(
        f"/api/incremental/batches/{batch_no}/retry",
        json={"values": {"分片条数": 6, "模拟写入失败于第几片": 2}},
    ).json()
    assert failed["ok"] is False
    assert failed["entry"]["status"] == "已交付"
    assert failed["entry"]["delivered_attempt"] == 1

    index_after = client.get(f"/api/incremental/batches/{batch_no}/index").json()
    assert index_after == index_before
    assert index_after["shard_size"] == 10


# --------------------------------------------------------------------- 口径
def test_shard_size_drift_aborts_attempt_instead_of_mixing_granularity(client, isolated_state):
    response = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟途中改为每片条数=6,
    )
    body = response.json()
    assert body["ok"] is False
    package = body["entry"]
    assert package["deliverable"] is False
    assert package["index"] is None
    attempt = package["attempts"][0]
    assert "口径" in attempt["failure"]
    assert all(shard["record_count"] == 10 for shard in attempt["shards"])
    assert not (isolated_state.root / package["batch_no"] / "index.json").exists()


def test_cursor_batch_size_change_mid_split_aborts_attempt(client, isolated_state):
    cursor = next(row for row in store.rows("incremental") if row["游标编号"] == "INCR-0004")
    assert cursor["批次大小"] == "10"

    original = isolated_state.sources.records

    def drifting(source, start, end):
        records = original(source, start, end)
        cursor["批次大小"] = "6"  # 现场在第一片写完后改了游标的批次大小
        return records

    isolated_state.sources.records = drifting  # type: ignore[assignment]
    body = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 游标编号="INCR-0004"
    ).json()
    assert body["ok"] is False
    assert body["entry"]["attempts"][0]["shard_size"] == 10
    assert "口径" in body["entry"]["attempts"][0]["failure"]

    # 重试按新的批次大小整包重切，索引里只有 6 条口径。
    retry = client.post(
        f"/api/incremental/batches/{body['entry']['batch_no']}/retry", json={"values": {}}
    ).json()
    assert retry["ok"] is True
    assert retry["entry"]["index"]["shard_size"] == 6
    assert [s["record_count"] for s in retry["entry"]["index"]["shards"]] == [6, 6, 6, 6, 6]


def test_shard_size_follows_cursor_batch_size(client):
    package = _delivered(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 游标编号="INCR-0004"
    )
    assert package["index"]["shard_size"] == 10
    assert package["index"]["shard_count"] == 3


# --------------------------------------------------------------------- 覆盖/回放
def test_coverage_reports_adjacent_shards_and_gaps(client, isolated_state):
    package = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=6)
    batch_no = package["batch_no"]

    covered = client.get(
        f"/api/incremental/batches/{batch_no}/coverage", params={"start": 180, "end": 209}
    ).json()
    assert covered["covered"] is True
    assert covered["adjacent"] is True
    assert covered["missing_in_range"] == []

    # 人为抹掉中间一片，核对必须报出缺失的游标段，而不是当成「没有变更」。
    shard = package["index"]["shards"][2]
    (isolated_state.root / batch_no / shard["file"]).unlink()
    refused = client.get(
        f"/api/incremental/batches/{batch_no}/coverage", params={"start": 180, "end": 209}
    )
    assert refused.status_code == 409
    assert "分片不齐备" in refused.json()["detail"]


def test_coverage_flags_internal_gap_from_logged_attempt(client):
    body = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟写入失败于第几片=1,
    ).json()
    batch_no = body["entry"]["batch_no"]
    coverage = client.get(
        f"/api/incremental/batches/{batch_no}/coverage",
        params={"start": 180, "end": 209, "attempt": 1},
    ).json()
    assert coverage["covered"] is False
    assert coverage["deliverable"] is False
    assert coverage["adjacent"] is True  # 已写的片首尾相接，但没覆盖到 209
    assert coverage["missing_in_range"] == [[190, 209]]


def test_replay_selects_the_whole_shard_containing_cursor(client):
    package = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10)
    replay = client.get(
        f"/api/incremental/batches/{package['batch_no']}/replay", params={"cursor": 188}
    ).json()
    assert replay["shard_no"] == 1
    assert (replay["cursor_start"], replay["cursor_end"]) == (180, 189)
    assert replay["record_count"] == 10
    assert replay["shelves"][0] == "SH-0180"
    assert replay["shelves"][-1] == "SH-0189"
    assert {record["游标位置"] for record in replay["records"]} == set(range(180, 190))


def test_replay_with_retry_uses_delivered_granularity(client):
    first = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟写入失败于第几片=1,
    ).json()["entry"]
    batch_no = first["batch_no"]
    client.post(f"/api/incremental/batches/{batch_no}/retry", json={"values": {"分片条数": 6}})

    replay = client.get(
        f"/api/incremental/batches/{batch_no}/replay", params={"cursor": 188}
    ).json()
    assert replay["shard_no"] == 2
    assert (replay["cursor_start"], replay["cursor_end"]) == (186, 191)
    assert replay["record_count"] == 6
    assert replay["shelves"] == ["SH-0186", "SH-0187", "SH-0188", "SH-0189", "SH-0190", "SH-0191"]

    # 未交付的第一次切分不能被选作回放输入。
    rejected = client.get(
        f"/api/incremental/batches/{batch_no}/replay",
        params={"cursor": 188, "attempt": 1},
    )
    assert rejected.status_code == 409
    assert "未交付" in rejected.json()["detail"]


def test_replay_rejects_cursor_outside_package(client):
    package = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10)
    response = client.get(
        f"/api/incremental/batches/{package['batch_no']}/replay", params={"cursor": 210}
    )
    assert response.status_code == 404
    assert "不在批次包" in response.json()["detail"]


# --------------------------------------------------------------------- 日志
def test_each_attempt_logs_actual_covered_range_and_shelf_ranges(client):
    first = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟写入失败于第几片=1,
    ).json()["entry"]
    batch_no = first["batch_no"]
    client.post(f"/api/incremental/batches/{batch_no}/retry", json={"values": {"分片条数": 6}})

    package = client.get(f"/api/incremental/batches/{batch_no}").json()
    logs = {log["尝试序号"]: log for log in package["logs"]}
    assert set(logs) == {1, 2}

    failed = logs[1]
    assert failed["交付状态"] == "未交付"
    assert failed["可回放"] == "否"
    assert failed["覆盖游标区间"] == "[180, 209]"
    assert failed["实际覆盖段"] == "[180, 189]"
    assert failed["切分口径"] == "每片 10 条"
    assert [shard["shelf_range"] for shard in failed["分片明细"]] == ["SH-0180~SH-0189"]

    delivered = logs[2]
    assert delivered["交付状态"] == "已交付"
    assert delivered["可回放"] == "是"
    assert delivered["实际覆盖段"] == "[180, 209]"
    assert delivered["切分口径"] == "每片 6 条"
    assert [shard["shelf_range"] for shard in delivered["分片明细"]] == [
        "SH-0180~SH-0185",
        "SH-0186~SH-0191",
        "SH-0192~SH-0197",
        "SH-0198~SH-0203",
        "SH-0204~SH-0209",
    ]

    # 采集日志模块的列表接口也能按批次包编号筛到这两条。
    rows = client.get(
        "/api/log_viewer", params={"批次包编号": batch_no, "size": 50}
    ).json()["items"]
    assert {row["尝试序号"] for row in rows} == {1, 2}


def test_log_records_only_reachable_shards_after_write_failure(client):
    body = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟写入失败于第几片=2,
    ).json()
    log = body["entry"]["logs"][0]
    assert log["实际覆盖段"] == "[180, 199]"
    assert len(log["分片明细"]) == 2


# --------------------------------------------------------------------- 描述
def test_package_summary_marks_unqueryable_attempts(client):
    first = _create(
        client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10,
        模拟写入失败于第几片=1,
    ).json()["entry"]
    detail = client.get(f"/api/incremental/batches/{first['batch_no']}").json()
    attempt = detail["attempts"][0]
    assert attempt["切分口径"] == "每片 10 条"
    assert attempt["覆盖游标区间"] == "[180, 209]"
    assert attempt["实际覆盖段"] == "[180, 189]"
    assert detail["index_error"]
    assert detail["deliverable"] is False


def test_list_packages_supports_filters(client):
    shelf = _delivered(client, 来源标识=SHELF, 起始游标=180, 结束游标=209, 分片条数=10)
    _delivered(client, 来源标识="订单变更", 起始游标=1, 结束游标=5, 分片条数=2)
    failed = _create(
        client, 来源标识=SHELF, 起始游标=300, 结束游标=320, 分片条数=10,
        模拟写入失败于第几片=1,
    ).json()["entry"]

    by_source = client.get("/api/incremental/batches", params={"source": SHELF}).json()
    assert by_source["total"] == 2
    undelivered = client.get("/api/incremental/batches", params={"status": "未交付"}).json()
    assert [item["batch_no"] for item in undelivered["items"]] == [failed["batch_no"]]
    keyword = client.get(
        "/api/incremental/batches", params={"keyword": shelf["batch_no"]}
    ).json()
    assert keyword["total"] == 1


def test_service_rejects_bad_inputs(client):
    bad_range = _create(client, 来源标识=SHELF, 起始游标=209, 结束游标=180)
    assert bad_range.status_code == 400
    assert "结束游标" in bad_range.json()["detail"]

    bad_size = _create(client, 来源标识=SHELF, 起始游标=1, 结束游标=10, 分片条数=0)
    assert bad_size.status_code == 400

    missing = _create(client, 起始游标=1, 结束游标=10)
    assert missing.status_code == 400
    assert "来源标识" in missing.json()["detail"]


def test_unknown_cursor_code_is_reported(client):
    response = _create(client, 来源标识=SHELF, 起始游标=1, 结束游标=10, 游标编号="INCR-9999")
    assert response.status_code == 404
    assert "INCR-9999" in response.json()["detail"]
