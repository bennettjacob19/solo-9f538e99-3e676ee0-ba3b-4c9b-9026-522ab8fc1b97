"""Tests for the Prometheus file_sd target-file publisher."""

from __future__ import annotations

import json

import pytest

from svcdir import DirectoryStore, TargetFilePublisher, create_app
from svcdir.publisher import is_host_port


@pytest.fixture()
def pair(tmp_path):
    store = DirectoryStore(tmp_path / "dir.sqlite3", restore_lease_seconds=30)
    store.initialize()
    publisher = TargetFilePublisher(store, tmp_path / "metrics-targets.json")
    return store, publisher


def read_targets(tmp_path_or_pair, name="metrics-targets.json"):
    path = tmp_path_or_pair / name
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def target_map(path):
    """service label -> address, parsed from the published file."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return {g["labels"]["service"]: g["targets"][0] for g in data}


# ---------------------------------------------------------------------------
# host:port validation


@pytest.mark.parametrize(
    "address",
    [
        "10.0.0.12:8080",
        "orders-api:9000",
        "a:1",
        "host_name:65535",
        "a.b.c:443",
        "[::1]:9090",
        "[2001:db8::1]:8080",
    ],
)
def test_valid_host_port_addresses(address):
    assert is_host_port(address) is True


@pytest.mark.parametrize(
    "address",
    [
        "",
        "10.0.0.12",          # 缺端口
        "10.0.0.12:",         # 空端口
        ":8080",              # 缺主机
        "host:notaport",
        "host:0",
        "host:65536",
        "host:8080 ",         # 尾随空白
        "host :8080",
        "http://host:8080",
        "::1:9090",           # IPv6 必须加方括号
        "host:80:90",
        None,
        8080,
    ],
)
def test_invalid_host_port_addresses(address):
    assert is_host_port(address) is False


# ---------------------------------------------------------------------------
# Basic publishing


def test_metrics_services_become_targets_with_service_label(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-orders", "10.0.0.12:8080")
    store.upsert("plain-service", "10.0.0.13:8080")  # 非 metrics- 前缀，不发布
    result = publisher.publish_current()
    assert result.ok and result.outcome == "published"

    groups = read_targets(tmp_path)
    assert groups == [
        {"targets": ["10.0.0.12:8080"], "labels": {"service": "metrics-orders"}}
    ]
    # 版本边车记录生成该文件的目录版本。
    version = int((tmp_path / "metrics-targets.json.version").read_text().strip())
    assert version == store.current_version_id()


def test_empty_directory_publishes_empty_list(pair, tmp_path):
    store, publisher = pair
    store.upsert("plain", "10.0.0.1:80")
    assert publisher.publish_current().ok
    assert read_targets(tmp_path) == []


def test_invalid_address_is_skipped_but_others_publish(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-good", "10.0.0.1:8000")
    store.upsert("metrics-bad", "not-a-host-port")
    result = publisher.publish_current()
    assert result.ok
    assert result.counts["published"] == 1
    assert result.counts["skipped"] == 1
    assert target_map(tmp_path / "metrics-targets.json") == {
        "metrics-good": "10.0.0.1:8000"
    }


def test_delete_removes_target(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    store.upsert("metrics-b", "10.0.0.2:8000")
    publisher.publish_current()
    store.delete("metrics-a")
    publisher.publish_current()
    assert target_map(tmp_path / "metrics-targets.json") == {
        "metrics-b": "10.0.0.2:8000"
    }


def test_ipv6_target_is_published(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-v6", "[::1]:9090")
    publisher.publish_current()
    assert target_map(tmp_path / "metrics-targets.json") == {"metrics-v6": "[::1]:9090"}


# ---------------------------------------------------------------------------
# Address reuse rule


def test_reuse_old_address_when_continuously_present(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    publisher.publish_current()  # 上次发布：文件里有 10.0.0.1:8000
    store.upsert("metrics-a", "garbage")  # 改成非法地址
    result = publisher.publish_current()
    assert result.counts["reused"] == 1
    # 服务从上次发布起每版都在 → 沿用文件里的旧地址。
    assert target_map(tmp_path / "metrics-targets.json") == {
        "metrics-a": "10.0.0.1:8000"
    }


def test_no_reuse_after_delete_and_recreate_without_publish(pair, tmp_path):
    """删除与同名重建之间没有发布文件，重建也不能沿用旧地址。"""
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    publisher.publish_current()  # 文件里有旧地址
    store.delete("metrics-a")    # 删除后（模拟）发布失败：文件未更新
    store.upsert("metrics-a", "garbage")  # 同名重建，地址非法
    result = publisher.publish_current()
    assert result.counts["reused"] == 0
    assert result.counts["skipped"] == 1
    # 不得沿用旧地址：目标被跳过，文件里不再有该服务。
    assert "metrics-a" not in target_map(tmp_path / "metrics-targets.json")


def test_no_reuse_when_file_version_unknown(pair, tmp_path):
    """版本边车缺失时无法确认连续性，不得沿用。"""
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    publisher.publish_current()
    (tmp_path / "metrics-targets.json.version").unlink()
    store.upsert("metrics-a", "garbage")
    result = publisher.publish_current()
    assert result.counts["reused"] == 0
    assert "metrics-a" not in target_map(tmp_path / "metrics-targets.json")


def test_no_reuse_for_brand_new_service_with_invalid_address(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-other", "10.0.0.9:9000")
    publisher.publish_current()
    store.upsert("metrics-new", "garbage")  # 上次发布时还不存在
    result = publisher.publish_current()
    assert result.counts["skipped"] == 1
    assert target_map(tmp_path / "metrics-targets.json") == {
        "metrics-other": "10.0.0.9:9000"
    }


def test_reuse_survives_unrelated_changes(pair, tmp_path):
    """非法地址期间其它服务正常变更，不影响沿用判断。"""
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    publisher.publish_current()
    store.upsert("metrics-a", "garbage")
    store.upsert("metrics-b", "10.0.0.2:8000")
    store.delete("metrics-b")
    store.upsert("metrics-c", "10.0.0.3:8000")
    publisher.publish_current()
    assert target_map(tmp_path / "metrics-targets.json") == {
        "metrics-a": "10.0.0.1:8000",  # 沿用
        "metrics-c": "10.0.0.3:8000",
    }


# ---------------------------------------------------------------------------
# Late publishes and failure handling


def test_late_publish_does_not_overwrite_newer(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    old_version = store.current_version_id()
    store.upsert("metrics-a", "10.0.0.2:8000")
    assert publisher.publish_current().ok  # 文件已是新版
    late = publisher.publish(old_version)  # 迟到的旧版发布
    assert late.ok and late.outcome == "stale"
    assert target_map(tmp_path / "metrics-targets.json") == {
        "metrics-a": "10.0.0.2:8000"
    }


def test_failed_publish_keeps_original_file(pair, tmp_path, monkeypatch):
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    assert publisher.publish_current().ok
    before = (tmp_path / "metrics-targets.json").read_text(encoding="utf-8")

    def boom(text, version):
        raise OSError("disk full")

    monkeypatch.setattr(publisher, "_write_atomic", boom)
    store.upsert("metrics-a", "10.0.0.2:8000")
    result = publisher.publish_current()
    assert not result.ok and result.outcome == "failed"
    assert publisher.last_error
    # 原文件保留，且状态页标记待重发。
    assert (tmp_path / "metrics-targets.json").read_text(encoding="utf-8") == before
    status = publisher.status()
    assert status["pending"] is True
    assert status["last_error"]

    # 故障恢复后（或重启后）按当前目录补发。
    monkeypatch.undo()
    assert publisher.publish_current().ok
    assert target_map(tmp_path / "metrics-targets.json") == {
        "metrics-a": "10.0.0.2:8000"
    }
    assert publisher.status()["pending"] is False


def test_restart_republishes_current_directory(pair, tmp_path, monkeypatch):
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    monkeypatch.setattr(publisher, "_write_atomic", lambda *a: (_ for _ in ()).throw(OSError("boom")))
    assert not publisher.publish_current().ok  # 发布失败，目录改动已生效
    assert not (tmp_path / "metrics-targets.json").exists()

    # 模拟重启：新发布器实例按当前目录补发。
    restarted = TargetFilePublisher(store, tmp_path / "metrics-targets.json")
    assert restarted.publish_current().ok
    assert target_map(tmp_path / "metrics-targets.json") == {
        "metrics-a": "10.0.0.1:8000"
    }


def test_corrupt_targets_file_is_replaced_without_reuse(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    publisher.publish_current()
    (tmp_path / "metrics-targets.json").write_text("{not json", encoding="utf-8")
    store.upsert("metrics-a", "garbage")
    result = publisher.publish_current()
    assert result.ok
    assert result.counts["reused"] == 0  # 文件损坏 → 无从沿用
    assert "metrics-a" not in target_map(tmp_path / "metrics-targets.json")


def test_publish_is_idempotent_for_same_version(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    assert publisher.publish_current().outcome == "published"
    assert publisher.publish_current().outcome == "unchanged"


def test_no_tmp_files_left_behind(pair, tmp_path):
    store, publisher = pair
    store.upsert("metrics-a", "10.0.0.1:8000")
    publisher.publish_current()
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith(".metrics-targets")]
    assert leftovers == []


# ---------------------------------------------------------------------------
# Wired through the web app (hook-driven)


@pytest.fixture()
def app_pair(tmp_path):
    store = DirectoryStore(tmp_path / "web.sqlite3")
    publisher = TargetFilePublisher(store, tmp_path / "metrics-targets.json")
    app = create_app(store, publisher=publisher)
    app.config.update(TESTING=True)
    return app, store, publisher, tmp_path


def test_changes_republish_automatically_via_hook(app_pair):
    app, store, publisher, tmp_path = app_pair
    client = app.test_client()
    client.post("/api/services", json={"service": "metrics-a", "address": "10.0.0.1:8000"})
    assert target_map(tmp_path / "metrics-targets.json") == {"metrics-a": "10.0.0.1:8000"}
    client.post("/api/services", json={"service": "metrics-a", "address": "10.0.0.2:8000"})
    assert target_map(tmp_path / "metrics-targets.json") == {"metrics-a": "10.0.0.2:8000"}
    client.delete("/api/services/metrics-a")
    assert read_targets(tmp_path) == []


def test_restore_republishes_backup_addresses(app_pair):
    app, store, publisher, tmp_path = app_pair
    client = app.test_client()
    client.post("/api/services", json={"service": "metrics-a", "address": "10.0.0.1:8000"})
    client.post("/api/backups", json={"name": "snap"})
    client.post("/api/services", json={"service": "metrics-a", "address": "10.0.0.2:8000"})
    assert target_map(tmp_path / "metrics-targets.json") == {"metrics-a": "10.0.0.2:8000"}
    resp = client.post("/api/backups/snap/restore")
    assert resp.status_code == 200
    assert target_map(tmp_path / "metrics-targets.json") == {"metrics-a": "10.0.0.1:8000"}


def test_failed_publish_does_not_break_directory_change(app_pair, monkeypatch):
    app, store, publisher, tmp_path = app_pair
    client = app.test_client()
    client.post("/api/services", json={"service": "metrics-a", "address": "10.0.0.1:8000"})
    assert target_map(tmp_path / "metrics-targets.json") == {"metrics-a": "10.0.0.1:8000"}

    monkeypatch.setattr(
        publisher, "_write_atomic", lambda *a: (_ for _ in ()).throw(OSError("disk full"))
    )
    resp = client.post("/api/services", json={"service": "metrics-a", "address": "10.0.0.2:8000"})
    assert resp.status_code == 200  # 目录改动照常生效
    assert client.get("/resolve?name=metrics-a").get_json()["address"] == "10.0.0.2:8000"
    # 原文件保留，首页标记待重发。
    assert target_map(tmp_path / "metrics-targets.json") == {"metrics-a": "10.0.0.1:8000"}
    page = client.get("/").get_data(as_text=True)
    assert "待重发" in page

    monkeypatch.undo()
    resp = client.post("/targets/republish")
    assert resp.status_code == 303
    assert target_map(tmp_path / "metrics-targets.json") == {"metrics-a": "10.0.0.2:8000"}


def test_homepage_shows_side_by_side_addresses_and_statuses(app_pair):
    app, store, publisher, tmp_path = app_pair
    client = app.test_client()
    client.post("/api/services", json={"service": "metrics-ok", "address": "10.0.0.1:8000"})
    client.post("/api/services", json={"service": "metrics-bad", "address": "garbage"})
    page = client.get("/").get_data(as_text=True)
    assert "目录地址" in page and "文件中的地址" in page
    assert "已发布" in page
    assert "跳过" in page
    assert "立即重发目标文件" in page
    # 非法地址改为合法前的沿用场景：
    client.post("/api/services", json={"service": "metrics-ok", "address": "broken"})
    page = client.get("/").get_data(as_text=True)
    assert "沿用" in page


def test_api_targets_and_manual_republish(app_pair):
    app, store, publisher, tmp_path = app_pair
    client = app.test_client()
    client.post("/api/services", json={"service": "metrics-a", "address": "10.0.0.1:8000"})
    resp = client.get("/api/targets")
    assert resp.status_code == 200
    body = resp.get_json()["targets"]
    assert body["pending"] is False
    assert body["rows"][0]["service"] == "metrics-a"
    assert body["rows"][0]["directory_address"] == "10.0.0.1:8000"
    assert body["rows"][0]["file_address"] == "10.0.0.1:8000"
    assert body["rows"][0]["status"] == "published"

    resp = client.post("/api/targets/republish")
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True


def test_restart_of_app_republishes_current_directory(tmp_path, monkeypatch):
    db = tmp_path / "dir.sqlite3"
    targets = tmp_path / "metrics-targets.json"
    monkeypatch.setenv("SVCDIR_DB", str(db))
    monkeypatch.setenv("SVCDIR_TARGETS_FILE", str(targets))
    app = create_app()
    client = app.test_client()
    client.post("/api/services", json={"service": "metrics-a", "address": "10.0.0.1:8000"})
    targets.unlink()
    (tmp_path / "metrics-targets.json.version").unlink()
    # 重启后按当前目录补发。
    create_app()
    assert target_map(targets) == {"metrics-a": "10.0.0.1:8000"}
