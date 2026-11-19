"""Prometheus file_sd target-file publisher.

After every committed directory change (add / re-address / delete / backup
restore) the whole target file is regenerated from one committed directory
version and atomically swapped in, so Prometheus can discover the ``metrics-*``
services through file_sd.

Design notes
------------
* Only services whose name starts with ``metrics-`` become targets; each
  target group carries a ``service`` label with the service name.
* The directory version a file was generated from is recorded in a sidecar
  ``<file>.version``.  The sidecar is always replaced *after* the targets
  file, so the recorded version can never be newer than the file content —
  a crash can only make it look older, which merely tightens the reuse rule.
* A late publish (its directory version is older than the version the file
  already reflects) is dropped and never overwrites the newer file.
* Addresses that are not ``host:port`` never enter the file and never fail
  the publish: the offending service is skipped, or — when it existed in
  every directory version since the last successful publish — its previous
  file address is reused.  A service deleted and re-created since the last
  publish (even if no file was published in between) is *not* allowed to
  reuse the old address, because the file address belongs to the pre-delete
  incarnation.
* Publish failures keep the previous file untouched, are remembered for the
  status page, and are retried on the next change, on manual republish, or
  on process start (``publish_current`` runs at boot).
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .store import DirectoryStore

# host:port — either a DNS name / IPv4, or a bracketed IPv6 literal.  This is
# deliberately a bit more permissive than strict DNS (underscores, tildes):
# the goal is "Prometheus can scrape it", not "is a registered hostname".
_HOST_PORT_RE = re.compile(
    r"(?:\[(?P<ipv6>[0-9A-Fa-f:.]+)\]|(?P<host>[A-Za-z0-9._~-]{1,253})):(?P<port>[0-9]{1,5})\Z"
)

OUTCOME_PUBLISHED = "published"
OUTCOME_UNCHANGED = "unchanged"
OUTCOME_STALE = "stale"
OUTCOME_FAILED = "failed"

STATUS_PUBLISHED = "published"  # 已发布：文件地址与目录地址一致
STATUS_REUSED = "reused"        # 沿用：目录地址无效，沿用文件中的旧地址
STATUS_SKIPPED = "skipped"      # 跳过：目录地址无效且不可沿用，未写入文件
STATUS_PENDING = "pending"      # 待重发：文件版本落后于当前目录版本


def is_host_port(address: object) -> bool:
    """Whether ``address`` is a usable ``host:port`` scrape target."""
    if not isinstance(address, str) or not address:
        return False
    match = _HOST_PORT_RE.fullmatch(address)
    if match is None:
        return False
    return 1 <= int(match.group("port")) <= 65535


@dataclass(frozen=True)
class FileState:
    """What the target file currently claims, as read from disk."""

    version: int | None          # directory version the file was built from
    addresses: dict[str, str]    # service label -> target address in the file
    raw: str | None              # raw file text (None when missing/unreadable)


@dataclass(frozen=True)
class PublishResult:
    ok: bool
    version: int
    outcome: str                 # published / unchanged / stale / failed
    error: str | None = None
    counts: dict[str, int] = field(default_factory=dict)


class TargetFilePublisher:
    """Regenerates and atomically replaces the Prometheus file_sd target file."""

    def __init__(
        self,
        store: "DirectoryStore",
        path: str | Path,
        *,
        prefix: str = "metrics-",
        label: str = "service",
    ) -> None:
        self._store = store
        self.path = Path(path)
        self.prefix = prefix
        self.label = label
        self._lock = threading.Lock()
        self._last_error: str | None = None

    # -- paths --------------------------------------------------------------

    @property
    def version_path(self) -> Path:
        return self.path.with_name(self.path.name + ".version")

    @property
    def lock_path(self) -> Path:
        return self.path.with_name(self.path.name + ".lock")

    # -- reading the current file state --------------------------------------

    def read_state(self) -> FileState:
        """Parse the current target file; anything fishy degrades safely.

        A missing or unparseable targets file means "no known version and no
        reusable addresses": any publish may proceed, but no old address may
        be reused.  A missing/corrupt version sidecar likewise forbids reuse
        (the continuity of the file content cannot be established).
        """
        try:
            raw = self.path.read_text(encoding="utf-8")
        except OSError:
            return FileState(version=None, addresses={}, raw=None)
        try:
            addresses = self._extract_addresses(json.loads(raw))
        except (ValueError, TypeError, AttributeError):
            return FileState(version=None, addresses={}, raw=raw)
        return FileState(version=self._read_version(), addresses=addresses, raw=raw)

    def _extract_addresses(self, data: object) -> dict[str, str]:
        if not isinstance(data, list):
            raise ValueError("目标文件必须是 JSON 数组")
        addresses: dict[str, str] = {}
        for group in data:
            if not isinstance(group, dict):
                raise ValueError("目标分组必须是对象")
            labels = group.get("labels") or {}
            targets = group.get("targets") or []
            if not isinstance(labels, dict) or not isinstance(targets, list):
                raise ValueError("目标分组格式无效")
            service = labels.get(self.label)
            if isinstance(service, str) and targets and isinstance(targets[0], str):
                addresses[service] = targets[0]
        return addresses

    def _read_version(self) -> int | None:
        try:
            version = int(self.version_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return None
        return version if version >= 0 else None

    # -- publishing -----------------------------------------------------------

    def publish_current(self) -> PublishResult:
        """Publish whatever the current committed directory version is."""
        return self.publish(self._store.current_version_id())

    def publish(self, version: int) -> PublishResult:
        """Regenerate the file from directory ``version``; never raises."""
        try:
            version = int(version)
        except (TypeError, ValueError):
            version = -1
        try:
            with contextlib.suppress(OSError):
                self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._lock, self._flock():
                result = self._publish_locked(version)
        except Exception as exc:  # a failed publish must never break the directory
            result = PublishResult(
                ok=False,
                version=version,
                outcome=OUTCOME_FAILED,
                error=f"{type(exc).__name__}: {exc}",
            )
        if result.outcome == OUTCOME_FAILED:
            self._last_error = result.error
        elif result.outcome in (OUTCOME_PUBLISHED, OUTCOME_UNCHANGED):
            self._last_error = None
        return result

    def _publish_locked(self, version: int) -> PublishResult:
        state = self.read_state()
        if state.version is not None and version < state.version:
            # 迟到的发布不能覆盖新版。
            return PublishResult(ok=True, version=version, outcome=OUTCOME_STALE)
        text, decisions = self._render(version, state)
        counts = {
            STATUS_PUBLISHED: sum(1 for d in decisions if d["status"] == STATUS_PUBLISHED),
            STATUS_REUSED: sum(1 for d in decisions if d["status"] == STATUS_REUSED),
            STATUS_SKIPPED: sum(1 for d in decisions if d["status"] == STATUS_SKIPPED),
        }
        if state.version == version and state.raw == text:
            return PublishResult(ok=True, version=version, outcome=OUTCOME_UNCHANGED, counts=counts)
        self._write_atomic(text, version)
        return PublishResult(ok=True, version=version, outcome=OUTCOME_PUBLISHED, counts=counts)

    def _render(self, version: int, state: FileState) -> tuple[str, list[dict]]:
        groups: list[dict] = []
        decisions: list[dict] = []
        for service, address in self._store.version_entries(version):
            if not service.startswith(self.prefix):
                continue
            chosen, status = self._choose(service, address, version, state)
            decisions.append(
                {
                    "service": service,
                    "directory_address": address,
                    "file_address": chosen,
                    "status": status,
                }
            )
            if chosen is not None:
                groups.append({"targets": [chosen], "labels": {self.label: service}})
        text = json.dumps(groups, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        return text, decisions

    def _choose(
        self, service: str, address: str, version: int, state: FileState
    ) -> tuple[str | None, str]:
        if is_host_port(address):
            return address, STATUS_PUBLISHED
        old = state.addresses.get(service)
        if (
            old is not None
            and state.version is not None
            and is_host_port(old)
            # 只有从上次发布起每版目录都存在，才可沿用文件里的旧地址；
            # 删除后同名重建（中间可能没发布过文件）不满足该条件。
            and self._store.service_present_in_versions(service, state.version, version)
        ):
            return old, STATUS_REUSED
        return None, STATUS_SKIPPED

    # -- atomic replacement ---------------------------------------------------

    def _write_atomic(self, text: str, version: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._replace_file(self.path, text)
        # The version marker is replaced last: it must never claim a version
        # newer than the targets file actually contains.
        self._replace_file(self.version_path, f"{version}\n")

    @staticmethod
    def _replace_file(path: Path, text: str) -> None:
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(tmp)

    @contextlib.contextmanager
    def _flock(self):
        """Best-effort cross-process serialization (mirrors the store's lock)."""
        fd: int | None = None
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError:
            if fd is not None:
                with contextlib.suppress(OSError):
                    os.close(fd)
            fd = None
        try:
            yield
        finally:
            if fd is not None:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
                with contextlib.suppress(OSError):
                    os.close(fd)

    # -- status for the homepage / API ----------------------------------------

    @property
    def last_error(self) -> str | None:
        return self._last_error

    def status(self) -> dict:
        """Side-by-side view of directory addresses vs. file addresses."""
        state = self.read_state()
        current = self._store.current_version_id()
        pending = state.version != current
        rows: list[dict] = []
        seen: set[str] = set()
        for service, address in self._store.version_entries(current):
            if not service.startswith(self.prefix):
                continue
            seen.add(service)
            file_address = state.addresses.get(service)
            rows.append(
                {
                    "service": service,
                    "directory_address": address,
                    "file_address": file_address,
                    "status": self._row_status(pending, address, file_address),
                }
            )
        # Targets still present in the file but gone from the directory
        # (e.g. the delete's publish failed): they come down on the next
        # successful publish.
        for service, address in sorted(state.addresses.items()):
            if service not in seen:
                rows.append(
                    {
                        "service": service,
                        "directory_address": None,
                        "file_address": address,
                        "status": STATUS_PENDING,
                    }
                )
        return {
            "path": str(self.path),
            "prefix": self.prefix,
            "label": self.label,
            "file_version": state.version,
            "current_version": current,
            "pending": pending,
            "last_error": self._last_error,
            "rows": rows,
        }

    @staticmethod
    def _row_status(pending: bool, address: str, file_address: str | None) -> str:
        if pending:
            return STATUS_PENDING
        if is_host_port(address):
            return STATUS_PUBLISHED if file_address == address else STATUS_PENDING
        return STATUS_REUSED if file_address is not None else STATUS_SKIPPED
