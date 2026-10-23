"""Crash-safe, filesystem-backed Silver release publication primitives."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any, Iterator


_HEX = re.compile(r"^[0-9a-f]{64}$")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def build_fingerprint(payload: dict) -> str:
    """Return a deterministic SHA-256 over a JSON-compatible build description."""
    if not isinstance(payload, dict):
        raise TypeError("fingerprint payload must be a dict")
    return hashlib.sha256(_json_bytes(payload)).hexdigest()


def _managed_root(root: Path) -> Path:
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


@contextlib.contextmanager
def exclusive_lock(root: Path) -> Iterator[None]:
    """Hold an exclusive process lock for this resolved managed root."""
    managed = _managed_root(root)
    lock_path = managed / ".silver.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_json_atomic(path: Path, value: dict) -> None:
    data = _json_bytes(value) + b"\n"
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        _fsync_dir(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temp.unlink()


def stage_release(root: Path, fingerprint: str, *, category: str | None = None) -> Path:
    """Create a unique staging directory on the managed release filesystem."""
    if not isinstance(fingerprint, str) or not _HEX.fullmatch(fingerprint):
        raise ValueError("fingerprint must be a lowercase SHA-256 hex digest")
    managed = _managed_root(root)
    staging_root = managed / ".staging"
    if category is not None:
        if category != "unvalidated":
            raise ValueError("unsupported staging category")
        staging_root = staging_root / category
    staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    stage = staging_root / f"{fingerprint}-{uuid.uuid4().hex}"
    stage.mkdir(mode=0o700)
    _fsync_dir(staging_root)
    return stage


def _safe_member(base: Path, relative: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("output paths must be nonempty relative paths")
    candidate = base / relative
    # Reject symlink traversal even if it resolves back inside the root.
    current = base
    for part in Path(relative).parts:
        if part in (".", ".."):
            raise ValueError("output path traversal is not allowed")
        current = current / part
        if current.is_symlink():
            raise ValueError("output path may not traverse symlinks")
    resolved = candidate.resolve(strict=True)
    if not resolved.is_relative_to(base.resolve()):
        raise ValueError("output path escapes its managed directory")
    return resolved


def _output_entries(descriptor: dict) -> list[dict]:
    outputs = descriptor.get("outputs")
    if isinstance(outputs, dict):
        entries = []
        for name, info in outputs.items():
            if not isinstance(info, dict):
                raise ValueError("each output requires integrity and schema metadata")
            entries.append({"name": name, **info})
        return entries
    if isinstance(outputs, list):
        return outputs
    raise ValueError("descriptor outputs must be a nonempty list or mapping")


def _validate_descriptor(descriptor: dict, staging: Path, *, check_fingerprint: bool = True) -> list[dict]:
    if not isinstance(descriptor, dict):
        raise ValueError("descriptor must be an object")
    if descriptor.get("state") == "unvalidated" or descriptor.get("validation_status") == "skipped":
        raise ValueError("unvalidated artifacts are not publishable releases")
    if descriptor.get("validation_status", "passed") != "passed":
        raise ValueError("release validation status is not passed")
    required = ("fingerprint", "schema_version", "sources", "gate_results")
    if any(key not in descriptor for key in required):
        raise ValueError("descriptor is missing required release metadata")
    if not isinstance(descriptor["fingerprint"], str) or not _HEX.fullmatch(descriptor["fingerprint"]):
        raise ValueError("invalid descriptor fingerprint")
    if check_fingerprint and staging.name.split("-", 1)[0] != descriptor["fingerprint"]:
        raise ValueError("descriptor fingerprint does not match staging release")
    if not isinstance(descriptor["schema_version"], (str, int)) or not descriptor["schema_version"]:
        raise ValueError("schema_version is required")
    if not isinstance(descriptor["sources"], (dict, list)):
        raise ValueError("sources metadata is required")
    gates = descriptor["gate_results"]
    if not isinstance(gates, (dict, list)) or not gates:
        raise ValueError("gate_results must contain passing assertions")
    gate_values = gates.values() if isinstance(gates, dict) else gates
    for result in gate_values:
        value = result.get("ok") if isinstance(result, dict) else result
        if value is not True:
            raise ValueError("release gates must all be explicitly true")
    entries = _output_entries(descriptor)
    if not entries:
        raise ValueError("descriptor must list at least one output")
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("invalid output metadata")
        relative = entry.get("path")
        if relative in seen:
            raise ValueError("duplicate output path")
        seen.add(relative)
        file_path = _safe_member(staging, relative)
        if not file_path.is_file():
            raise ValueError("listed output is not a regular file")
        expected = entry.get("sha256")
        if not isinstance(expected, str) or not _HEX.fullmatch(expected):
            raise ValueError("output requires a SHA-256 digest")
        digest_builder = hashlib.sha256()
        with file_path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest_builder.update(chunk)
        digest = digest_builder.hexdigest()
        if digest != expected:
            raise ValueError(f"output checksum mismatch: {relative}")
        if not isinstance(entry.get("schema"), (dict, list)) or not entry["schema"]:
            raise ValueError("output requires schema metadata")
    return entries


def publish_release(root: Path, staging: Path, descriptor: dict) -> dict:
    """Commit a fully validated immutable directory, then atomically switch current."""
    managed = _managed_root(root)
    staging = Path(staging).absolute()
    if staging.is_symlink() or not staging.is_dir() or not staging.resolve().is_relative_to(managed):
        raise ValueError("staging directory must be inside the managed root")
    entries = _validate_descriptor(descriptor, staging)
    # Persist every staged file before making the directory reachable by readers.
    for entry in entries:
        with _safe_member(staging, entry["path"]).open("rb") as stream:
            os.fsync(stream.fileno())
    _write_json_atomic(staging / "release.json", descriptor)
    for directory, _, _ in os.walk(staging, topdown=False):
        _fsync_dir(Path(directory))
    releases = managed / "releases"
    releases.mkdir(mode=0o700, exist_ok=True)
    release_id = f"{descriptor['fingerprint'][:16]}-{uuid.uuid4().hex[:12]}"
    final_dir = releases / release_id
    os.replace(staging, final_dir)
    _fsync_dir(releases)
    committed = dict(descriptor)
    committed["release_id"] = release_id
    # Release metadata includes its assigned immutable identifier.
    _write_json_atomic(final_dir / "release.json", committed)
    pointer = {"release_id": release_id, "descriptor": f"releases/{release_id}/release.json"}
    _write_json_atomic(managed / "current.json", pointer)
    committed["state"] = "published"
    return committed


def _read_descriptor(managed: Path, release_id: str | None) -> dict:
    if release_id is None:
        pointer_path = managed / "current.json"
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        release_id = pointer.get("release_id")
        if not isinstance(release_id, str) or not re.fullmatch(r"[0-9a-f]{16}-[0-9a-f]{12}", release_id):
            raise ValueError("invalid current release pointer")
        expected = f"releases/{release_id}/release.json"
        if pointer.get("descriptor") != expected:
            raise ValueError("current pointer descriptor path is invalid")
    elif not isinstance(release_id, str) or not re.fullmatch(r"[0-9a-f]{16}-[0-9a-f]{12}", release_id):
        raise ValueError("invalid release_id")
    release_dir = managed / "releases" / release_id
    path = release_dir / "release.json"
    if (managed / "releases").is_symlink() or release_dir.is_symlink() or path.is_symlink() or not path.is_file():
        raise ValueError("release descriptor is missing or unsafe")
    if not path.resolve(strict=True).is_relative_to(managed):
        raise ValueError("release descriptor escapes managed root")
    descriptor = json.loads(path.read_text(encoding="utf-8"))
    if descriptor.get("release_id") != release_id:
        raise ValueError("descriptor release_id mismatch")
    return descriptor


def load_release(root: Path, release_id: str | None = None, *, verify: bool = True) -> dict:
    """Resolve one release descriptor and optionally verify every listed artifact."""
    managed = Path(root).resolve()
    if not managed.is_dir():
        raise FileNotFoundError(managed)
    descriptor = _read_descriptor(managed, release_id)
    if descriptor.get("state") == "unvalidated" or descriptor.get("validation_status") == "skipped":
        raise ValueError("unvalidated artifacts are not managed releases")
    if descriptor.get("validation_status", "passed") != "passed":
        raise ValueError("release validation status is not passed")
    if verify:
        base = managed / "releases" / descriptor["release_id"]
        entries = _validate_descriptor(descriptor, base, check_fingerprint=False)
        # release.json paths and digests are relative to this immutable release.
        for entry in entries:
            _safe_member(base, entry["path"])
    return descriptor


def rollback_release(root: Path, release_id: str, *, reason: str) -> dict:
    """Atomically repoint to a verified retained release without modifying its files."""
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("rollback reason is required")
    with exclusive_lock(root):
        descriptor = load_release(root, release_id, verify=True)
        descriptor["rollback"] = {"reason": reason}
        _write_json_atomic(Path(root).resolve() / "current.json", {
            "release_id": release_id,
            "descriptor": f"releases/{release_id}/release.json",
            "rollback_reason": reason,
        })
        return descriptor
