#!/usr/bin/env python3
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

import requests
from huggingface_hub import HfApi, hf_hub_download, hf_hub_url
from huggingface_hub.file_download import get_hf_file_metadata


REQUIRED_ENV_VARS = [
    "HF_ENDPOINT",
    "HF_TOKEN",
    "REPRO_MODE",
    "REPRO_TARGET_REPO",
    "REPRO_TARGET_FILE",
]


@dataclass
class ProbeResult:
    phase: str
    repo_id: str
    target: str
    ok: bool
    duration: float
    pre_list_repos: List[str] = field(default_factory=list)
    commit_hash: Optional[str] = None
    etag: Optional[str] = None
    size: Optional[int] = None
    error_type: Optional[str] = None
    error_text: Optional[str] = None
    traceback_text: Optional[str] = None


def require_env() -> Dict[str, str]:
    missing = [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
            + ". Export them before running this script."
        )
    return {name: os.environ[name] for name in REQUIRED_ENV_VARS}


def parse_pre_list_repos() -> List[str]:
    raw = os.environ.get("REPRO_PRE_LIST_REPOS", "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def list_preconditions(repos: List[str], endpoint: str, token: str) -> None:
    if not repos:
        return

    api = HfApi(endpoint=endpoint, token=token)
    for repo_id in repos:
        entries = list(api.list_repo_tree(repo_id=repo_id, repo_type="dataset", recursive=False, expand=True))
        print(
            json.dumps(
                {
                    "entry_count": len(entries),
                    "phase": "pre_list",
                    "repo_id": repo_id,
                },
                sort_keys=True,
            ),
            flush=True,
        )


def snapshot_headers(target: str, repo_id: str, endpoint: str, token: str, timeout: float) -> Dict[str, object]:
    url = hf_hub_url(repo_id=repo_id, filename=target, repo_type="dataset", endpoint=endpoint)
    auth_headers = {"Authorization": f"Bearer {token}"}
    snapshot: Dict[str, object] = {"resolve_url": url}

    with requests.Session() as session:
        first = session.head(url, headers=auth_headers, allow_redirects=False, timeout=timeout)
        snapshot["resolve_status"] = first.status_code
        snapshot["resolve_headers"] = dict(first.headers)

        location = first.headers.get("Location")
        if location:
            second = session.head(location, allow_redirects=False, timeout=timeout)
            snapshot["storage_status"] = second.status_code
            snapshot["storage_headers"] = dict(second.headers)

    return snapshot


def is_metadata_bug(result: ProbeResult) -> bool:
    if result.phase == "metadata":
        return not result.ok

    if result.ok:
        return False

    text = "\n".join(filter(None, [result.error_type, result.error_text, result.traceback_text]))
    return "FileMetadataError" in text or (
        "LocalEntryNotFoundError" in text and "_get_metadata_or_catch_error" in text
    )


def run_metadata(repo_id: str, endpoint: str, token: str, target: str, timeout: float, pre_list_repos: List[str]) -> ProbeResult:
    url = hf_hub_url(repo_id=repo_id, filename=target, repo_type="dataset", endpoint=endpoint)
    start = time.time()
    try:
        meta = get_hf_file_metadata(url=url, token=token, timeout=timeout, endpoint=endpoint)
        commit_hash = getattr(meta, "commit_hash", None)
        etag = getattr(meta, "etag", None)
        size = getattr(meta, "size", None)
        ok = bool(commit_hash) and bool(etag) and isinstance(size, int) and size > 0
        return ProbeResult(
            phase="metadata",
            repo_id=repo_id,
            target=target,
            ok=ok,
            duration=round(time.time() - start, 3),
            pre_list_repos=pre_list_repos,
            commit_hash=commit_hash,
            etag=etag,
            size=size,
            error_type=None if ok else "BadMetadata",
            error_text=None if ok else repr(meta),
        )
    except Exception as err:
        return ProbeResult(
            phase="metadata",
            repo_id=repo_id,
            target=target,
            ok=False,
            duration=round(time.time() - start, 3),
            pre_list_repos=pre_list_repos,
            error_type=type(err).__name__,
            error_text=repr(err),
            traceback_text=traceback.format_exc(),
        )


def run_download(repo_id: str, endpoint: str, token: str, target: str, timeout: float, pre_list_repos: List[str]) -> ProbeResult:
    start = time.time()
    temp_root = tempfile.mkdtemp(prefix="deepghs_repro_")
    try:
        hf_hub_download(
            repo_id=repo_id,
            filename=target,
            repo_type="dataset",
            endpoint=endpoint,
            token=token,
            cache_dir=os.path.join(temp_root, "cache"),
            local_dir=os.path.join(temp_root, "out"),
            force_download=True,
            etag_timeout=timeout,
        )
        return ProbeResult(
            phase="download",
            repo_id=repo_id,
            target=target,
            ok=True,
            duration=round(time.time() - start, 3),
            pre_list_repos=pre_list_repos,
        )
    except Exception as err:
        return ProbeResult(
            phase="download",
            repo_id=repo_id,
            target=target,
            ok=False,
            duration=round(time.time() - start, 3),
            pre_list_repos=pre_list_repos,
            error_type=type(err).__name__,
            error_text=repr(err),
            traceback_text=traceback.format_exc(),
        )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def report_failure(result: ProbeResult, endpoint: str, token: str, timeout: float) -> int:
    print("=== REPRODUCED ===", flush=True)
    print(json.dumps(asdict(result), indent=2, sort_keys=True), flush=True)
    try:
        snapshot = snapshot_headers(
            target=result.target,
            repo_id=result.repo_id,
            endpoint=endpoint,
            token=token,
            timeout=timeout,
        )
    except Exception as err:
        snapshot = {
            "snapshot_error_type": type(err).__name__,
            "snapshot_error_text": repr(err),
            "snapshot_traceback": traceback.format_exc(),
        }
    print("=== HEADER SNAPSHOT ===", flush=True)
    print(json.dumps(snapshot, indent=2, sort_keys=True), flush=True)
    return 1


def main() -> int:
    env = require_env()
    endpoint = env["HF_ENDPOINT"]
    token = env["HF_TOKEN"]
    repo_id = env["REPRO_TARGET_REPO"]
    target = env["REPRO_TARGET_FILE"]
    mode = env["REPRO_MODE"]
    timeout = float(os.environ.get("REPRO_TIMEOUT", "10"))
    pre_list_repos = parse_pre_list_repos()

    print(
        json.dumps(
            {
                "mode": mode,
                "pre_list_repos": pre_list_repos,
                "repo_id": repo_id,
                "target": target,
                "timeout": timeout,
            },
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )

    list_preconditions(pre_list_repos, endpoint=endpoint, token=token)

    if mode == "metadata":
        result = run_metadata(
            repo_id=repo_id,
            endpoint=endpoint,
            token=token,
            target=target,
            timeout=timeout,
            pre_list_repos=pre_list_repos,
        )
    elif mode == "download":
        result = run_download(
            repo_id=repo_id,
            endpoint=endpoint,
            token=token,
            target=target,
            timeout=timeout,
            pre_list_repos=pre_list_repos,
        )
    else:
        raise RuntimeError(f"Unsupported REPRO_MODE: {mode!r}")

    print(json.dumps(asdict(result), indent=2, sort_keys=True), flush=True)
    if is_metadata_bug(result):
        return report_failure(result, endpoint=endpoint, token=token, timeout=timeout)
    if not result.ok:
        return 2

    print("No metadata bug reproduced for this one-shot scenario.", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
