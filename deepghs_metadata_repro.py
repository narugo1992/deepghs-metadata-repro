#!/usr/bin/env python3
import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

import requests
from huggingface_hub import HfApi, hf_hub_download, hf_hub_url
from huggingface_hub.file_download import get_hf_file_metadata


REQUIRED_ENV_VARS = ["HF_ENDPOINT", "HF_TOKEN", "REMOTE_REPOSITORY_ORD"]

ALL_TARGETS = [
    "pack_20240308_124840_855759.zip",
    "pack_20251107_155840_036210.zip",
    "pack_20240111_125008_765631.zip",
    "pack_20240309_124615_702977.zip",
    "pack_20260228_131258_580023.zip",
    "pack_20240110_125029_279170.zip",
    "pack_20240307_124918_080137.zip",
    "pack_20250210_151204_477343.zip",
    "pack_20260415_141207_903455.zip",
    "pack_20260410_135705_547798.zip",
    "pack_20240415_124718_725293.zip",
    "pack_20260228_103516_421486.zip",
    "pack_20260414_141959_813033.zip",
    "pack_20240620_125556_882312.zip",
    "pack_20250603_131206_087051.zip",
    "pack_20260416_142243_770315.zip",
    "pack_20260417_140724_463504.zip",
    "pack_20260413_141703_660406.zip",
    "pack_20260412_134310_287288.zip",
    "pack_20260411_133848_451279.zip",
    "pack_20260409_142337_208171.zip",
    "pack_20260408_141301_840155.zip",
    "pack_20260405_133841_538589.zip",
    "pack_20260404_133626_914558.zip",
    "pack_20260403_134530_235963.zip",
    "pack_20260402_140339_111229.zip",
    "pack_20260401_141358_577288.zip",
    "pack_20260331_140958_561508.zip",
]

METADATA_BATCH = ALL_TARGETS[24:] + ALL_TARGETS[:20]
DOWNLOAD_TARGETS = [
    "pack_20240308_124840_855759.zip",
    "pack_20260228_131258_580023.zip",
    "pack_20260417_140724_463504.zip",
]

PRELIST_METADATA_TRIALS = int(os.environ.get("REPRO_PRELIST_METADATA_TRIALS", "3"))
DIRECT_METADATA_TRIALS = int(os.environ.get("REPRO_DIRECT_METADATA_TRIALS", "3"))
METADATA_TIMEOUT = float(os.environ.get("REPRO_METADATA_TIMEOUT", "10"))
DOWNLOAD_ETAG_TIMEOUT = float(os.environ.get("REPRO_DOWNLOAD_ETAG_TIMEOUT", "10"))


@dataclass
class ProbeResult:
    phase: str
    target: str
    ok: bool
    duration: float
    trial_id: str
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


def probe_metadata(repo_id: str, endpoint: str, token: str, target: str, timeout: float) -> ProbeResult:
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
            target=target,
            ok=ok,
            duration=round(time.time() - start, 3),
            trial_id=os.environ.get("REPRO_TRIAL_ID", "unknown"),
            commit_hash=commit_hash,
            etag=etag,
            size=size,
            error_type=None if ok else "BadMetadata",
            error_text=None if ok else repr(meta),
        )
    except Exception as err:
        return ProbeResult(
            phase="metadata",
            target=target,
            ok=False,
            duration=round(time.time() - start, 3),
            trial_id=os.environ.get("REPRO_TRIAL_ID", "unknown"),
            error_type=type(err).__name__,
            error_text=repr(err),
            traceback_text=traceback.format_exc(),
        )


def probe_download(repo_id: str, endpoint: str, token: str, target: str) -> ProbeResult:
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
            etag_timeout=DOWNLOAD_ETAG_TIMEOUT,
        )
        return ProbeResult(
            phase="download",
            target=target,
            ok=True,
            duration=round(time.time() - start, 3),
            trial_id=os.environ.get("REPRO_TRIAL_ID", "unknown"),
        )
    except Exception as err:
        return ProbeResult(
            phase="download",
            target=target,
            ok=False,
            duration=round(time.time() - start, 3),
            trial_id=os.environ.get("REPRO_TRIAL_ID", "unknown"),
            error_type=type(err).__name__,
            error_text=repr(err),
            traceback_text=traceback.format_exc(),
        )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def print_result(result: ProbeResult) -> None:
    print(json.dumps(asdict(result), indent=2, sort_keys=True), flush=True)


def report_failure(result: ProbeResult, repo_id: str, endpoint: str, token: str) -> int:
    print("=== REPRODUCED ===", flush=True)
    print_result(result)
    try:
        snapshot = snapshot_headers(
            target=result.target,
            repo_id=repo_id,
            endpoint=endpoint,
            token=token,
            timeout=METADATA_TIMEOUT,
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


def child_metadata(pre_list: bool) -> int:
    env = require_env()
    repo_id = env["REMOTE_REPOSITORY_ORD"]
    endpoint = env["HF_ENDPOINT"]
    token = env["HF_TOKEN"]

    if pre_list:
        api = HfApi(endpoint=endpoint, token=token)
        entries = list(api.list_repo_tree(repo_id=repo_id, repo_type="dataset", recursive=False, expand=True))
        print(
            json.dumps(
                {
                    "phase": "pre_list",
                    "trial_id": os.environ.get("REPRO_TRIAL_ID", "unknown"),
                    "entry_count": len(entries),
                },
                sort_keys=True,
            ),
            flush=True,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(METADATA_BATCH)) as executor:
        futures = [executor.submit(probe_metadata, repo_id, endpoint, token, target, METADATA_TIMEOUT) for target in METADATA_BATCH]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    bad_results = [item for item in results if is_metadata_bug(item)]
    for item in sorted(results, key=lambda current: current.target):
        print_result(item)
    if bad_results:
        return report_failure(bad_results[0], repo_id=repo_id, endpoint=endpoint, token=token)

    print(
        json.dumps(
            {
                "phase": "metadata",
                "mode": "pre_list" if pre_list else "direct",
                "trial_id": os.environ.get("REPRO_TRIAL_ID", "unknown"),
                "status": "ok",
                "request_count": len(METADATA_BATCH),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


def child_download(target: str) -> int:
    env = require_env()
    repo_id = env["REMOTE_REPOSITORY_ORD"]
    endpoint = env["HF_ENDPOINT"]
    token = env["HF_TOKEN"]

    result = probe_download(repo_id=repo_id, endpoint=endpoint, token=token, target=target)
    print_result(result)
    if is_metadata_bug(result):
        return report_failure(result, repo_id=repo_id, endpoint=endpoint, token=token)
    return 0


def run_child(mode: str, trial_id: str, extra_args: List[str]) -> int:
    cmd = [sys.executable, os.path.abspath(__file__), "--child", mode, *extra_args]
    env = os.environ.copy()
    env["REPRO_TRIAL_ID"] = trial_id
    completed = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if completed.stdout:
        sys.stdout.write(completed.stdout)
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    return completed.returncode


def parent_main() -> int:
    require_env()

    print(
        json.dumps(
            {
                "prelist_metadata_trials": PRELIST_METADATA_TRIALS,
                "direct_metadata_trials": DIRECT_METADATA_TRIALS,
                "metadata_batch_size": len(METADATA_BATCH),
                "download_targets": DOWNLOAD_TARGETS,
            },
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )

    for trial_index in range(1, PRELIST_METADATA_TRIALS + 1):
        trial_id = f"prelist-{trial_index}"
        print(f"=== START {trial_id} ===", flush=True)
        code = run_child("metadata-pre-list", trial_id, [])
        if code != 0:
            return code

    for trial_index in range(1, DIRECT_METADATA_TRIALS + 1):
        trial_id = f"direct-{trial_index}"
        print(f"=== START {trial_id} ===", flush=True)
        code = run_child("metadata-direct", trial_id, [])
        if code != 0:
            return code

    for index, target in enumerate(DOWNLOAD_TARGETS, start=1):
        trial_id = f"download-{index}"
        print(f"=== START {trial_id} {target} ===", flush=True)
        code = run_child("download", trial_id, [target])
        if code != 0:
            return code

    print("No metadata bug reproduced in the fixed matrix.", flush=True)
    return 0


def child_main(arguments: List[str]) -> int:
    if not arguments:
        raise SystemExit("Missing child mode.")

    mode = arguments[0]
    extra_args = arguments[1:]
    if mode == "metadata-pre-list":
        return child_metadata(pre_list=True)
    if mode == "metadata-direct":
        return child_metadata(pre_list=False)
    if mode == "download":
        if len(extra_args) != 1:
            raise SystemExit("download mode expects exactly one target.")
        return child_download(extra_args[0])
    raise SystemExit(f"Unknown child mode: {mode}")


def main(argv: List[str]) -> int:
    if len(argv) >= 2 and argv[1] == "--child":
        return child_main(argv[2:])
    return parent_main()


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv))
    except KeyboardInterrupt:
        raise SystemExit(130)
