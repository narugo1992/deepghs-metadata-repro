# deepghs-metadata-repro

> [!IMPORTANT]
> **This repo is deprecated — the bug it reproduces is fixed.**
>
> - Tracking issue: [deepghs/KohakuHub#24](https://github.com/deepghs/KohakuHub/issues/24) (closed)
> - Fix: [deepghs/KohakuHub#25](https://github.com/deepghs/KohakuHub/pull/25) (merged 2026-04-24)
> - Verified in production against `hub.deepghs.org` on a GitHub-hosted Ubuntu runner: [run 24837550364 / job 72701957896](https://github.com/narugo1992/deepghs-metadata-repro/actions/runs/24837550364/job/72701957896) — green, no reproduction detected.
>
> The script's contract is that *any* bad-metadata result or metadata-related `hf_hub_download` exception exits non-zero. A green run therefore means the failure mode is absent, not that the bug is still present. The pre-fix runs (`24607211818` / `24607232149` / `24607247445`) all exited non-zero on the same script against the same endpoint, with the exact production exception.
>
> Contents below are preserved for historical reference only.

---

Minimal public repro for the intermittent metadata error seen against `https://hub.deepghs.org`.

## Local run

The script does not read `.env` directly. Export environment variables first, then run:

```bash
source .env
export REPRO_MODE=download
export REPRO_TARGET_REPO=deepghs/reddit_mostlyhumans_index
export REPRO_TARGET_FILE=mostlyhumans.csv
python deepghs_metadata_repro.py
```

Required environment variables:

- `HF_ENDPOINT`
- `HF_TOKEN`
- `REPRO_MODE`
- `REPRO_TARGET_REPO`
- `REPRO_TARGET_FILE`

## GitHub Actions

The workflow is manual-only (`workflow_dispatch`) and is already pinned to the stable one-shot scenario:

- `REPRO_MODE=download`
- `REPRO_TARGET_REPO=deepghs/reddit_mostlyhumans_index`
- `REPRO_TARGET_FILE=mostlyhumans.csv`

Repository secret required:

- `HF_TOKEN`

Workflow environment defaults:

- `HF_ENDPOINT=https://hub.deepghs.org`

## What the script does

The script performs exactly one deterministic scenario per run:

1. Optionally pre-lists one or more fixed `deepghs/*` repos.
2. Runs exactly one metadata or download probe against one fixed `deepghs/*` file.
3. Treats bad metadata or metadata-related download exceptions as a reproduction.

## Verified GitHub Actions runs

The following runs reproduced the same failure on GitHub-hosted Ubuntu runners, **before** the fix in [deepghs/KohakuHub#25](https://github.com/deepghs/KohakuHub/pull/25):

- `24607211818`
- `24607232149`
- `24607247445`

Any bad metadata result or metadata-related `hf_hub_download` exception is treated as a reproduction and exits with a non-zero code.
