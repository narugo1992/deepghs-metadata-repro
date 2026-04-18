# deepghs-metadata-repro

Minimal public repro for the intermittent metadata error seen against `https://hub.deepghs.org`.

## Local run

The script does not read `.env` directly. Export environment variables first, then run:

```bash
source .env
export REPRO_MODE=metadata
export REPRO_TARGET_REPO=deepghs/nhentai_index
export REPRO_TARGET_FILE=images.parquet
export REPRO_PRE_LIST_REPOS=deepghs/nhentai_index
python deepghs_metadata_repro.py
```

Required environment variables:

- `HF_ENDPOINT`
- `HF_TOKEN`
- `REPRO_MODE`
- `REPRO_TARGET_REPO`
- `REPRO_TARGET_FILE`

Optional environment variables:

- `REPRO_PRE_LIST_REPOS`
- `REPRO_TIMEOUT`

## GitHub Actions

The workflow is manual-only (`workflow_dispatch`).

Repository secret required:

- `HF_TOKEN`

Workflow environment defaults:

- `HF_ENDPOINT=https://hub.deepghs.org`

## What the script does

The script performs exactly one deterministic scenario per run:

1. Optionally pre-lists one or more fixed `deepghs/*` repos.
2. Runs exactly one metadata or download probe against one fixed `deepghs/*` file.
3. Treats bad metadata or metadata-related download exceptions as a reproduction.

Any bad metadata result or metadata-related `hf_hub_download` exception is treated as a reproduction and exits with a non-zero code.
