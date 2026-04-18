# deepghs-metadata-repro

Minimal public repro for the intermittent metadata error seen against `https://hub.deepghs.org`.

## Local run

The script does not read `.env` directly. Export environment variables first, then run:

```bash
source .env
python deepghs_metadata_repro.py
```

Required environment variables:

- `HF_ENDPOINT`
- `HF_TOKEN`
- `REMOTE_REPOSITORY_ORD`

## GitHub Actions

The workflow is manual-only (`workflow_dispatch`).

Repository secret required:

- `HF_TOKEN`

Workflow environment defaults:

- `HF_ENDPOINT=https://hub.deepghs.org`
- `REMOTE_REPOSITORY_ORD=hk1901/ordered`

## What the script does

The script runs a small fixed matrix:

1. Fresh-process metadata batch after `list_repo_tree(..., expand=True)`.
2. Fresh-process metadata batch without the pre-list step.
3. Fresh-process single-file `hf_hub_download` probes for historically flaky targets.

Any bad metadata result or metadata-related `hf_hub_download` exception is treated as a reproduction and exits with a non-zero code.
