# pw-groups: ACTIVATE → Slurm Group Sync

Syncs groups and their members from the ACTIVATE computing control plane to Slurm accounts and user associations. Designed to run on cron for continuous synchronization.

## What it does

1. Fetches groups and member lists from the ACTIVATE API
2. Reads the current Slurm accounting state via `sacctmgr`
3. Computes the diff (accounts/associations to add, remove, or update)
4. Applies changes in the correct dependency order

Each ACTIVATE group becomes a Slurm account. Group members become user associations under that account. Users in multiple groups get associations in all of them, with their `DefaultAccount` set to the first group alphabetically.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- `sacctmgr` available on `$PATH` (run on the Slurm head node or a host with sacctmgr access)
- ACTIVATE API key with org read permissions

## Setup

1. Clone or copy this directory to the Slurm head node (e.g., `/opt/pw-groups/`)

2. Copy the example environment file and fill in your values:
   ```bash
   cp env.example .env
   # Edit .env with your ACTIVATE_API_KEY, ACTIVATE_ORG_ID, ACTIVATE_ORG_NAME
   ```

3. Install dependencies:
   ```bash
   uv sync
   ```

The script automatically loads `.env` from its own directory — no need to `source` it.

## Testing workflow

Use the gradual rollout approach to validate behavior before syncing all groups.

### Step 1: Dry-run a single group

Set `SYNC_GROUPS` and `DRY_RUN` in your `.env`:
```
SYNC_GROUPS=TeamAlpha
DRY_RUN=true
```

Then run:
```bash
uv run sync_groups.py
```

Review the logged sync plan. No changes are made in dry-run mode.

### Step 2: Apply to that single group

Remove `DRY_RUN` (or set to `false`) in `.env`, keep `SYNC_GROUPS=TeamAlpha`:
```bash
uv run sync_groups.py
```

Verify the result:
```bash
sacctmgr show account withassoc where cluster=mycluster
```

### Step 3: Expand to a few groups

Update `.env`:
```
SYNC_GROUPS=TeamAlpha,TeamBeta,TeamGamma
```

```bash
uv run sync_groups.py
```

### Step 4: Test idempotency

Run the same command again immediately. It should log:
```
Nothing to do, Slurm is already in sync
```

### Step 5: Dry-run all groups

Update `.env`:
```
DRY_RUN=true
# SYNC_GROUPS=          (comment out or remove to sync all)
```

```bash
uv run sync_groups.py
```

### Step 6: Full sync

Update `.env`:
```
# DRY_RUN=true          (comment out or set to false)
# SYNC_GROUPS=          (comment out or remove to sync all)
```

```bash
uv run sync_groups.py
```

## Cron setup

Once validated, set up a cron job for continuous sync. Example crontab entry (syncs every 15 minutes):

```cron
*/15 * * * * cd /opt/pw-groups && /usr/local/bin/uv run sync_groups.py >> /var/log/pw-groups-sync.log 2>&1
```

Adjust the path to `uv` based on your installation. Find it with `which uv`.

To rotate logs, add a logrotate config at `/etc/logrotate.d/pw-groups`:
```
/var/log/pw-groups-sync.log {
    weekly
    rotate 4
    compress
    missingok
    notifempty
}
```

## Configuration reference

All configuration is via `.env` file or environment variables. See [env.example](env.example) for a documented template.

| Variable | Required | Default | Description |
|---|---|---|---|
| `ACTIVATE_API_KEY` | Yes | — | ACTIVATE API bearer token |
| `ACTIVATE_ORG_ID` | Yes | — | Organization ID |
| `ACTIVATE_ORG_NAME` | Yes | — | Organization slug |
| `ACTIVATE_API_URL` | No | `https://activate.parallel.works` | API base URL |
| `SLURM_CLUSTER` | No | `ACTIVATE_ORG_NAME` | Slurm ClusterName |
| `SLURM_ACCOUNT_ORG` | No | `ACTIVATE_ORG_NAME` | Organization field on Slurm accounts |
| `SYNC_GROUPS` | No | (all) | Comma-separated group names to sync |
| `DRY_RUN` | No | `false` | Log changes without executing |
| `LOG_LEVEL` | No | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

## How sync works

The script only manages Slurm accounts whose `Organization` field matches `SLURM_ACCOUNT_ORG`. It will never modify or delete accounts created outside this tool (e.g., `root`, manually created accounts).

**Execution order** (respects Slurm dependency constraints):
1. Create new accounts (must exist before users can be added)
2. Add user associations (must exist before setting as DefaultAccount)
3. Update DefaultAccount where needed
4. Remove stale user associations
5. Remove empty managed accounts

## Troubleshooting

**`Missing required environment variables`** — Ensure `ACTIVATE_API_KEY`, `ACTIVATE_ORG_ID`, and `ACTIVATE_ORG_NAME` are set in `.env`.

**`API request failed: GET ... -> 401`** — API key is invalid or expired. Generate a new one in ACTIVATE under Account > Authentication > API Keys.

**`sacctmgr failed`** — The script must run as a user with Slurm admin privileges (typically root or the slurm user). Check that `sacctmgr` is on `$PATH`.

**`Cannot connect to ACTIVATE API`** — Check network connectivity and `ACTIVATE_API_URL`. The Slurm head node needs outbound HTTPS access to the ACTIVATE API.

**Cron not running** — Check `crontab -l`, verify paths are absolute, and check `/var/log/pw-groups-sync.log` for output.
