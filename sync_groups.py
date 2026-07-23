#!/usr/bin/env python3
"""Sync ACTIVATE platform groups to Slurm accounts and associations.

Reads group membership from the ACTIVATE API or from a local nsscache
group cache file and ensures corresponding Slurm accounts and user
associations exist. Supports dry-run mode and group filtering for
incremental rollout.

Usage:
    uv run sync_groups.py

Configuration via .env file or environment variables (see env.example).
"""

from __future__ import annotations

import fcntl
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from urllib.parse import quote

import httpx
from dotenv import load_dotenv
from parallelworks_client import Client, CredentialError, SyncClient, extract_platform_host

# Load .env from the script's directory
load_dotenv(Path(__file__).resolve().parent / ".env")

__version__ = "1.1.0"

logger = logging.getLogger("sync_groups")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    groups_source: str
    nsscache_group_file: str
    activate_api_url: str
    activate_api_key: str
    activate_org_name: str
    slurm_cluster: str
    slurm_account_org: str
    sync_groups: list[str]
    sync_allocations: bool
    slurm_allocation_tres: str
    dry_run: bool
    log_level: str
    heartbeat_url: str


def load_config() -> Config:
    """Load and validate configuration from environment variables.

    In api mode, only ACTIVATE_API_KEY is required: the API URL is embedded
    in the key, the org name comes from the whoami endpoint, and the Slurm
    settings default from the org name. In nsscache mode, SLURM_ACCOUNT_ORG
    is required (unless ACTIVATE_ORG_NAME provides a fallback) and the
    cluster is read from slurmdbd when unset.
    """
    groups_source = os.environ.get("GROUPS_SOURCE", "api").strip().lower()
    if groups_source not in ("api", "nsscache"):
        raise SystemExit(f"Invalid GROUPS_SOURCE '{groups_source}' (expected 'api' or 'nsscache')")

    api_key = os.environ.get("ACTIVATE_API_KEY", "")
    org_name = os.environ.get("ACTIVATE_ORG_NAME", "")

    missing = []
    if groups_source == "api":
        if not api_key:
            missing.append("ACTIVATE_API_KEY")
    else:
        if not os.environ.get("SLURM_ACCOUNT_ORG") and not org_name:
            missing.append("SLURM_ACCOUNT_ORG")
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")

    sync_allocations = os.environ.get("SYNC_ALLOCATIONS", "false").lower() in ("true", "1", "yes")
    if groups_source == "nsscache" and sync_allocations:
        raise SystemExit("SYNC_ALLOCATIONS requires GROUPS_SOURCE=api (allocation data is not in the group cache)")

    api_url = os.environ.get("ACTIVATE_API_URL", "").rstrip("/")
    if not api_url and api_key:
        try:
            api_url = f"https://{extract_platform_host(api_key)}"
        except CredentialError:
            api_url = "https://activate.parallel.works"

    sync_groups_raw = os.environ.get("SYNC_GROUPS", "").strip()
    sync_groups = [g.strip() for g in sync_groups_raw.split(",") if g.strip()] if sync_groups_raw else []

    return Config(
        groups_source=groups_source,
        nsscache_group_file=os.environ.get("NSSCACHE_GROUP_FILE", "/etc/group.cache"),
        activate_api_url=api_url,
        activate_api_key=api_key,
        activate_org_name=org_name,
        slurm_cluster=os.environ.get("SLURM_CLUSTER", org_name),
        slurm_account_org=os.environ.get("SLURM_ACCOUNT_ORG", org_name),
        sync_groups=sync_groups,
        sync_allocations=sync_allocations,
        slurm_allocation_tres=os.environ.get("SLURM_ALLOCATION_TRES", "gres/gpu"),
        dry_run=os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes"),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        heartbeat_url=os.environ.get("HEARTBEAT_URL", ""),
    )


def setup_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level, logging.INFO))


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ActivateGroup:
    name: str
    description: str
    members: list[str]  # usernames
    allocation: int | None  # from allocations.total, e.g. GPU count
    created_at: str  # sortable creation marker, used for DefaultAccount ordering


@dataclass(frozen=True)
class SlurmAccount:
    name: str
    description: str
    organization: str


@dataclass(frozen=True)
class SlurmAssociation:
    user: str
    account: str
    default_account: str
    cluster: str


@dataclass
class SyncPlan:
    accounts_to_add: list[tuple[str, str]] = field(default_factory=list)       # (name, description)
    accounts_to_remove: list[str] = field(default_factory=list)                # account names
    associations_to_add: list[tuple[str, str, str | None]] = field(default_factory=list)  # (user, acct, default)
    associations_to_remove: list[tuple[str, str]] = field(default_factory=list)  # (user, acct)
    defaults_to_update: list[tuple[str, str]] = field(default_factory=list)    # (user, new_default)
    allocations_to_set: list[tuple[str, int]] = field(default_factory=list)    # (account, allocation_value)
    new_users: list[str] = field(default_factory=list)                        # users with no prior associations

    def is_empty(self) -> bool:
        return not any([
            self.accounts_to_add,
            self.accounts_to_remove,
            self.associations_to_add,
            self.associations_to_remove,
            self.defaults_to_update,
            self.allocations_to_set,
        ])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize_description(desc: str) -> str:
    """Remove characters that could break sacctmgr commands."""
    return desc.replace('"', "'").replace("|", "-").strip()[:200]


def normalize_account_name(name: str) -> str:
    """Convert ACTIVATE group name to a valid Slurm account name."""
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9_-]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


# ---------------------------------------------------------------------------
# ACTIVATE API client (parallelworks-client SDK)
# ---------------------------------------------------------------------------

def _api_get(client: SyncClient, path: str) -> dict | list | str:
    """GET via the SDK client. Retries on transient network errors."""
    retries = 3
    retry_delay = 10  # seconds between retries

    for attempt in range(1, retries + 1):
        try:
            resp = client.get(path)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.error(
                "API request failed: GET %s -> %d %s",
                path,
                e.response.status_code,
                e.response.text[:500],
            )
            raise  # HTTP errors are not transient, don't retry
        except httpx.TransportError as e:
            if attempt < retries:
                logger.warning(
                    "Network error on attempt %d/%d, retrying in %ds: %s",
                    attempt, retries, retry_delay, e,
                )
                time.sleep(retry_delay)
            else:
                logger.error("Cannot reach the ACTIVATE API after %d attempts", retries)
                raise


def resolve_api_defaults(config: Config, client: SyncClient) -> Config:
    """Fill org-derived settings that were not set explicitly."""
    if not config.activate_org_name:
        org_name = _api_get(client, "/api/auth/whoami/organization")
        logger.info("Resolved organization from API key: %s", org_name)
        config = replace(config, activate_org_name=org_name)
    if not config.slurm_cluster:
        config = replace(config, slurm_cluster=config.activate_org_name)
    if not config.slurm_account_org:
        config = replace(config, slurm_account_org=config.activate_org_name)
    return config


def apply_group_filter(raw_groups: list[dict], config: Config, source: str) -> list[dict]:
    """Filter raw groups (dicts with a 'name' key) down to SYNC_GROUPS if set."""
    if not config.sync_groups:
        return raw_groups
    filtered = [g for g in raw_groups if g["name"] in config.sync_groups]
    logger.info(
        "Filtered to %d groups matching SYNC_GROUPS: %s",
        len(filtered),
        ", ".join(config.sync_groups),
    )
    found_names = {g["name"] for g in filtered}
    for name in config.sync_groups:
        if name not in found_names:
            logger.warning("SYNC_GROUPS filter includes '%s' but no such group found in %s", name, source)
    return filtered


def fetch_activate_state(config: Config, client: SyncClient) -> list[ActivateGroup]:
    """Fetch all groups and their members from ACTIVATE."""
    logger.info("Fetching ACTIVATE groups for org %s...", config.activate_org_name)
    raw_groups = _api_get(client, f"/api/organizations/{config.activate_org_name}/groups")
    logger.info("Found %d groups in ACTIVATE", len(raw_groups))

    raw_groups = apply_group_filter(raw_groups, config, "ACTIVATE")

    groups = []
    for g in raw_groups:
        data = _api_get(
            client,
            f"/api/organizations/{config.activate_org_name}/groups/{quote(g['name'], safe='')}/members",
        )
        members = sorted({m["username"].lower() for m in data})
        account_name = normalize_account_name(g["name"])
        allocation = g.get("allocations", {}).get("total", None)
        logger.debug("Group %s (%s): %d members, allocation=%s", g["name"], account_name, len(members), allocation)
        groups.append(ActivateGroup(
            name=account_name,
            description=sanitize_description(g.get("description") or g["name"]),
            members=members,
            allocation=allocation,
            created_at=g.get("createdAt", ""),
        ))

    total_members = sum(len(g.members) for g in groups)
    logger.info("Fetched %d groups with %d total member assignments", len(groups), total_members)
    return groups


# ---------------------------------------------------------------------------
# nsscache source
# ---------------------------------------------------------------------------

def fetch_nsscache_state(config: Config) -> list[ActivateGroup]:
    """Read groups and members from an nsscache group cache file.

    The file uses standard group(5) format and contains only the groups
    nsscache synced from the platform, so every entry is a sync candidate.
    """
    path = Path(config.nsscache_group_file)
    logger.info("Reading groups from %s...", path)
    raw_groups = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) < 4:
            logger.warning("Skipping malformed group entry: %s", line[:80])
            continue
        members = sorted({m.strip().lower() for m in parts[3].split(",") if m.strip()})
        raw_groups.append({"name": parts[0], "gid": parts[2], "members": members})
    logger.info("Found %d groups in %s", len(raw_groups), path)

    raw_groups = apply_group_filter(raw_groups, config, str(path))

    groups = []
    for g in raw_groups:
        account_name = normalize_account_name(g["name"])
        logger.debug("Group %s (%s): %d members", g["name"], account_name, len(g["members"]))
        groups.append(ActivateGroup(
            name=account_name,
            description=sanitize_description(g["name"]),
            members=g["members"],
            allocation=None,
            # gids are assigned in creation order, so zero-padding preserves
            # the earliest-created-group-wins DefaultAccount semantics of the
            # API's createdAt ordering
            created_at=g["gid"].zfill(10),
        ))

    total_members = sum(len(g.members) for g in groups)
    logger.info("Fetched %d groups with %d total member assignments", len(groups), total_members)
    return groups


# ---------------------------------------------------------------------------
# Slurm state (sacctmgr wrappers)
# ---------------------------------------------------------------------------

def run_sacctmgr(
    args: list[str],
    config: Config,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """Low-level sacctmgr wrapper. In dry-run mode, mutating commands are logged but not executed."""
    cmd = ["sacctmgr"] + args
    is_mutating = any(verb in args for verb in ("add", "remove", "modify", "delete", "create"))

    if config.dry_run and is_mutating:
        logger.info("[DRY RUN] Would run: %s", " ".join(cmd))
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    logger.debug("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            check=check,
        )
        if result.stderr.strip():
            logger.warning("sacctmgr stderr: %s", result.stderr.strip())
        return result
    except subprocess.CalledProcessError as e:
        logger.error("sacctmgr failed (exit %d): %s", e.returncode, " ".join(cmd))
        logger.error("stdout: %s", e.stdout)
        logger.error("stderr: %s", e.stderr)
        raise
    except subprocess.TimeoutExpired:
        logger.error("sacctmgr timed out after 30s: %s", " ".join(cmd))
        raise


def resolve_slurm_cluster(config: Config) -> str:
    """Read the cluster name from slurmdbd when exactly one is registered."""
    result = run_sacctmgr(["list", "cluster", "-n", "-P", "format=Cluster"], config)
    clusters = [line.split("|")[0] for line in result.stdout.strip().splitlines() if line.strip()]
    if len(clusters) == 1:
        logger.info("Resolved Slurm cluster from slurmdbd: %s", clusters[0])
        return clusters[0]
    raise SystemExit(
        f"SLURM_CLUSTER is not set and slurmdbd has {len(clusters)} clusters; set SLURM_CLUSTER explicitly"
    )


def slurm_list_accounts(config: Config) -> list[SlurmAccount]:
    """List current Slurm accounts (accounts are global, not per-cluster)."""
    result = run_sacctmgr(
        ["list", "account", "-n", "-P",
         "format=Account,Description,Organization"],
        config,
    )
    accounts = []
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 3:
            accounts.append(SlurmAccount(
                name=parts[0],
                description=parts[1],
                organization=parts[2],
            ))
    return accounts


def slurm_list_associations(config: Config) -> list[SlurmAssociation]:
    """List current Slurm user associations for the cluster.

    Uses 'show user withassoc' instead of 'list associations' because
    the latter does not populate the DefaultAccount field.
    """
    result = run_sacctmgr(
        ["show", "user", "-n", "-P", "withassoc",
         "format=User,Account,DefaultAccount",
         "where", f"cluster={config.slurm_cluster}"],
        config,
    )
    associations = []
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 3:
            user = parts[0]
            if not user:
                continue
            associations.append(SlurmAssociation(
                user=user,
                account=parts[1],
                default_account=parts[2],
                cluster=config.slurm_cluster,
            ))
    return associations


def slurm_add_account(config: Config, name: str, description: str) -> None:
    logger.info("Adding Slurm account: %s", name)
    run_sacctmgr(
        ["-i", "add", "account", name,
         f"Cluster={config.slurm_cluster}",
         f"Description={sanitize_description(description)}",
         f"Organization={config.slurm_account_org}"],
        config,
    )


def slurm_remove_account(config: Config, name: str) -> None:
    logger.info("Removing Slurm account: %s", name)
    run_sacctmgr(
        ["-i", "remove", "account",
         "where", f"name={name}"],
        config,
    )


def slurm_add_user(config: Config, username: str, account: str, default_account: str | None = None) -> None:
    logger.info("Adding user association: %s -> %s", username, account)
    cmd = ["-i", "add", "user", username, f"Account={account}"]
    if default_account:
        cmd.append(f"DefaultAccount={default_account}")
    run_sacctmgr(cmd, config)


def slurm_set_user_qos(config: Config, username: str) -> None:
    """Assign normal QoS to a newly created user."""
    logger.info("Setting QoS for new user: %s (qos+=normal, defaultqos+=normal)", username)
    run_sacctmgr(
        ["-i", "modify", "user",
         "where", f"user={username}",
         "set", "qos+=normal", "defaultqos+=normal"],
        config,
    )


def slurm_remove_user_association(config: Config, username: str, account: str) -> None:
    logger.info("Removing user association: %s -> %s", username, account)
    result = run_sacctmgr(
        ["-i", "remove", "user",
         "where", f"user={username}",
         f"account={account}",
         f"cluster={config.slurm_cluster}"],
        config,
        check=False,
    )
    if result.returncode != 0 and "Nothing deleted" not in result.stdout:
        raise subprocess.CalledProcessError(result.returncode, result.args)


def slurm_set_account_allocation(config: Config, account: str, tres: str, value: int) -> None:
    logger.info("Setting allocation for %s: GrpTRES=%s=%d", account, tres, value)
    run_sacctmgr(
        ["-i", "modify", "account",
         "where", f"name={account}",
         f"cluster={config.slurm_cluster}",
         "set", f"GrpTRES={tres}={value}"],
        config,
    )


def slurm_modify_default_account(config: Config, username: str, new_default: str) -> None:
    logger.info("Updating DefaultAccount for %s -> %s", username, new_default)
    run_sacctmgr(
        ["-i", "modify", "user",
         "where", f"user={username}",
         f"cluster={config.slurm_cluster}",
         "set", f"DefaultAccount={new_default}"],
        config,
    )


# ---------------------------------------------------------------------------
# Diff / sync logic
# ---------------------------------------------------------------------------

def compute_desired_state(
    groups: list[ActivateGroup],
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, str], dict[str, int]]:
    """Derive desired Slurm state from ACTIVATE groups.

    Returns:
        desired_accounts: {account_name: description}
        desired_associations: {username: set of account_names}
        desired_defaults: {username: default_account_name}
        desired_allocations: {account_name: allocation_value}
    """
    desired_accounts: dict[str, str] = {}
    desired_associations: dict[str, set[str]] = {}
    desired_allocations: dict[str, int] = {}

    # Map account name → created_at for DefaultAccount ordering
    account_created_at: dict[str, str] = {}

    for group in groups:
        desired_accounts[group.name] = group.description
        account_created_at[group.name] = group.created_at
        if group.allocation is not None:
            desired_allocations[group.name] = group.allocation
        for username in group.members:
            desired_associations.setdefault(username, set()).add(group.name)

    # DefaultAccount = earliest-created group (falls back to alphabetical if no timestamps)
    desired_defaults: dict[str, str] = {}
    for username, accounts in desired_associations.items():
        desired_defaults[username] = sorted(
            accounts,
            key=lambda a: (account_created_at.get(a, ""), a),
        )[0]

    return desired_accounts, desired_associations, desired_defaults, desired_allocations


def compute_sync_plan(
    desired_accounts: dict[str, str],
    desired_associations: dict[str, set[str]],
    desired_defaults: dict[str, str],
    desired_allocations: dict[str, int],
    current_accounts: list[SlurmAccount],
    current_associations: list[SlurmAssociation],
    managed_account_names: set[str],
    sync_allocations: bool = False,
) -> SyncPlan:
    """Diff desired vs current Slurm state. Only operates on managed accounts."""
    plan = SyncPlan()

    current_account_names = {a.name for a in current_accounts}
    desired_account_names = set(desired_accounts.keys())

    # --- ACCOUNTS ---
    for name in sorted(desired_account_names - current_account_names):
        plan.accounts_to_add.append((name, desired_accounts[name]))

    for name in sorted((current_account_names & managed_account_names) - desired_account_names):
        plan.accounts_to_remove.append(name)

    # --- ASSOCIATIONS ---
    # Build current state maps (only for managed accounts)
    current_assoc_map: dict[str, set[str]] = {}
    current_default_map: dict[str, str] = {}
    for assoc in current_associations:
        if assoc.account in managed_account_names:
            current_assoc_map.setdefault(assoc.user, set()).add(assoc.account)
            current_default_map[assoc.user] = assoc.default_account

    all_users = set(desired_associations.keys()) | set(current_assoc_map.keys())

    for username in sorted(all_users):
        desired_accts = desired_associations.get(username, set())
        current_accts = current_assoc_map.get(username, set())

        # Associations to add
        to_add = sorted(desired_accts - current_accts)
        if to_add and not current_accts:
            plan.new_users.append(username)
        for acct in to_add:
            # Set DefaultAccount when adding a brand-new user (no existing associations)
            default = desired_defaults.get(username) if not current_accts and acct == to_add[0] else None
            plan.associations_to_add.append((username, acct, default))

        # Associations to remove
        for acct in sorted(current_accts - desired_accts):
            plan.associations_to_remove.append((username, acct))

    # --- DEFAULT ACCOUNT UPDATES ---
    for username in sorted(desired_associations.keys()):
        desired_default = desired_defaults[username]
        current_default = current_default_map.get(username)
        if current_default is not None and current_default != desired_default:
            plan.defaults_to_update.append((username, desired_default))

    # --- ALLOCATIONS ---
    if sync_allocations:
        for account_name in sorted(desired_allocations.keys()):
            plan.allocations_to_set.append((account_name, desired_allocations[account_name]))

    return plan


def log_sync_plan(plan: SyncPlan) -> None:
    """Log a human-readable summary of the sync plan."""
    logger.info("=== Sync Plan ===")

    if plan.is_empty():
        logger.info("  Nothing to do, Slurm is already in sync")
        logger.info("=================")
        return

    if plan.accounts_to_add:
        logger.info("  Accounts to add (%d):", len(plan.accounts_to_add))
        for name, desc in plan.accounts_to_add:
            logger.info("    + %s (%s)", name, desc)

    if plan.accounts_to_remove:
        logger.info("  Accounts to remove (%d):", len(plan.accounts_to_remove))
        for name in plan.accounts_to_remove:
            logger.info("    - %s", name)

    if plan.associations_to_add:
        logger.info("  Associations to add (%d):", len(plan.associations_to_add))
        for user, acct, default in plan.associations_to_add:
            suffix = f" (DefaultAccount={default})" if default else ""
            logger.info("    + %s -> %s%s", user, acct, suffix)

    if plan.associations_to_remove:
        logger.info("  Associations to remove (%d):", len(plan.associations_to_remove))
        for user, acct in plan.associations_to_remove:
            logger.info("    - %s -> %s", user, acct)

    if plan.defaults_to_update:
        logger.info("  Default account updates (%d):", len(plan.defaults_to_update))
        for user, new_default in plan.defaults_to_update:
            logger.info("    ~ %s -> DefaultAccount=%s", user, new_default)

    if plan.new_users:
        logger.info("  QoS to assign (%d new users):", len(plan.new_users))
        for user in plan.new_users:
            logger.info("    ~ %s -> qos+=normal, defaultqos+=normal", user)

    if plan.allocations_to_set:
        logger.info("  Allocations to set (%d):", len(plan.allocations_to_set))
        for account, value in plan.allocations_to_set:
            logger.info("    ~ %s -> GrpTRES=%d", account, value)

    logger.info("=================")


def execute_sync_plan(plan: SyncPlan, config: Config) -> int:
    """Execute the sync plan in dependency order. Returns count of errors."""
    errors = 0

    # Phase 1: Create new accounts
    for name, desc in plan.accounts_to_add:
        try:
            slurm_add_account(config, name, desc)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            errors += 1

    # Phase 2: Add new user associations
    for username, account, default in plan.associations_to_add:
        try:
            slurm_add_user(config, username, account, default)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            errors += 1

    # Phase 2b: Set QoS for brand-new users
    for username in plan.new_users:
        try:
            slurm_set_user_qos(config, username)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            errors += 1

    # Phase 3: Update default accounts
    for username, new_default in plan.defaults_to_update:
        try:
            slurm_modify_default_account(config, username, new_default)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            errors += 1

    # Phase 4: Remove user associations
    for username, account in plan.associations_to_remove:
        try:
            slurm_remove_user_association(config, username, account)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            errors += 1

    # Phase 5: Set account allocations
    for account, value in plan.allocations_to_set:
        try:
            slurm_set_account_allocation(config, account, config.slurm_allocation_tres, value)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            errors += 1

    # Phase 6: Remove empty accounts
    for name in plan.accounts_to_remove:
        try:
            slurm_remove_account(config, name)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            errors += 1

    return errors


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def ping_heartbeat(config: Config) -> None:
    """Ping the heartbeat URL to signal successful completion."""
    if not config.heartbeat_url:
        return
    try:
        resp = httpx.get(config.heartbeat_url, timeout=10)
        logger.debug("Heartbeat ping: %d", resp.status_code)
    except Exception:
        logger.warning("Failed to ping heartbeat URL (non-fatal)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    config = load_config()
    setup_logging(config.log_level)

    logger.info("sync_groups v%s starting", __version__)

    if config.dry_run:
        logger.info("=== DRY RUN MODE - no changes will be made ===")

    if config.sync_groups:
        logger.info("Group filter active: %s", ", ".join(config.sync_groups))

    # Fetch desired state
    try:
        if config.groups_source == "nsscache":
            groups = fetch_nsscache_state(config)
        else:
            with Client.with_api_key(config.activate_api_url, config.activate_api_key).sync() as client:
                config = resolve_api_defaults(config, client)
                groups = fetch_activate_state(config, client)
    except httpx.TransportError:
        logger.error("Failed to fetch ACTIVATE state: network error (already retried), aborting")
        raise SystemExit(1)
    except Exception:
        logger.exception("Failed to fetch group state: unexpected error, aborting")
        raise SystemExit(1)

    if not config.slurm_cluster:
        config = replace(config, slurm_cluster=resolve_slurm_cluster(config))

    if not groups:
        logger.warning("No groups found (check SYNC_GROUPS filter). Nothing to do.")
        return

    desired_accounts, desired_associations, desired_defaults, desired_allocations = compute_desired_state(groups)

    if not config.sync_allocations and desired_allocations:
        logger.info(
            "Allocations found for %d groups but SYNC_ALLOCATIONS is off (not applying)",
            len(desired_allocations),
        )

    # Fetch current Slurm state
    try:
        current_accounts = slurm_list_accounts(config)
        current_associations = slurm_list_associations(config)
    except subprocess.TimeoutExpired:
        logger.error("Failed to read Slurm state: sacctmgr timed out, aborting")
        raise SystemExit(1)
    except Exception:
        logger.exception("Failed to read Slurm state: unexpected error, aborting")
        raise SystemExit(1)

    logger.info(
        "Current Slurm state: %d accounts, %d user associations",
        len(current_accounts),
        len(current_associations),
    )

    # Determine managed accounts: ACTIVATE group names + existing accounts tagged with our org
    managed = set(desired_accounts.keys()) | {
        a.name for a in current_accounts if a.organization == config.slurm_account_org
    }

    plan = compute_sync_plan(
        desired_accounts,
        desired_associations,
        desired_defaults,
        desired_allocations,
        current_accounts,
        current_associations,
        managed,
        sync_allocations=config.sync_allocations,
    )
    log_sync_plan(plan)

    if plan.is_empty():
        ping_heartbeat(config)
        return

    errors = execute_sync_plan(plan, config)
    if errors:
        logger.error("Sync completed with %d error(s)", errors)
        raise SystemExit(1)

    logger.info("Sync completed successfully")
    ping_heartbeat(config)


if __name__ == "__main__":
    lock_path = Path(__file__).resolve().parent / ".sync.lock"
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("Another sync is already running, skipping.", file=sys.stderr)
        raise SystemExit(0)
    try:
        main()
    finally:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()
