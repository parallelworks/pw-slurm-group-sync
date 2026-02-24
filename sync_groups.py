#!/usr/bin/env python3
"""Sync ACTIVATE platform groups to Slurm accounts and associations.

Fetches group membership from the ACTIVATE API and ensures corresponding
Slurm accounts and user associations exist. Supports dry-run mode and
group filtering for incremental rollout.

Usage:
    uv run sync_groups.py

Configuration via .env file or environment variables (see env.example).
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load .env from the script's directory
load_dotenv(Path(__file__).resolve().parent / ".env")

logger = logging.getLogger("sync_groups")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    activate_api_url: str
    activate_api_key: str
    activate_org_id: str
    activate_org_name: str
    slurm_cluster: str
    slurm_account_org: str
    sync_groups: list[str]
    dry_run: bool
    log_level: str


def load_config() -> Config:
    """Load and validate configuration from environment variables."""
    api_key = os.environ.get("ACTIVATE_API_KEY", "")
    org_id = os.environ.get("ACTIVATE_ORG_ID", "")
    org_name = os.environ.get("ACTIVATE_ORG_NAME", "")

    missing = []
    if not api_key:
        missing.append("ACTIVATE_API_KEY")
    if not org_id:
        missing.append("ACTIVATE_ORG_ID")
    if not org_name:
        missing.append("ACTIVATE_ORG_NAME")
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")

    sync_groups_raw = os.environ.get("SYNC_GROUPS", "").strip()
    sync_groups = [g.strip() for g in sync_groups_raw.split(",") if g.strip()] if sync_groups_raw else []

    return Config(
        activate_api_url=os.environ.get("ACTIVATE_API_URL", "https://activate.parallel.works").rstrip("/"),
        activate_api_key=api_key,
        activate_org_id=org_id,
        activate_org_name=org_name,
        slurm_cluster=os.environ.get("SLURM_CLUSTER", org_name),
        slurm_account_org=os.environ.get("SLURM_ACCOUNT_ORG", org_name),
        sync_groups=sync_groups,
        dry_run=os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes"),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
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
    id: str
    name: str
    description: str
    members: list[str]  # usernames


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

    def is_empty(self) -> bool:
        return not any([
            self.accounts_to_add,
            self.accounts_to_remove,
            self.associations_to_add,
            self.associations_to_remove,
            self.defaults_to_update,
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
# ACTIVATE API client
# ---------------------------------------------------------------------------

def _api_get(config: Config, path: str, params: dict | None = None) -> dict | list:
    """Shared GET helper with auth."""
    url = f"{config.activate_api_url}{path}"
    headers = {"Authorization": f"Bearer {config.activate_api_key}"}
    logger.debug("GET %s (params: %s)", url, params)
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        logger.error(
            "API request failed: GET %s -> %d %s",
            url,
            e.response.status_code,
            e.response.text[:500],
        )
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to ACTIVATE API at %s", config.activate_api_url)
        raise


def activate_list_groups(config: Config) -> list[dict]:
    """Fetch all groups for the organization."""
    return _api_get(config, f"/api/organizations/{config.activate_org_name}/groups")


def activate_get_group_members(config: Config, group_id: str) -> list[str]:
    """Fetch member usernames for a single group."""
    data = _api_get(
        config,
        f"/api/v2/organization/teams/{group_id}",
        params={"organization": config.activate_org_id},
    )
    return [m["username"] for m in data.get("members", [])]


def fetch_activate_state(config: Config) -> list[ActivateGroup]:
    """Fetch all groups and their members from ACTIVATE."""
    logger.info("Fetching ACTIVATE groups for org %s...", config.activate_org_name)
    raw_groups = activate_list_groups(config)
    logger.info("Found %d groups in ACTIVATE", len(raw_groups))

    # Apply group filter if set
    if config.sync_groups:
        raw_groups = [g for g in raw_groups if g["name"] in config.sync_groups]
        logger.info(
            "Filtered to %d groups matching SYNC_GROUPS: %s",
            len(raw_groups),
            ", ".join(config.sync_groups),
        )
        # Warn about groups in filter that don't exist
        found_names = {g["name"] for g in raw_groups}
        for name in config.sync_groups:
            if name not in found_names:
                logger.warning("SYNC_GROUPS filter includes '%s' but no such group found in ACTIVATE", name)

    groups = []
    for g in raw_groups:
        members = activate_get_group_members(config, g["id"])
        account_name = normalize_account_name(g["name"])
        logger.debug("Group %s (%s): %d members", g["name"], account_name, len(members))
        groups.append(ActivateGroup(
            id=g["id"],
            name=account_name,
            description=sanitize_description(g.get("description", g["name"])),
            members=members,
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


def slurm_list_accounts(config: Config) -> list[SlurmAccount]:
    """List current Slurm accounts for the cluster."""
    result = run_sacctmgr(
        ["list", "account", "-n", "-P",
         "format=Account,Description,Organization",
         f"where cluster={config.slurm_cluster}"],
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
    """List current Slurm user associations for the cluster."""
    result = run_sacctmgr(
        ["list", "associations", "-n", "-P",
         "format=User,Account,DefaultAccount,Cluster",
         f"where cluster={config.slurm_cluster}"],
        config,
    )
    associations = []
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 4:
            user = parts[0]
            if not user:
                # Account-level association row (no user), skip
                continue
            associations.append(SlurmAssociation(
                user=user,
                account=parts[1],
                default_account=parts[2],
                cluster=parts[3],
            ))
    return associations


def slurm_add_account(config: Config, name: str, description: str) -> None:
    logger.info("Adding Slurm account: %s", name)
    run_sacctmgr(
        ["-i", "add", "account", name,
         f"Cluster={config.slurm_cluster}",
         f'Description="{sanitize_description(description)}"',
         f'Organization="{config.slurm_account_org}"'],
        config,
    )


def slurm_remove_account(config: Config, name: str) -> None:
    logger.info("Removing Slurm account: %s", name)
    run_sacctmgr(
        ["-i", "remove", "account",
         f"where name={name}",
         f"cluster={config.slurm_cluster}"],
        config,
    )


def slurm_add_user(config: Config, username: str, account: str, default_account: str | None = None) -> None:
    logger.info("Adding user association: %s -> %s", username, account)
    cmd = ["-i", "add", "user", username, f"Account={account}"]
    if default_account:
        cmd.append(f"DefaultAccount={default_account}")
    run_sacctmgr(cmd, config)


def slurm_remove_user_association(config: Config, username: str, account: str) -> None:
    logger.info("Removing user association: %s -> %s", username, account)
    run_sacctmgr(
        ["-i", "remove", "user",
         f"where user={username}",
         f"account={account}",
         f"cluster={config.slurm_cluster}"],
        config,
    )


def slurm_modify_default_account(config: Config, username: str, new_default: str) -> None:
    logger.info("Updating DefaultAccount for %s -> %s", username, new_default)
    run_sacctmgr(
        ["-i", "modify", "user",
         f"where user={username}",
         f"cluster={config.slurm_cluster}",
         f"set DefaultAccount={new_default}"],
        config,
    )


# ---------------------------------------------------------------------------
# Diff / sync logic
# ---------------------------------------------------------------------------

def compute_desired_state(
    groups: list[ActivateGroup],
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, str]]:
    """Derive desired Slurm state from ACTIVATE groups.

    Returns:
        desired_accounts: {account_name: description}
        desired_associations: {username: set of account_names}
        desired_defaults: {username: default_account_name}
    """
    desired_accounts: dict[str, str] = {}
    desired_associations: dict[str, set[str]] = {}

    for group in groups:
        desired_accounts[group.name] = group.description
        for username in group.members:
            desired_associations.setdefault(username, set()).add(group.name)

    # DefaultAccount = first group name alphabetically
    desired_defaults: dict[str, str] = {}
    for username, accounts in desired_associations.items():
        desired_defaults[username] = sorted(accounts)[0]

    return desired_accounts, desired_associations, desired_defaults


def compute_sync_plan(
    desired_accounts: dict[str, str],
    desired_associations: dict[str, set[str]],
    desired_defaults: dict[str, str],
    current_accounts: list[SlurmAccount],
    current_associations: list[SlurmAssociation],
    managed_account_names: set[str],
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

    # Phase 5: Remove empty accounts
    for name in plan.accounts_to_remove:
        try:
            slurm_remove_account(config, name)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            errors += 1

    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    config = load_config()
    setup_logging(config.log_level)

    if config.dry_run:
        logger.info("=== DRY RUN MODE - no changes will be made ===")

    if config.sync_groups:
        logger.info("Group filter active: %s", ", ".join(config.sync_groups))

    # Fetch desired state from ACTIVATE
    try:
        groups = fetch_activate_state(config)
    except Exception:
        logger.exception("Failed to fetch ACTIVATE state, aborting")
        raise SystemExit(1)

    if not groups:
        logger.warning("No groups found (check SYNC_GROUPS filter). Nothing to do.")
        return

    desired_accounts, desired_associations, desired_defaults = compute_desired_state(groups)

    # Fetch current Slurm state
    try:
        current_accounts = slurm_list_accounts(config)
        current_associations = slurm_list_associations(config)
    except Exception:
        logger.exception("Failed to read Slurm state, aborting")
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
        current_accounts,
        current_associations,
        managed,
    )
    log_sync_plan(plan)

    if plan.is_empty():
        return

    errors = execute_sync_plan(plan, config)
    if errors:
        logger.error("Sync completed with %d error(s)", errors)
        raise SystemExit(1)
    else:
        logger.info("Sync completed successfully")


if __name__ == "__main__":
    main()
