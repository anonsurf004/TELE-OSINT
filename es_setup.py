"""
es_setup.py — Bootstrap and migrate Elasticsearch indices.

Usage
-----
  python es_setup.py                # Create indices if they don't exist (safe)
  python es_setup.py --migrate      # Push new fields to existing live indices
  python es_setup.py --force        # DELETE and recreate indices (destroys data)
  python es_setup.py --force --yes  # Skip the destruction confirmation prompt

Indices managed
---------------
  tele_messages   (or ELASTICSEARCH_INDEX_MESSAGES from .env)
  tele_channels   (or ELASTICSEARCH_INDEX_CHANNELS from .env)

Exit codes
----------
  0  Success
  1  Elasticsearch unreachable
  2  User aborted --force
  3  Index operation failed
"""
from __future__ import annotations

import argparse
import asyncio
import sys

from elasticsearch import NotFoundError

from config import get_settings
from indexer.es_client import close_es_client, get_es_client, wait_for_elasticsearch
from indexer.index_manager import (
    CHANNEL_INDEX_SETTINGS,
    MESSAGE_INDEX_SETTINGS,
    apply_mapping_updates,
    ensure_indices,
)
from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)
settings = get_settings()


# ── Helpers ───────────────────────────────────────────────────────────────────

async def delete_index(index_name: str) -> None:
    """Delete an index if it exists; silently no-op otherwise."""
    es = get_es_client()
    try:
        await es.indices.delete(index=index_name)
        log.info("es_setup.deleted", index=index_name)
    except NotFoundError:
        log.info("es_setup.delete_skip_missing", index=index_name)


async def print_mapping_summary(index_name: str) -> None:
    """Log the field names present in a live index mapping."""
    es = get_es_client()
    try:
        mapping = await es.indices.get_mapping(index=index_name)
        props = mapping[index_name]["mappings"].get("properties", {})
        log.info(
            "es_setup.mapping_summary",
            index=index_name,
            field_count=len(props),
            fields=sorted(props.keys()),
        )
    except Exception as exc:
        log.warning("es_setup.mapping_summary_failed", index=index_name, error=str(exc))


async def print_index_stats(index_name: str) -> None:
    """Log doc count and store size for an existing index."""
    es = get_es_client()
    try:
        stats = await es.indices.stats(index=index_name, metric="docs,store")
        primaries = stats["indices"][index_name]["primaries"]
        log.info(
            "es_setup.index_stats",
            index=index_name,
            doc_count=primaries["docs"]["count"],
            deleted_docs=primaries["docs"]["deleted"],
            store_size_bytes=primaries["store"]["size_in_bytes"],
        )
    except Exception as exc:
        log.warning("es_setup.stats_failed", index=index_name, error=str(exc))


def _confirm_destruction(indices: list[str]) -> bool:
    """Prompt the operator to confirm before deleting indices."""
    print(
        "\n[WARNING] --force will permanently delete and recreate:\n"
        + "\n".join(f"  • {i}" for i in indices)
        + "\n\nAll indexed data will be lost."
    )
    answer = input("Type 'yes' to confirm: ").strip().lower()
    return answer == "yes"


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(*, force: bool, migrate: bool, yes: bool) -> int:
    """
    Returns an exit code (0 = success, non-zero = failure).
    """
    log.info(
        "es_setup.start",
        es_url=settings.elasticsearch_url,
        messages_index=settings.elasticsearch_index_messages,
        channels_index=settings.elasticsearch_index_channels,
        force=force,
        migrate=migrate,
    )

    # ── 1. Wait for Elasticsearch ──────────────────────────────────────────
    try:
        await wait_for_elasticsearch()
    except Exception as exc:
        log.error("es_setup.unreachable", error=str(exc))
        print(
            f"\nCould not connect to Elasticsearch at {settings.elasticsearch_url}\n"
            "Make sure it is running:\n"
            "  docker compose up -d elasticsearch\n",
            file=sys.stderr,
        )
        return 1

    msg_index = settings.elasticsearch_index_messages
    ch_index  = settings.elasticsearch_index_channels

    # ── 2. --force: confirm then drop ─────────────────────────────────────
    if force:
        if not yes and not _confirm_destruction([msg_index, ch_index]):
            print("Aborted.", file=sys.stderr)
            return 2
        log.info("es_setup.force_delete")
        await delete_index(msg_index)
        await delete_index(ch_index)

    # ── 3. --migrate: push mapping additions to live indices ───────────────
    if migrate and not force:
        log.info("es_setup.migrate")
        try:
            await apply_mapping_updates(get_es_client())
        except Exception as exc:
            log.error("es_setup.migrate_failed", error=str(exc))
            return 3
        await print_mapping_summary(msg_index)
        await print_mapping_summary(ch_index)
        await close_es_client()
        log.info("es_setup.done")
        return 0

    # ── 4. Create indices (idempotent) ─────────────────────────────────────
    try:
        await ensure_indices(get_es_client())
    except Exception as exc:
        log.error("es_setup.create_failed", error=str(exc))
        return 3

    # ── 5. Verification summaries ──────────────────────────────────────────
    await print_mapping_summary(msg_index)
    await print_mapping_summary(ch_index)
    await print_index_stats(msg_index)
    await print_index_stats(ch_index)

    await close_es_client()
    log.info("es_setup.done")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bootstrap Elasticsearch indices for the Telegram OSINT platform.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python es_setup.py               # create indices if missing\n"
            "  python es_setup.py --migrate     # add new fields to live indices\n"
            "  python es_setup.py --force       # drop and recreate (prompts)\n"
            "  python es_setup.py --force --yes # drop and recreate (no prompt)\n"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete and recreate indices. Destroys all indexed data.",
    )
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Push new field mappings to existing live indices (safe, additive only).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the --force confirmation prompt (for CI/automation).",
    )
    args = parser.parse_args()

    exit_code = asyncio.run(
        main(force=args.force, migrate=args.migrate, yes=args.yes)
    )
    sys.exit(exit_code)
