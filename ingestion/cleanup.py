#!/usr/bin/env python3
"""
Cleanup script to remove stale indicators.
Any indicator not seen across ANY feed in the last 14 days will be deleted.
Due to ON DELETE CASCADE in the database, this will also wipe out
their associated evidence, observations, and assessments.

Deletes in small batches with retry to avoid Supabase statement timeout.
"""
import sys
import os
import time
import structlog
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from db import get_supabase

logger = structlog.get_logger()

BATCH_SIZE = 100
MAX_RETRIES = 3
RETRY_DELAY = 5


def delete_batch(db, ids):
    """Delete a batch of indicators by ID, with retry on timeout."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            db.table("indicators").delete().in_("id", ids).execute()
            return True
        except Exception as e:
            err = str(e)
            if "57014" in err or "statement timeout" in err.lower():
                logger.warning(
                    "Timeout on batch delete, retrying",
                    attempt=attempt,
                    batch_size=len(ids),
                )
                time.sleep(RETRY_DELAY * attempt)
            else:
                raise
    return False


def run_cleanup():
    logger.info("Starting cleanup of stale indicators")
    db = get_supabase()

    threshold = (datetime.utcnow() - timedelta(days=14)).isoformat()
    total_deleted = 0
    failed_batches = 0

    try:
        while True:
            batch = (
                db.table("indicators")
                .select("id")
                .lt("last_seen", threshold)
                .limit(BATCH_SIZE)
                .execute()
            )

            if not batch.data:
                break

            ids = [row["id"] for row in batch.data]

            if delete_batch(db, ids):
                total_deleted += len(ids)
                logger.info("Deleted batch", batch_size=len(ids), total_so_far=total_deleted)
            else:
                failed_batches += 1
                logger.warning("Batch failed after retries, skipping", batch_size=len(ids))
                if failed_batches >= 3:
                    logger.error("Too many failed batches, stopping")
                    break

        logger.info(
            "Cleanup finished",
            deleted_indicators=total_deleted,
            failed_batches=failed_batches,
            threshold=threshold,
        )

        if failed_batches > 0 and total_deleted == 0:
            sys.exit(1)

    except Exception as e:
        logger.error("Error during cleanup", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    run_cleanup()
