#!/usr/bin/env python3
"""
Cleanup script to remove stale indicators.
Any indicator not seen across ANY feed in the last 14 days will be deleted.
Due to ON DELETE CASCADE in the database, this will also wipe out
their associated evidence, observations, and assessments.

Deletes in small batches to avoid Supabase statement timeout.
"""
import sys
import os
import structlog
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from db import get_supabase

logger = structlog.get_logger()

BATCH_SIZE = 500


def run_cleanup():
    logger.info("Starting cleanup of stale indicators")
    db = get_supabase()

    threshold = (datetime.utcnow() - timedelta(days=14)).isoformat()
    total_deleted = 0

    try:
        while True:
            # Fetch a small batch of stale indicator IDs
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

            db.table("indicators").delete().in_("id", ids).execute()

            total_deleted += len(ids)
            logger.info("Deleted batch", batch_size=len(ids), total_so_far=total_deleted)

        logger.info("Cleanup finished successfully", deleted_indicators=total_deleted, threshold=threshold)
    except Exception as e:
        logger.error("Error during cleanup", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    run_cleanup()
