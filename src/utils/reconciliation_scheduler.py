import sys
import time
import logging

from pathlib import Path
from datetime import datetime, timezone


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

UTILS_DIR = PROJECT_ROOT / "src" / "utils"

sys.path.insert(0, str(UTILS_DIR))

from reconcile_events import run_reconciliation


# ==================================================
# 2. SCHEDULER CONFIGURATION
# ==================================================

RECONCILIATION_INTERVAL = 30


# ==================================================
# 3. LOGGING CONFIGURATION
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("CommercePulse-Reconciliation")


# ==================================================
# 4. RUN RECONCILIATION
# ==================================================

def execute_reconciliation():

    start_time = datetime.now(timezone.utc)

    logger.info(
        "Starting automatic reconciliation..."
    )

    try:

        run_reconciliation()

        duration = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds()

        logger.info(
            "Reconciliation completed successfully. "
            "Duration: %.2f seconds",
            duration
        )

    except Exception:

        logger.exception(
            "Reconciliation failed. "
            "The scheduler will try again."
        )


# ==================================================
# 5. CONTINUOUS SCHEDULER
# ==================================================

def start_scheduler():

    logger.info(
        "=========================================="
    )

    logger.info(
        "COMMERCEPULSE AUTOMATIC RECONCILIATION"
    )

    logger.info(
        "=========================================="
    )

    logger.info(
        "Reconciliation interval: %s seconds",
        RECONCILIATION_INTERVAL
    )

    logger.info(
        "Scheduler started."
    )

    try:

        while True:

            execute_reconciliation()

            logger.info(
                "Next reconciliation in %s seconds.",
                RECONCILIATION_INTERVAL
            )

            time.sleep(
                RECONCILIATION_INTERVAL
            )

    except KeyboardInterrupt:

        logger.info(
            "Reconciliation scheduler stopped by user."
        )


# ==================================================
# 6. ENTRY POINT
# ==================================================

if __name__ == "__main__":

    start_scheduler()