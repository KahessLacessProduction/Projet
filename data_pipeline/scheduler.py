"""Continuous Deployment Worker / Scheduler (Niveau 3 - Option 1).
Runs continuously or periodically (scheduler/cron) to detect, fetch,
and process newly arriving transaction files from S3 or local folders.
Designed for containerized execution in production (Docker / Kubernetes).
"""
import logging
import os
import signal
import sys
import time
from pathlib import Path
import schedule

from config import (
    POLL_INTERVAL_SECONDS,
    RAW_DATA_DIR,
    DB_URL,
    S3_ENDPOINT_URL,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET_NAME,
)
from data_pipeline.pipeline import run_pipeline
from data_pipeline.s3_service import run_s3_pipeline, get_s3_client, list_s3_files

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SCHEDULER] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("data_pipeline.scheduler")

RUNNING = True


def handle_shutdown(signum, frame):
    """Graceful shutdown signal handler for Docker / Kubernetes termination."""
    global RUNNING
    logger.info(f"Signal de terminaison reçu ({signum}). Arrêt propre du scheduler...")
    RUNNING = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def check_and_process_cycle():
    """Single polling cycle: checks for new data in S3 and local storage, executes pipeline."""
    logger.info("--- Début du cycle de détection de nouvelles données ---")

    # 1. Check S3 if endpoint is reachable
    try:
        s3_client = get_s3_client()
        s3_keys = list_s3_files(s3_client, bucket_name=S3_BUCKET_NAME)
        if s3_keys:
            logger.info(f"Détection de {len(s3_keys)} objet(s) dans le bucket S3 '{S3_BUCKET_NAME}'.")
            summary = run_s3_pipeline(
                s3_client=s3_client,
                bucket_name=S3_BUCKET_NAME,
                db_url=DB_URL,
                generate_local_first=False,
            )
            logger.info(
                f"Cycle S3 terminé : {summary['processed_files']} traités, "
                f"{summary['skipped_files']} doublons ignorés."
            )
    except Exception as s3_err:
        logger.warning(
            f"S3 non disponible ou en attente ({s3_err}). Vérification du stockage local..."
        )

    # 2. Check Local raw folder
    try:
        summary_local = run_pipeline(
            data_dir=RAW_DATA_DIR,
            db_url=DB_URL,
            generate_data=False,
        )
        logger.info(
            f"Cycle local terminé : {summary_local['processed_files']} traités, "
            f"{summary_local['skipped_files']} doublons ignorés."
        )
    except Exception as local_err:
        logger.error(f"Erreur durant le traitement local : {local_err}")

    logger.info("--- Fin du cycle de détection ---")


def start_scheduler(interval_seconds: int = POLL_INTERVAL_SECONDS, max_cycles: int = None):
    """Start continuous polling scheduler loop."""
    logger.info(
        f"Démarrage du Scheduler CD (intervalle: {interval_seconds} secondes). "
        f"Surveillance active du stockage."
    )

    # Run immediately on launch
    check_and_process_cycle()

    # Schedule recurring task
    schedule.every(interval_seconds).seconds.do(check_and_process_cycle)

    cycle_count = 0
    while RUNNING:
        schedule.run_pending()
        time.sleep(1)
        cycle_count += 1
        if max_cycles and cycle_count >= max_cycles:
            logger.info(f"Nombre maximal de cycles atteint ({max_cycles}). Fin.")
            break

    logger.info("Scheduler arrêté avec succès.")


if __name__ == "__main__":
    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
    start_scheduler(interval_seconds=interval)
