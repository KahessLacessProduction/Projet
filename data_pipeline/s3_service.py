"""S3 Storage Service compatible with MinIO / Garage / SeaweedFS / AWS S3 (Niveau 2).
Handles bucket creation, upload, listing, and fetching with automated retry.
Guarantees idempotency and prevents double-processing of files retrieved from S3.
"""
import logging
from io import BytesIO, StringIO
from pathlib import Path
from typing import List, Dict, Any, Optional
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectTimeoutError

from config import (
    S3_ENDPOINT_URL,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET_NAME,
    S3_REGION,
    RETRY_MAX_ATTEMPTS,
    RETRY_INITIAL_DELAY,
    RETRY_BACKOFF_FACTOR,
    DB_URL,
    RAW_DATA_DIR,
    HIGH_AMOUNT_THRESHOLD,
)
from data_pipeline.generator import generate_valid_csv_files
from data_pipeline.loader import load_transactions_from_content
from data_pipeline.processor import traiter_transactions
from data_pipeline.database import init_database, insert_processed_data_with_retry
from data_pipeline.retry import with_retry, MaxRetriesExceededError

logger = logging.getLogger("data_pipeline.s3")


class S3ServiceError(Exception):
    """Base exception for S3 service interactions."""
    pass


def get_s3_client(
    endpoint_url: str = S3_ENDPOINT_URL,
    access_key: str = S3_ACCESS_KEY,
    secret_key: str = S3_SECRET_KEY,
    region: str = S3_REGION,
):
    """Instantiate a boto3 S3 client configured for local S3 / MinIO compatibility."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket_exists(s3_client, bucket_name: str = S3_BUCKET_NAME) -> None:
    """Check if the bucket exists; if not, create it."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"[S3] Le bucket '{bucket_name}' existe déjà.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchBucket"):
            logger.info(f"[S3] Création du bucket '{bucket_name}'...")
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"[S3] Bucket '{bucket_name}' créé avec succès.")
        else:
            logger.error(f"[S3] Impossible de vérifier l'existence du bucket : {e}")
            raise S3ServiceError(f"Erreur vérification bucket : {e}") from e


def upload_file_to_s3(
    s3_client,
    local_path: Path,
    s3_key: str,
    bucket_name: str = S3_BUCKET_NAME,
) -> str:
    """Upload a local file to S3 bucket."""
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"Fichier local introuvable : {local_path}")
    logger.info(f"[S3 UPLOAD] Envoi de '{path.name}' vers s3://{bucket_name}/{s3_key}...")
    s3_client.upload_file(str(path), bucket_name, s3_key)
    return f"s3://{bucket_name}/{s3_key}"


def list_s3_files(s3_client, bucket_name: str = S3_BUCKET_NAME, prefix: str = "") -> List[str]:
    """List all object keys present in the specified bucket."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        contents = response.get("Contents", [])
        return [item["Key"] for item in contents]
    except Exception as err:
        logger.error(f"[S3 LIST ERROR] Impossible de lister les objets : {err}")
        raise S3ServiceError(f"Erreur listage S3 : {err}") from err


def get_s3_file_content_with_retry(
    s3_client,
    s3_key: str,
    bucket_name: str = S3_BUCKET_NAME,
    max_retries: int = RETRY_MAX_ATTEMPTS,
    initial_delay: float = RETRY_INITIAL_DELAY,
    backoff_factor: float = RETRY_BACKOFF_FACTOR,
    transient_failure_hook: Optional[Any] = None,
) -> str:
    """Download and return CSV content as string from S3 with automatic retry.

    Niveau 2.4 : Détecte l'échec, attend avec exponential backoff,
    réessaie jusqu'au maximum, et arrête proprement avec log clair.
    """

    @with_retry(
        max_attempts=max_retries,
        initial_delay=initial_delay,
        backoff_factor=backoff_factor,
        retryable_exceptions=(
            ClientError,
            EndpointConnectionError,
            ConnectTimeoutError,
            RuntimeError,
            ConnectionError,
        ),
        operation_name=f"Récupération S3 (s3://{bucket_name}/{s3_key})",
    )
    def _fetch_from_s3() -> str:
        if transient_failure_hook:
            transient_failure_hook()

        logger.info(f"[S3 FETCH] Téléchargement de 's3://{bucket_name}/{s3_key}'...")
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        raw_bytes = response["Body"].read()
        return raw_bytes.decode("utf-8")

    return _fetch_from_s3()


def run_s3_pipeline(
    s3_client=None,
    endpoint_url: str = S3_ENDPOINT_URL,
    access_key: str = S3_ACCESS_KEY,
    secret_key: str = S3_SECRET_KEY,
    bucket_name: str = S3_BUCKET_NAME,
    db_url: str = DB_URL,
    generate_local_first: bool = True,
    local_data_dir: Path = RAW_DATA_DIR,
    seuil_montant: float = HIGH_AMOUNT_THRESHOLD,
    simulate_s3_failure_count: int = 0,
    simulate_s3_fatal_failure: bool = False,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """Execute the Level 2 S3-based Data Pipeline:
    1. Connect to S3 service.
    2. Ensure bucket exists.
    3. Generate and upload CSV files to S3 bucket.
    4. Verify file presence in S3.
    5. Fetch from S3 with retry mechanism.
    6. Load, pure process, and insert into DB with deduplication.
    """
    logger.info("=" * 60)
    logger.info(" DÉMARRAGE DE LA PIPELINE DATA S3 (Niveau 2)")
    logger.info("=" * 60)

    # 1. Connexion S3
    if s3_client is None:
        logger.info(f"[S3 ÉTAPE 1] Connexion au service S3 sur {endpoint_url}...")
        s3_client = get_s3_client(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
        )

    # 2. Vérification / Création du bucket
    logger.info(f"[S3 ÉTAPE 2] Configuration du bucket '{bucket_name}'...")
    ensure_bucket_exists(s3_client, bucket_name=bucket_name)

    # 3. Génération & Upload automatique vers S3
    if generate_local_first:
        logger.info(f"[S3 ÉTAPE 3] Génération et upload des CSV vers S3...")
        local_files = generate_valid_csv_files(output_dir=local_data_dir)
        for lf in local_files:
            upload_file_to_s3(s3_client, lf, s3_key=lf.name, bucket_name=bucket_name)

    # 4. Vérification de la présence des fichiers dans S3
    logger.info("[S3 ÉTAPE 4] Vérification de la présence des fichiers dans le bucket...")
    s3_keys = [k for k in list_s3_files(s3_client, bucket_name=bucket_name) if k.endswith(".csv")]
    logger.info(f"-> {len(s3_keys)} fichier(s) CSV trouvés dans s3://{bucket_name}/ : {s3_keys}")

    # 5. Base de données
    engine = init_database(db_url=db_url)

    # Setup simulation de panne S3
    s3_failure_counter = {"count": 0}

    def transient_s3_hook():
        if simulate_s3_fatal_failure:
            raise RuntimeError("Simulation d'une panne S3 fatale permanente.")
        if s3_failure_counter["count"] < simulate_s3_failure_count:
            s3_failure_counter["count"] += 1
            raise RuntimeError(
                f"Simulation indisponibilité temporaire S3 "
                f"({s3_failure_counter['count']}/{simulate_s3_failure_count})"
            )

    failure_hook = transient_s3_hook if (simulate_s3_failure_count > 0 or simulate_s3_fatal_failure) else None

    # 6. Lecture depuis S3 -> Traitement -> Base de données
    summary: Dict[str, Any] = {
        "s3_bucket": bucket_name,
        "total_files_in_s3": len(s3_keys),
        "processed_files": 0,
        "skipped_files": 0,
        "failed_files": 0,
        "total_transactions_inserted": 0,
        "errors": [],
    }

    for s3_key in s3_keys:
        logger.info(f"--- Récupération depuis S3 : {s3_key} ---")
        try:
            # Récupération avec retry
            content = get_s3_file_content_with_retry(
                s3_client=s3_client,
                s3_key=s3_key,
                bucket_name=bucket_name,
                max_retries=max_retries,
                initial_delay=0.2,
                transient_failure_hook=failure_hook,
            )

            # Chargement depuis le contenu en mémoire
            transactions, file_hash = load_transactions_from_content(
                content=content, source_name=s3_key
            )

            # Traitement pur
            processed_txs, _ = traiter_transactions(
                transactions,
                seuil=seuil_montant,
                source_file_hash=file_hash,
                source_file_name=s3_key,
            )

            # Insertion en base avec déduplication
            res = insert_processed_data_with_retry(
                engine=engine,
                transactions=processed_txs,
                file_hash=file_hash,
                filename=s3_key,
                max_retries=max_retries,
                initial_delay=0.2,
            )

            if res["status"] == "INSERTED":
                summary["processed_files"] += 1
                summary["total_transactions_inserted"] += res["inserted_count"]
            elif res["status"] == "SKIPPED":
                summary["skipped_files"] += 1

        except (MaxRetriesExceededError, Exception) as err:
            logger.error(f"[S3 ERREUR SUR {s3_key}] {type(err).__name__} : {err}")
            summary["failed_files"] += 1
            summary["errors"].append({"key": s3_key, "error": str(err)})

    logger.info("=" * 60)
    logger.info(" RÉSUMÉ DE LA PIPELINE S3 (Niveau 2)")
    logger.info(f" Fichiers S3 traités         : {summary['processed_files']}")
    logger.info(f" Fichiers S3 ignorés (doublon): {summary['skipped_files']}")
    logger.info(f" Fichiers S3 en échec        : {summary['failed_files']}")
    logger.info(f" Transactions insérées en DB : {summary['total_transactions_inserted']}")
    logger.info("=" * 60)

    engine.dispose()
    return summary
