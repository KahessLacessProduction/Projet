"""Local Data Pipeline Orchestrator (Niveau 1.4 & 1.7).
Automates the full flow:
1. Generates CSV files if required.
2. Initializes the relational database.
3. Loads data from CSV files.
4. Executes pure business treatments (aggregates, flags > 5000).
5. Inserts processed data into the database with retry and deduplication.
"""
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import click

from config import RAW_DATA_DIR, DB_URL, HIGH_AMOUNT_THRESHOLD
from data_pipeline.generator import generate_valid_csv_files
from data_pipeline.loader import load_transactions_from_csv, DataLoadingError
from data_pipeline.processor import traiter_transactions
from data_pipeline.database import init_database, insert_processed_data_with_retry, get_engine
from data_pipeline.retry import MaxRetriesExceededError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("data_pipeline.pipeline")


class PipelineExecutionError(Exception):
    """Raised when the pipeline fails on one or more files."""
    pass


def run_pipeline(
    data_dir: Path = RAW_DATA_DIR,
    db_url: str = DB_URL,
    generate_data: bool = False,
    seuil_montant: float = HIGH_AMOUNT_THRESHOLD,
    simulate_transient_db_failure: int = 0,
    simulate_fatal_db_failure: bool = False,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """Execute the end-to-end Local Data Pipeline.

    Returns:
        Summary dict of pipeline execution statistics.
    """
    logger.info("=" * 60)
    logger.info(" DÉMARRAGE DE LA PIPELINE DATA LOCALE (Niveau 1)")
    logger.info("=" * 60)

    # Étape 1 : Génération des données CSV (si demandée ou si dossier vide)
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_files = sorted(list(data_dir.glob("*.csv")))

    if generate_data or not csv_files:
        logger.info(f"[ÉTAPE 1/4] Génération des fichiers CSV dans {data_dir}...")
        generated = generate_valid_csv_files(output_dir=data_dir)
        csv_files = sorted(list(data_dir.glob("*.csv")))
        logger.info(f"-> {len(generated)} fichiers CSV générés avec succès.")
    else:
        logger.info(f"[ÉTAPE 1/4] {len(csv_files)} fichier(s) CSV existant(s) trouvé(s).")

    # Étape 2 : Initialisation de la base de données relationnelle
    logger.info(f"[ÉTAPE 2/4] Initialisation de la base de données : {db_url}")
    engine = init_database(db_url=db_url)

    # Configuration de la simulation de panne pour le retry si activée
    transient_failure_counter = {"count": 0}

    def transient_failure_sim():
        if simulate_fatal_db_failure:
            raise RuntimeError("Simulation d'une panne DB fatale (erreur non résolue).")
        if transient_failure_counter["count"] < simulate_transient_db_failure:
            transient_failure_counter["count"] += 1
            raise RuntimeError(
                f"Simulation d'une panne DB temporaire "
                f"({transient_failure_counter['count']}/{simulate_transient_db_failure})"
            )

    failure_hook = transient_failure_sim if (simulate_transient_db_failure > 0 or simulate_fatal_db_failure) else None

    # Étape 3 & 4 : Chargement, traitement pur et insertion
    logger.info("[ÉTAPE 3 & 4/4] Chargement, traitement et insertion des transactions...")
    summary: Dict[str, Any] = {
        "total_files": len(csv_files),
        "processed_files": 0,
        "skipped_files": 0,
        "failed_files": 0,
        "total_transactions_inserted": 0,
        "errors": [],
    }

    for file_path in csv_files:
        filename = file_path.name
        logger.info(f"--- Traitement du fichier : {filename} ---")
        try:
            # 1. Chargement & validation
            transactions, file_hash = load_transactions_from_csv(file_path)
            logger.info(
                f"  Chargé: {len(transactions)} transactions (SHA256: {file_hash[:10]}...)"
            )

            # 2. Traitement pur (sans effet de bord)
            processed_txs, aggregats = traiter_transactions(
                transactions,
                seuil=seuil_montant,
                source_file_hash=file_hash,
                source_file_name=filename,
            )
            nb_high_amount = sum(1 for t in processed_txs if t.flag_montant_superieur_5000)
            logger.info(
                f"  Traitement pur terminé : {nb_high_amount} transactions avec montant > {seuil_montant} €"
            )

            # 3. Insertion avec retry et déduplication
            result = insert_processed_data_with_retry(
                engine=engine,
                transactions=processed_txs,
                file_hash=file_hash,
                filename=filename,
                max_retries=max_retries,
                initial_delay=0.2,
                transient_failure_sim=failure_hook,
            )

            if result["status"] == "INSERTED":
                summary["processed_files"] += 1
                summary["total_transactions_inserted"] += result["inserted_count"]
            elif result["status"] == "SKIPPED":
                summary["skipped_files"] += 1

        except (DataLoadingError, MaxRetriesExceededError, Exception) as err:
            logger.error(f"[ERREUR SUR {filename}] {type(err).__name__} : {err}")
            summary["failed_files"] += 1
            summary["errors"].append({"file": filename, "error": str(err)})

    logger.info("=" * 60)
    logger.info(" RÉSUMÉ DE L'EXÉCUTION DE LA PIPELINE")
    logger.info(f" Fichiers traités avec succès : {summary['processed_files']}")
    logger.info(f" Fichiers ignorés (doublons)   : {summary['skipped_files']}")
    logger.info(f" Fichiers en échec             : {summary['failed_files']}")
    logger.info(f" Transactions insérées en DB   : {summary['total_transactions_inserted']}")
    logger.info("=" * 60)

    engine.dispose()

    if summary["failed_files"] > 0:
        raise PipelineExecutionError(
            f"{summary['failed_files']} fichier(s) ont échoué lors de l'exécution de la pipeline."
        )

    return summary


@click.command()
@click.option(
    "--generate",
    is_flag=True,
    help="Générer de nouveaux fichiers CSV de transactions avant l'exécution.",
)
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path),
    default=RAW_DATA_DIR,
    help="Répertoire source des fichiers CSV.",
)
@click.option(
    "--db-url",
    default=DB_URL,
    help="URL SQLAlchemy de la base de données relationnelle.",
)
@click.option(
    "--simulate-transient-db-failure",
    type=int,
    default=0,
    help="Nombre de pannes temporaires simulées pour tester le retry automatique.",
)
@click.option(
    "--simulate-fatal-db-failure",
    is_flag=True,
    help="Simuler une panne permanente pour tester l'arrêt propre de la pipeline.",
)
def cli(
    generate: bool,
    data_dir: Path,
    db_url: str,
    simulate_transient_db_failure: int,
    simulate_fatal_db_failure: bool,
):
    """Point d'entrée CLI pour exécuter la pipeline data."""
    try:
        run_pipeline(
            data_dir=data_dir,
            db_url=db_url,
            generate_data=generate,
            simulate_transient_db_failure=simulate_transient_db_failure,
            simulate_fatal_db_failure=simulate_fatal_db_failure,
        )
        sys.exit(0)
    except PipelineExecutionError as exc:
        click.secho(f"\n[PIPELINE ÉCHOUÉE] {exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:
        click.secho(f"\n[ERREUR CRITIQUE] {exc}", fg="red", err=True)
        sys.exit(2)


if __name__ == "__main__":
    cli()
