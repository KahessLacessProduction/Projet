"""Simulation and verification of Pipeline Failure and Recovery (Niveau 1.6).
Demonstrates:
1. Cause of problem (corrupted CSV file with invalid transaction schema).
2. Failure of the pipeline with explicit error message.
3. Applied correction.
4. Success of the pipeline after correction.
"""
import pytest
from pathlib import Path
from data_pipeline.generator import (
    generate_valid_csv_files,
    generate_corrupted_csv_file,
    write_csv_file,
    DETERMINISTIC_BATCH_1,
)
from data_pipeline.pipeline import run_pipeline, PipelineExecutionError


def test_pipeline_failure_scenario_and_resolution(temp_dir):
    """Scenario 1.6:
    Phase 1: Ingesting an invalid CSV causes pipeline failure with structured error.
    Phase 2: Correcting the file resolves the issue and pipeline succeeds.
    """
    db_path = temp_dir / "failure_sim.db"
    db_url = f"sqlite:///{db_path}"

    # --- PHASE 1 : GÉNÉRATION DU FICHIER CORROMPU & ÉCHEC ---
    # On génère un fichier contenant un montant négatif et du texte invalide
    corrupted_file = generate_corrupted_csv_file(
        output_dir=temp_dir,
        filename="transactions_corrupted.csv",
        corruption_type="invalid_montant",
    )
    assert corrupted_file.is_file()

    # La pipeline doit échouer et lever PipelineExecutionError
    with pytest.raises(PipelineExecutionError) as exc_info:
        run_pipeline(
            data_dir=temp_dir,
            db_url=db_url,
            generate_data=False,
        )

    # Vérification du message d'erreur explicite
    error_message = str(exc_info.value)
    assert "1 fichier(s) ont échoué" in error_message

    # --- PHASE 2 : CORRECTION APPORTÉE & RÉUSSITE ---
    # On corrige le fichier en remplaçant son contenu par des données valides
    write_csv_file(corrupted_file, DETERMINISTIC_BATCH_1)

    # Ré-exécution de la pipeline : doit réussir sans aucune erreur !
    summary = run_pipeline(
        data_dir=temp_dir,
        db_url=db_url,
        generate_data=False,
    )

    assert summary["failed_files"] == 0
    assert summary["processed_files"] == 1
    assert summary["total_transactions_inserted"] == len(DETERMINISTIC_BATCH_1)
