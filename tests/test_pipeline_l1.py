"""Integration Tests for Level 1 Local Data Pipeline (Niveau 1.4 & 1.5)."""
from pathlib import Path
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from data_pipeline.pipeline import run_pipeline, PipelineExecutionError
from data_pipeline.database import (
    TransactionRecord,
    ProcessedFileRecord,
    AggregateBanqueSource,
    AggregateIbanOrigine,
)


def test_pipeline_level1_end_to_end(temp_dir):
    """Execute complete Level 1 pipeline: generation -> loading -> pure processing -> DB insertion.
    Verifies data presence, flags, aggregates, and idempotency.
    """
    db_path = temp_dir / "pipeline_test.db"
    db_url = f"sqlite:///{db_path}"

    # 1. First execution with data generation
    summary = run_pipeline(
        data_dir=temp_dir,
        db_url=db_url,
        generate_data=True,
    )

    assert summary["processed_files"] == 3
    assert summary["skipped_files"] == 0
    assert summary["failed_files"] == 0
    assert summary["total_transactions_inserted"] == 21

    # Verify database contents
    engine = create_engine(db_url)
    with Session(engine) as session:
        # Verify 3 files recorded
        files = session.execute(select(ProcessedFileRecord)).scalars().all()
        assert len(files) == 3

        # Verify 21 transactions
        txs = session.execute(select(TransactionRecord)).scalars().all()
        assert len(txs) == 21

        # Verify flags: transactions > 5000 must have flag_montant_superieur_5000 = True
        high_amount_txs = [t for t in txs if t.montant > 5000]
        assert len(high_amount_txs) > 0
        for tx in high_amount_txs:
            assert tx.flag_montant_superieur_5000 is True

        normal_txs = [t for t in txs if t.montant <= 5000]
        for tx in normal_txs:
            assert tx.flag_montant_superieur_5000 is False

        # Verify aggregates exist and are non-zero
        banque_aggs = session.execute(select(AggregateBanqueSource)).scalars().all()
        assert len(banque_aggs) >= 3
        bnp = session.get(AggregateBanqueSource, "BNP_PARIBAS")
        assert bnp is not None
        assert bnp.total_envoye > 0

    # 2. Second execution (Idempotency test)
    # Rerunning should find existing files and skip all of them
    summary_rerun = run_pipeline(
        data_dir=temp_dir,
        db_url=db_url,
        generate_data=False,
    )
    assert summary_rerun["processed_files"] == 0
    assert summary_rerun["skipped_files"] == 3
    assert summary_rerun["total_transactions_inserted"] == 0

    # Total in DB must remain strictly 21
    with Session(engine) as session:
        txs_after = session.execute(select(TransactionRecord)).scalars().all()
        assert len(txs_after) == 21


def test_pipeline_level1_with_retry(temp_dir):
    """Verify pipeline successfully finishes even when database encounters transient failures."""
    db_path = temp_dir / "retry_pipeline_test.db"
    db_url = f"sqlite:///{db_path}"

    summary = run_pipeline(
        data_dir=temp_dir,
        db_url=db_url,
        generate_data=True,
        simulate_transient_db_failure=2,
    )
    assert summary["processed_files"] == 3
    assert summary["failed_files"] == 0


def test_pipeline_level1_fatal_failure_stops_cleanly(temp_dir):
    """Verify pipeline stops cleanly and reports failure when max retries are exceeded."""
    db_path = temp_dir / "fatal_pipeline_test.db"
    db_url = f"sqlite:///{db_path}"

    with pytest.raises(PipelineExecutionError):
        run_pipeline(
            data_dir=temp_dir,
            db_url=db_url,
            generate_data=True,
            simulate_fatal_db_failure=True,
        )
