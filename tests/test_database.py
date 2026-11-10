"""Tests for Relational Database, Idempotency, and Retry (Niveau 1.3 & 1.7)."""
import pytest
from sqlalchemy.orm import Session
from sqlalchemy import select

from data_pipeline.models import ProcessedTransaction
from data_pipeline.database import (
    insert_processed_data_with_retry,
    is_file_already_processed,
    ProcessedFileRecord,
    TransactionRecord,
    AggregateIbanOrigine,
    AggregateBanqueSource,
    AggregateIbanDestinataire,
)
from data_pipeline.retry import MaxRetriesExceededError


@pytest.fixture
def sample_processed_transactions():
    """Create sample ProcessedTransaction objects."""
    return [
        ProcessedTransaction(
            datetime_transaction="2026-11-01T10:00:00",
            iban_origine="FR7630006000011234567890189",
            pays_source="FR",
            banque_source="BNP_PARIBAS",
            iban_destinataire="DE89370400440532013000",
            pays_destinataire="DE",
            montant=1500.00,
            flag_montant_superieur_5000=False,
        ),
        ProcessedTransaction(
            datetime_transaction="2026-11-01T11:00:00",
            iban_origine="FR7630006000011234567890189",
            pays_source="FR",
            banque_source="BNP_PARIBAS",
            iban_destinataire="ES9121000418450200051332",
            pays_destinataire="ES",
            montant=6200.00,
            flag_montant_superieur_5000=True,
        ),
    ]


def test_insert_processed_data_populates_tables(sqlite_engine, sample_processed_transactions):
    """Verify that transactions and aggregates are inserted properly into the database."""
    file_hash = "a" * 64
    filename = "batch_test.csv"

    res = insert_processed_data_with_retry(
        engine=sqlite_engine,
        transactions=sample_processed_transactions,
        file_hash=file_hash,
        filename=filename,
    )
    assert res["status"] == "INSERTED"
    assert res["inserted_count"] == 2

    with Session(sqlite_engine) as session:
        # Check ProcessedFileRecord
        file_rec = session.execute(
            select(ProcessedFileRecord).where(ProcessedFileRecord.file_hash == file_hash)
        ).scalar_one()
        assert file_rec.record_count == 2
        assert file_rec.status == "SUCCESS"

        # Check Transactions
        txs = session.execute(select(TransactionRecord)).scalars().all()
        assert len(txs) == 2
        assert any(t.flag_montant_superieur_5000 for t in txs)

        # Check Aggregates
        bnp_agg = session.get(AggregateBanqueSource, "BNP_PARIBAS")
        assert bnp_agg is not None
        assert bnp_agg.total_envoye == 7700.00
        assert bnp_agg.nb_transactions == 2


def test_idempotency_prevents_duplicate_processing(sqlite_engine, sample_processed_transactions):
    """Verify that re-inserting the same file hash is skipped to prevent duplicate records."""
    file_hash = "b" * 64
    filename = "batch_idempotence.csv"

    # First insertion
    res1 = insert_processed_data_with_retry(
        engine=sqlite_engine,
        transactions=sample_processed_transactions,
        file_hash=file_hash,
        filename=filename,
    )
    assert res1["status"] == "INSERTED"

    # Second insertion of same file (duplicate)
    res2 = insert_processed_data_with_retry(
        engine=sqlite_engine,
        transactions=sample_processed_transactions,
        file_hash=file_hash,
        filename=filename,
    )
    assert res2["status"] == "SKIPPED"
    assert res2["inserted_count"] == 0

    # Ensure transaction table still has only 2 rows, not 4
    with Session(sqlite_engine) as session:
        count = session.execute(select(TransactionRecord)).scalars().all()
        assert len(count) == 2


def test_retry_on_transient_failure_succeeds(sqlite_engine, sample_processed_transactions):
    """Verify retry handles transient failures and succeeds when failure recovers."""
    attempts = {"count": 0}

    def transient_hook():
        attempts["count"] += 1
        if attempts["count"] <= 2:  # fail first 2 attempts
            raise RuntimeError(f"Panne DB temporaire (essai {attempts['count']})")

    file_hash = "c" * 64
    filename = "batch_transient.csv"

    res = insert_processed_data_with_retry(
        engine=sqlite_engine,
        transactions=sample_processed_transactions,
        file_hash=file_hash,
        filename=filename,
        max_retries=3,
        initial_delay=0.01,
        transient_failure_sim=transient_hook,
    )
    assert res["status"] == "INSERTED"
    assert attempts["count"] == 3  # Succeeded on the 3rd attempt


def test_retry_exceeding_max_attempts_raises_error(sqlite_engine, sample_processed_transactions):
    """Verify exceeding max retry attempts raises MaxRetriesExceededError cleanly."""
    def fatal_hook():
        raise RuntimeError("Panne DB permanente!")

    file_hash = "d" * 64
    filename = "batch_fatal.csv"

    with pytest.raises(MaxRetriesExceededError) as exc_info:
        insert_processed_data_with_retry(
            engine=sqlite_engine,
            transactions=sample_processed_transactions,
            file_hash=file_hash,
            filename=filename,
            max_retries=3,
            initial_delay=0.01,
            transient_failure_sim=fatal_hook,
        )

    assert "a échoué après 3 tentatives" in str(exc_info.value)
