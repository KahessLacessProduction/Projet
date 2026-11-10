"""Tests for Data Loader (Niveau 1.2)."""
from pathlib import Path
from io import StringIO
import pytest

from data_pipeline.loader import (
    load_transactions_from_csv,
    load_transactions_from_content,
    parse_csv_stream,
    compute_content_hash,
    compute_file_hash,
    InvalidCSVFormatError,
    TransactionValidationError,
)


def test_load_valid_csv(temp_dir):
    """Verify loading of a valid CSV file."""
    csv_file = temp_dir / "valid.csv"
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write(
            "datetime_transaction,iban_origine,pays_source,banque_source,iban_destinataire,pays_destinataire,montant\n"
            "2026-11-01T10:00:00,FR7630006000011234567890189,FR,BNP_PARIBAS,DE89370400440532013000,DE,1500.50\n"
        )

    txs, file_hash = load_transactions_from_csv(csv_file)
    assert len(txs) == 1
    assert txs[0].iban_origine == "FR7630006000011234567890189"
    assert txs[0].montant == 1500.50
    assert len(file_hash) == 64  # SHA-256


def test_load_missing_columns_raises_error():
    """Verify loading fails cleanly when required columns are absent."""
    bad_csv = StringIO("datetime_transaction,montant\n2026-11-01T10:00:00,100\n")
    with pytest.raises(InvalidCSVFormatError) as exc:
        parse_csv_stream(bad_csv, source_name="bad.csv")
    assert "Colonnes obligatoires manquantes" in str(exc.value)


def test_load_invalid_transaction_values_raises_error():
    """Verify loading fails cleanly on negative amounts or bad formats."""
    bad_amount = StringIO(
        "datetime_transaction,iban_origine,pays_source,banque_source,iban_destinataire,pays_destinataire,montant\n"
        "2026-11-01T10:00:00,FR7630006000011234567890189,FR,BNP_PARIBAS,DE89370400440532013000,DE,-50.0\n"
    )
    with pytest.raises(TransactionValidationError):
        parse_csv_stream(bad_amount, source_name="negative_amount.csv")


def test_compute_content_hash_deterministic():
    """Verify hash computation is deterministic and reproducible."""
    content = "test,data,123"
    h1 = compute_content_hash(content)
    h2 = compute_content_hash(content)
    assert h1 == h2
    assert len(h1) == 64
