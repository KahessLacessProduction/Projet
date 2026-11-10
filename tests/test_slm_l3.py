"""Tests for Small Language Model (SLM) Extraction (Niveau 3 - Option 3)."""
from datetime import datetime
from data_pipeline.slm_extractor import (
    SLMTransactionExtractor,
    process_unstructured_text_into_pipeline,
)
from data_pipeline.models import RawTransaction


def test_slm_extracts_transaction_from_narrative_french_text():
    """Verify SLM extracts transaction attributes from natural language sentence."""
    text = (
        "Le 12 septembre 2026 à 14h30, le compte FR7630006000011234567890189 "
        "chez BNP Paribas a envoyé 6200 € vers le compte DE89370400440532013000 "
        "appartenant à une banque allemande."
    )
    extractor = SLMTransactionExtractor(default_year=2026)
    tx = extractor.parse_text_to_transaction(text)

    assert isinstance(tx, RawTransaction)
    assert tx.iban_origine == "FR7630006000011234567890189"
    assert tx.iban_destinataire == "DE89370400440532013000"
    assert tx.pays_source == "FR"
    assert tx.pays_destinataire == "DE"
    assert tx.montant == 6200.00
    assert tx.banque_source == "BNP_PARIBAS"
    assert tx.datetime_transaction == datetime(2026, 9, 12, 14, 30)


def test_slm_extracts_multiple_transactions_from_document():
    """Verify SLM extracts multiple transactions from multi-line text document."""
    doc = (
        "Le 12 septembre à 14h30, le compte FR7630006000011234567890189 chez BNP a envoyé 6200 € vers le compte DE89370400440532013000.\n"
        "Notification : Compte FR7610011000229876543210987 chez SG vers ES9121000418450200051332 pour un montant de 350.00 EUR le 2026-11-01T10:00:00.\n"
        "Ligne non financière sans IBAN qui doit être ignorée.\n"
    )
    extractor = SLMTransactionExtractor()
    txs = extractor.parse_document(doc)
    assert len(txs) == 2
    assert txs[0].montant == 6200.00
    assert txs[1].montant == 350.00


def test_slm_pipeline_integration(temp_dir):
    """Verify that transactions extracted via SLM feed into the common pipeline & database."""
    db_path = temp_dir / "slm_test.db"
    db_url = f"sqlite:///{db_path}"

    text_sample = (
        "Le 10 novembre 2026 à 10h00, le compte FR7630006000011234567890189 chez BNP Paribas "
        "a envoyé 7500 € vers le compte DE89370400440532013000."
    )

    result = process_unstructured_text_into_pipeline(
        text_content=text_sample,
        source_name="slm_memo_test",
        db_url=db_url,
        seuil_montant=5000.0,
    )

    assert result["extracted_count"] == 1
    assert result["db_status"] == "INSERTED"
    # Check that flag > 5000 and aggregates are populated
    assert "somme_par_iban_origine" in result["aggregates"]
    assert result["aggregates"]["somme_par_iban_origine"]["FR7630006000011234567890189"] == 7500.0
