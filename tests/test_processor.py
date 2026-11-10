"""Tests for Pure Functional Processor (Niveau 1.2).
Guarantees immutability, determinism, and correctness of financial calculations.
"""
import copy
from data_pipeline.processor import (
    calculer_somme_par_iban_origine,
    calculer_somme_par_banque_source,
    calculer_somme_par_iban_destinataire,
    ajouter_flag_montant_superieur_5000,
    traiter_transactions,
)


def test_pure_functions_do_not_mutate_input(sample_transactions_raw):
    """Verify that processing functions do not modify their input data."""
    original_copy = copy.deepcopy(sample_transactions_raw)

    # Call all processing functions
    _ = calculer_somme_par_iban_origine(sample_transactions_raw)
    _ = calculer_somme_par_banque_source(sample_transactions_raw)
    _ = calculer_somme_par_iban_destinataire(sample_transactions_raw)
    _ = ajouter_flag_montant_superieur_5000(sample_transactions_raw)
    _ = traiter_transactions(sample_transactions_raw)

    # Input must be strictly identical
    assert sample_transactions_raw == original_copy


def test_calculer_somme_par_iban_origine(sample_transactions_raw):
    """Verify sum of amounts sent per iban_origine.
    sample data:
      FR7630006000011234567890189 -> 1500.00 + 6200.00 = 7700.00
      FR7610011000229876543210987 -> 450.50
    """
    totals = calculer_somme_par_iban_origine(sample_transactions_raw)
    assert totals["FR7630006000011234567890189"] == 7700.00
    assert totals["FR7610011000229876543210987"] == 450.50


def test_calculer_somme_par_banque_source(sample_transactions_raw):
    """Verify sum of amounts sent per banque_source.
    sample data:
      BNP_PARIBAS -> 1500.00 + 6200.00 = 7700.00
      SOCIETE_GENERALE -> 450.50
    """
    totals = calculer_somme_par_banque_source(sample_transactions_raw)
    assert totals["BNP_PARIBAS"] == 7700.00
    assert totals["SOCIETE_GENERALE"] == 450.50


def test_calculer_somme_par_iban_destinataire(sample_transactions_raw):
    """Verify sum of amounts received per iban_destinataire.
    sample data:
      DE89370400440532013000 -> 1500.00 + 450.50 = 1950.50
      ES9121000418450200051332 -> 6200.00
    """
    totals = calculer_somme_par_iban_destinataire(sample_transactions_raw)
    assert totals["DE89370400440532013000"] == 1950.50
    assert totals["ES9121000418450200051332"] == 6200.00


def test_ajouter_flag_montant_superieur_5000(sample_transactions_raw):
    """Verify flag is True if amount > 5000 and False otherwise.
    sample:
      tx 0: 1500.00 -> False
      tx 1: 6200.00 -> True
      tx 2: 450.50  -> False
    """
    flagged = ajouter_flag_montant_superieur_5000(sample_transactions_raw, seuil=5000.0)
    assert len(flagged) == 3
    assert flagged[0].flag_montant_superieur_5000 is False
    assert flagged[1].flag_montant_superieur_5000 is True
    assert flagged[2].flag_montant_superieur_5000 is False


def test_traiter_transactions_orchestrator(sample_transactions_raw):
    """Verify full pure transformation produces both flagged records and aggregates."""
    flagged, aggregats = traiter_transactions(sample_transactions_raw, seuil=5000.0)
    assert len(flagged) == 3
    assert "somme_par_iban_origine" in aggregats
    assert "somme_par_banque_source" in aggregats
    assert "somme_par_iban_destinataire" in aggregats
    assert aggregats["somme_par_iban_origine"]["FR7630006000011234567890189"] == 7700.00
