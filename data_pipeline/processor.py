"""Data processing module (Niveau 1.2).
Strictly adheres to functional programming principles:
- Pure functions: no side effects, no mutation of input arguments.
- Deterministic output: always returns identical results for identical inputs.
- No external state dependencies (no I/O, no DB, no globals).
"""
import copy
from typing import List, Dict, Union, Any, Tuple
from data_pipeline.models import RawTransaction, ProcessedTransaction


TransactionInput = Union[RawTransaction, Dict[str, Any]]


def _extract_field(item: TransactionInput, field_name: str) -> Any:
    """Helper to read attribute or dict key without mutating."""
    if isinstance(item, dict):
        return item[field_name]
    return getattr(item, field_name)


def _get_montant(item: TransactionInput) -> float:
    """Helper to extract transaction amount."""
    val = _extract_field(item, "montant")
    return float(val)


def calculer_somme_par_iban_origine(transactions: List[TransactionInput]) -> Dict[str, float]:
    """Pure function: calculate total amount sent by each iban_origine.

    Does NOT modify the input list or its elements.
    Returns: dict mapping iban_origine -> total_montant_envoye (rounded to 2 decimals).
    """
    totals: Dict[str, float] = {}
    for tx in transactions:
        iban = str(_extract_field(tx, "iban_origine"))
        montant = _get_montant(tx)
        totals[iban] = round(totals.get(iban, 0.0) + montant, 2)
    return totals


def calculer_somme_par_banque_source(transactions: List[TransactionInput]) -> Dict[str, float]:
    """Pure function: calculate total amount sent by each banque_source.

    Does NOT modify the input list or its elements.
    Returns: dict mapping banque_source -> total_montant_envoye (rounded to 2 decimals).
    """
    totals: Dict[str, float] = {}
    for tx in transactions:
        banque = str(_extract_field(tx, "banque_source"))
        montant = _get_montant(tx)
        totals[banque] = round(totals.get(banque, 0.0) + montant, 2)
    return totals


def calculer_somme_par_iban_destinataire(transactions: List[TransactionInput]) -> Dict[str, float]:
    """Pure function: calculate total amount received by each iban_destinataire.

    Does NOT modify the input list or its elements.
    Returns: dict mapping iban_destinataire -> total_montant_recu (rounded to 2 decimals).
    """
    totals: Dict[str, float] = {}
    for tx in transactions:
        iban = str(_extract_field(tx, "iban_destinataire"))
        montant = _get_montant(tx)
        totals[iban] = round(totals.get(iban, 0.0) + montant, 2)
    return totals


def ajouter_flag_montant_superieur_5000(
    transactions: List[TransactionInput],
    seuil: float = 5000.0,
    source_file_hash: str = None,
    source_file_name: str = None,
) -> List[ProcessedTransaction]:
    """Pure function: add a flag when transaction amount exceeds seuil (default 5000.0).

    Does NOT mutate input objects; returns a new list of ProcessedTransaction models.
    """
    processed: List[ProcessedTransaction] = []
    for tx in transactions:
        # Create pure independent copy or extract dict
        if isinstance(tx, RawTransaction):
            tx_dict = tx.to_dict()
        else:
            tx_dict = copy.deepcopy(tx)

        montant = float(tx_dict["montant"])
        flag = montant > seuil

        processed_tx = ProcessedTransaction(
            datetime_transaction=tx_dict["datetime_transaction"],
            iban_origine=tx_dict["iban_origine"],
            pays_source=tx_dict["pays_source"],
            banque_source=tx_dict["banque_source"],
            iban_destinataire=tx_dict["iban_destinataire"],
            pays_destinataire=tx_dict["pays_destinataire"],
            montant=round(montant, 2),
            flag_montant_superieur_5000=flag,
            source_file_hash=source_file_hash,
            source_file_name=source_file_name,
        )
        processed.append(processed_tx)

    return processed


def traiter_transactions(
    transactions: List[TransactionInput],
    seuil: float = 5000.0,
    source_file_hash: str = None,
    source_file_name: str = None,
) -> Tuple[List[ProcessedTransaction], Dict[str, Dict[str, float]]]:
    """Pure function orchestrating all required transformations without any side effects.

    Returns:
        Tuple of:
          - List[ProcessedTransaction] with flag_montant_superieur_5000 applied.
          - Aggregates dictionary containing:
              * 'somme_par_iban_origine'
              * 'somme_par_banque_source'
              * 'somme_par_iban_destinataire'
    """
    flagged_txs = ajouter_flag_montant_superieur_5000(
        transactions,
        seuil=seuil,
        source_file_hash=source_file_hash,
        source_file_name=source_file_name,
    )

    aggregats = {
        "somme_par_iban_origine": calculer_somme_par_iban_origine(transactions),
        "somme_par_banque_source": calculer_somme_par_banque_source(transactions),
        "somme_par_iban_destinataire": calculer_somme_par_iban_destinataire(transactions),
    }

    return flagged_txs, aggregats
