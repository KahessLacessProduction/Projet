"""Small Language Model (SLM) & Financial NER Extractor (Niveau 3 - Option 3).
Extracts structured financial transactions from unstructured natural language text.
Converts extracted information into standard RawTransaction models and joins the common pipeline.
"""
from datetime import datetime
import hashlib
import logging
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from config import RAW_DATA_DIR, HIGH_AMOUNT_THRESHOLD, DB_URL
from data_pipeline.models import RawTransaction, ProcessedTransaction
from data_pipeline.processor import traiter_transactions
from data_pipeline.database import init_database, insert_processed_data_with_retry
from data_pipeline.loader import compute_content_hash

logger = logging.getLogger("data_pipeline.slm")

FRENCH_MONTHS = {
    "janvier": 1,
    "février": 2,
    "fevrier": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "août": 8,
    "aout": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "décembre": 12,
    "decembre": 12,
}

KNOWN_BANKS = {
    "bnp paribas": "BNP_PARIBAS",
    "bnp": "BNP_PARIBAS",
    "société générale": "SOCIETE_GENERALE",
    "societe generale": "SOCIETE_GENERALE",
    "sg": "SOCIETE_GENERALE",
    "crédit agricole": "CREDIT_AGRICOLE",
    "credit agricole": "CREDIT_AGRICOLE",
    "deutsche bank": "DEUTSCHE_BANK",
    "santander": "BANCO_SANTANDER",
    "banco santander": "BANCO_SANTANDER",
    "intesa sanpaolo": "INTESA_SANPAOLO",
    "abn amro": "ABN_AMRO",
}


class SLMExtractionError(Exception):
    """Raised when text cannot be parsed into a valid financial transaction."""
    pass


class SLMTransactionExtractor:
    """Natural Language & Financial NER extraction engine representing a Small Language Model.
    Parses unstructured narrative statements describing financial transfers.
    """

    def __init__(self, default_year: int = 2026):
        self.default_year = default_year

    def extract_datetime(self, text: str) -> datetime:
        """Extract transaction date and time from text."""
        # 1. Try ISO pattern: 2026-11-10T14:30:00 or 2026-11-10 14:30
        iso_match = re.search(r"(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}(?::\d{2})?)", text)
        if iso_match:
            val = iso_match.group(1).replace(" ", "T")
            if len(val.split("T")[1]) == 5:
                val += ":00"
            return datetime.fromisoformat(val)

        # 2. Try French narrative: "12 septembre [2026] à 14h30" or "12 septembre 14:30"
        months_pattern = "|".join(FRENCH_MONTHS.keys())
        date_regex = rf"(?:le\s+)?(\d{{1,2}})\s+({months_pattern})(?:\s+(\d{{4}}))?(?:\s+(?:à|vers)\s+(\d{{1,2}})[h:](\d{{2}}))?"
        m = re.search(date_regex, text, re.IGNORECASE)
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            month = FRENCH_MONTHS.get(month_name, 1)
            year = int(m.group(3)) if m.group(3) else self.default_year
            hour = int(m.group(4)) if m.group(4) else 12
            minute = int(m.group(5)) if m.group(5) else 0
            return datetime(year, month, day, hour, minute)

        # Default fallback: current timestamp
        return datetime.utcnow()

    def extract_ibans(self, text: str) -> Tuple[str, str]:
        """Extract origin and destination IBANs."""
        # Standard IBAN regex (2 letters + 2 digits + up to 30 alphanumeric characters)
        pattern = r"\b([A-Z]{2}\d{2}[A-Z0-9]{11,30})\b"
        matches = re.findall(pattern, text)
        if len(matches) < 2:
            # Look for slightly looser pattern if sender/receiver context is given
            relaxed = re.findall(r"\b([A-Z]{2}\d{10,32})\b", text)
            if len(relaxed) >= 2:
                return relaxed[0], relaxed[1]
            raise SLMExtractionError(
                f"Impossible d'extraire deux IBANs (émetteur et destinataire) dans le texte : '{text}'"
            )
        return matches[0], matches[1]

    def extract_montant(self, text: str) -> float:
        """Extract transaction amount and currency."""
        # Patterns like: 6200 €, 6200.50 EUR, 6 200 euros, 10,500.00 €
        pattern = r"(\d+(?:[\s.,]\d+)?)\s*(?:€|EUR|euros?|dollars?|\$)"
        m = re.search(pattern, text, re.IGNORECASE)
        if not m:
            # Fallback: look for keywords like "montant de 6200"
            m = re.search(r"montant(?:\s+de)?\s+(\d+(?:[\s.,]\d+)?)", text, re.IGNORECASE)
            if not m:
                raise SLMExtractionError(f"Montant introuvable dans le texte : '{text}'")

        raw_num = m.group(1).replace(" ", "").replace(",", ".")
        try:
            return float(raw_num)
        except ValueError as err:
            raise SLMExtractionError(f"Montant invalide '{raw_num}' : {err}")

    def extract_banque_source(self, text: str, default: str = "BANQUE_INCONNUE") -> str:
        """Extract bank name based on contextual keywords and known entities."""
        text_lower = text.lower()
        for bank_keyword, bank_code in KNOWN_BANKS.items():
            if bank_keyword in text_lower:
                return bank_code

        # Check contextual regex: "banque X" or "géré par X"
        m = re.search(r"(?:banque|chez|par la)\s+([A-Za-z0-9_-]+)", text, re.IGNORECASE)
        if m:
            return m.group(1).upper()

        return default

    def parse_text_to_transaction(self, text: str) -> RawTransaction:
        """Parse unstructured sentence into a validated RawTransaction."""
        iban_origine, iban_destinataire = self.extract_ibans(text)
        montant = self.extract_montant(text)
        dt = self.extract_datetime(text)
        banque = self.extract_banque_source(text)

        pays_source = iban_origine[:2]
        pays_destinataire = iban_destinataire[:2]

        return RawTransaction(
            datetime_transaction=dt,
            iban_origine=iban_origine,
            pays_source=pays_source,
            banque_source=banque,
            iban_destinataire=iban_destinataire,
            pays_destinataire=pays_destinataire,
            montant=montant,
        )

    def parse_document(self, text_content: str) -> List[RawTransaction]:
        """Parse multiple sentences or lines from a document into transaction records."""
        lines = [line.strip() for line in text_content.splitlines() if line.strip()]
        transactions: List[RawTransaction] = []

        for line in lines:
            try:
                tx = self.parse_text_to_transaction(line)
                transactions.append(tx)
            except SLMExtractionError as err:
                logger.warning(f"[SLM SKIP] Ligne non reconnue comme transaction : {err}")

        return transactions


def process_unstructured_text_into_pipeline(
    text_content: str,
    source_name: str = "text_source",
    db_url: str = DB_URL,
    seuil_montant: float = HIGH_AMOUNT_THRESHOLD,
) -> Dict[str, Any]:
    """Integration function: Ingest raw text, extract with SLM,
    pure process, and insert into the relational database.
    """
    extractor = SLMTransactionExtractor()
    transactions = extractor.parse_document(text_content)
    if not transactions:
        raise SLMExtractionError("Aucune transaction valide n'a pu être extraite du texte fourni.")

    content_hash = compute_content_hash(text_content)
    processed_txs, aggregats = traiter_transactions(
        transactions,
        seuil=seuil_montant,
        source_file_hash=content_hash,
        source_file_name=source_name,
    )

    engine = init_database(db_url=db_url)
    res = insert_processed_data_with_retry(
        engine=engine,
        transactions=processed_txs,
        file_hash=content_hash,
        filename=source_name,
    )

    engine.dispose()

    return {
        "source": source_name,
        "extracted_count": len(transactions),
        "db_status": res["status"],
        "aggregates": aggregats,
    }
