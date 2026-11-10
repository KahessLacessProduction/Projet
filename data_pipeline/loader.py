"""Data loading module (Niveau 1.2).
Strictly separates data loading and I/O from pure processing logic.
Computes SHA-256 hash for deduplication and idempotency verification.
"""
import csv
import hashlib
from io import StringIO
from pathlib import Path
from typing import List, Tuple, Union
from pydantic import ValidationError

from data_pipeline.models import RawTransaction


class DataLoadingError(Exception):
    """Base exception for data loading failures."""
    pass


class InvalidCSVFormatError(DataLoadingError):
    """Raised when CSV structure or required columns are invalid."""
    pass


class TransactionValidationError(DataLoadingError):
    """Raised when a transaction record fails schema validation."""
    pass


def compute_content_hash(content: Union[str, bytes]) -> str:
    """Compute SHA-256 checksum of raw content to guarantee uniqueness & idempotency."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def compute_file_hash(file_path: Union[str, Path]) -> str:
    """Compute SHA-256 hash of a local file."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Fichier introuvable : {file_path}")
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def parse_csv_stream(
    stream: StringIO, source_name: str = "<stream>"
) -> List[RawTransaction]:
    """Parse CSV text stream into a list of validated RawTransaction objects."""
    reader = csv.DictReader(stream)
    required_columns = {
        "datetime_transaction",
        "iban_origine",
        "pays_source",
        "banque_source",
        "iban_destinataire",
        "pays_destinataire",
        "montant",
    }

    if not reader.fieldnames:
        raise InvalidCSVFormatError(f"Le fichier '{source_name}' est vide ou mal formaté.")

    missing_cols = required_columns - set(reader.fieldnames)
    if missing_cols:
        raise InvalidCSVFormatError(
            f"Colonnes obligatoires manquantes dans '{source_name}' : {sorted(list(missing_cols))}"
        )

    transactions: List[RawTransaction] = []
    for line_idx, row in enumerate(reader, start=2):  # line 1 is header
        try:
            # Parse row into Pydantic model
            tx = RawTransaction(
                datetime_transaction=row["datetime_transaction"],
                iban_origine=row["iban_origine"],
                pays_source=row["pays_source"],
                banque_source=row["banque_source"],
                iban_destinataire=row["iban_destinataire"],
                pays_destinataire=row["pays_destinataire"],
                montant=float(row["montant"]),
            )
            transactions.append(tx)
        except (ValueError, ValidationError) as err:
            raise TransactionValidationError(
                f"Erreur de validation à la ligne {line_idx} de '{source_name}': {err}"
            ) from err

    return transactions


def load_transactions_from_csv(file_path: Union[str, Path]) -> Tuple[List[RawTransaction], str]:
    """Load and validate transactions from a CSV file.
    
    Returns:
        Tuple of (List[RawTransaction], file_sha256_hash)
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Le fichier spécifié n'existe pas : {file_path}")

    file_hash = compute_file_hash(path)

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    transactions = parse_csv_stream(StringIO(content), source_name=path.name)
    return transactions, file_hash


def load_transactions_from_content(content: str, source_name: str = "content") -> Tuple[List[RawTransaction], str]:
    """Load and validate transactions directly from text/string content (e.g. from S3).
    
    Returns:
        Tuple of (List[RawTransaction], content_sha256_hash)
    """
    content_hash = compute_content_hash(content)
    transactions = parse_csv_stream(StringIO(content), source_name=source_name)
    return transactions, content_hash
