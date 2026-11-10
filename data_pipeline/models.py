"""Data models for financial transactions.
Defines schemas for raw transactions, validated records, and aggregated results.
"""
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class RawTransaction(BaseModel):
    """Schema representing an incoming raw financial transaction."""

    datetime_transaction: datetime
    iban_origine: str = Field(..., min_length=15, max_length=34)
    pays_source: str = Field(..., min_length=2, max_length=2)
    banque_source: str = Field(..., min_length=2)
    iban_destinataire: str = Field(..., min_length=15, max_length=34)
    pays_destinataire: str = Field(..., min_length=2, max_length=2)
    montant: float = Field(..., gt=0)

    @field_validator("pays_source", "pays_destinataire")
    @classmethod
    def uppercase_country_code(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("iban_origine", "iban_destinataire")
    @classmethod
    def clean_iban(cls, v: str) -> str:
        cleaned = "".join(v.split()).upper()
        if not cleaned.isalnum():
            raise ValueError(f"IBAN contains invalid characters: {v}")
        return cleaned

    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary with ISO formatted datetime."""
        return {
            "datetime_transaction": self.datetime_transaction.isoformat(),
            "iban_origine": self.iban_origine,
            "pays_source": self.pays_source,
            "banque_source": self.banque_source,
            "iban_destinataire": self.iban_destinataire,
            "pays_destinataire": self.pays_destinataire,
            "montant": round(self.montant, 2),
        }


class ProcessedTransaction(RawTransaction):
    """Schema representing a processed transaction with applied business flags."""

    flag_montant_superieur_5000: bool = False
    source_file_hash: Optional[str] = None
    source_file_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        d["flag_montant_superieur_5000"] = self.flag_montant_superieur_5000
        d["source_file_hash"] = self.source_file_hash
        d["source_file_name"] = self.source_file_name
        return d
