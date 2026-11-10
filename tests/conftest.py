"""Pytest fixtures for unit and integration testing."""
import gc
import shutil
import tempfile
from pathlib import Path
import pytest
from sqlalchemy import create_engine

from data_pipeline.database import Base


@pytest.fixture
def temp_dir():
    """Provide a temporary directory for file I/O safely cleaned up across OS platforms."""
    tmp = tempfile.mkdtemp()
    yield Path(tmp)
    gc.collect()
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def sqlite_engine():
    """Provide an in-memory SQLite engine with tables initialized."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def sample_transactions_raw():
    """Provide deterministic sample transaction dictionaries."""
    return [
        {
            "datetime_transaction": "2026-11-01T09:15:00",
            "iban_origine": "FR7630006000011234567890189",
            "pays_source": "FR",
            "banque_source": "BNP_PARIBAS",
            "iban_destinataire": "DE89370400440532013000",
            "pays_destinataire": "DE",
            "montant": 1500.00,
        },
        {
            "datetime_transaction": "2026-11-01T10:30:00",
            "iban_origine": "FR7630006000011234567890189",
            "pays_source": "FR",
            "banque_source": "BNP_PARIBAS",
            "iban_destinataire": "ES9121000418450200051332",
            "pays_destinataire": "ES",
            "montant": 6200.00,  # > 5000
        },
        {
            "datetime_transaction": "2026-11-01T11:45:00",
            "iban_origine": "FR7610011000229876543210987",
            "pays_source": "FR",
            "banque_source": "SOCIETE_GENERALE",
            "iban_destinataire": "DE89370400440532013000",
            "pays_destinataire": "DE",
            "montant": 450.50,
        },
    ]
