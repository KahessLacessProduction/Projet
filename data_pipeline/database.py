"""Relational Database layer using SQLAlchemy (Niveau 1.3 & 1.7).
Manages tables for transactions, aggregated metrics, and processed file deduplication.
Ensures idempotency: files already processed are never re-inserted.
Includes automated retry handling for database insertions.
"""
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    select,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.exc import OperationalError, DatabaseError

from config import (
    DB_URL,
    RETRY_MAX_ATTEMPTS,
    RETRY_INITIAL_DELAY,
    RETRY_BACKOFF_FACTOR,
)
from data_pipeline.models import ProcessedTransaction
from data_pipeline.retry import with_retry, MaxRetriesExceededError

logger = logging.getLogger("data_pipeline.database")

Base = declarative_base()


class ProcessedFileRecord(Base):
    """Tracks files processed by the pipeline to guarantee idempotency and avoid duplicates."""

    __tablename__ = "processed_files"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    file_hash = Column(String(64), unique=True, nullable=False, index=True)
    record_count = Column(Integer, nullable=False, default=0)
    status = Column(String(32), nullable=False, default="SUCCESS")
    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class TransactionRecord(Base):
    """Stores individual processed transactions with flag."""

    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime_transaction = Column(DateTime, nullable=False)
    iban_origine = Column(String(34), nullable=False, index=True)
    pays_source = Column(String(2), nullable=False)
    banque_source = Column(String(100), nullable=False, index=True)
    iban_destinataire = Column(String(34), nullable=False, index=True)
    pays_destinataire = Column(String(2), nullable=False)
    montant = Column(Float, nullable=False)
    flag_montant_superieur_5000 = Column(Boolean, nullable=False, default=False)
    source_file_hash = Column(
        String(64),
        ForeignKey("processed_files.file_hash"),
        nullable=False,
    )
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class AggregateIbanOrigine(Base):
    """Aggregate table: total amounts sent per iban_origine."""

    __tablename__ = "aggregats_iban_origine"

    iban_origine = Column(String(34), primary_key=True)
    total_envoye = Column(Float, nullable=False, default=0.0)
    nb_transactions = Column(Integer, nullable=False, default=0)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AggregateBanqueSource(Base):
    """Aggregate table: total amounts sent per banque_source."""

    __tablename__ = "aggregats_banque_source"

    banque_source = Column(String(100), primary_key=True)
    total_envoye = Column(Float, nullable=False, default=0.0)
    nb_transactions = Column(Integer, nullable=False, default=0)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AggregateIbanDestinataire(Base):
    """Aggregate table: total amounts received per iban_destinataire."""

    __tablename__ = "aggregats_iban_destinataire"

    iban_destinataire = Column(String(34), primary_key=True)
    total_recu = Column(Float, nullable=False, default=0.0)
    nb_transactions = Column(Integer, nullable=False, default=0)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def get_engine(db_url: str = DB_URL):
    """Create SQLAlchemy engine with appropriate connect args."""
    connect_args = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(db_url, connect_args=connect_args)


def init_database(engine=None, db_url: str = DB_URL):
    """Initialize database schema (creates all tables if they do not exist)."""
    if engine is None:
        engine = get_engine(db_url)
    Base.metadata.create_all(engine)
    logger.info(f"Base de données initialisée avec succès ({engine.url})")
    return engine


def is_file_already_processed(session: Session, file_hash: str) -> bool:
    """Check whether a file with this hash was already processed successfully."""
    stmt = select(ProcessedFileRecord).where(
        ProcessedFileRecord.file_hash == file_hash,
        ProcessedFileRecord.status == "SUCCESS",
    )
    result = session.execute(stmt).scalar_one_or_none()
    return result is not None


def _update_aggregates(session: Session, transactions: List[ProcessedTransaction]):
    """Update running aggregate totals for IBANs and banks in the database."""
    # 1. Update iban_origine aggregates
    iban_envoyes: Dict[str, float] = {}
    iban_counts: Dict[str, int] = {}
    banque_envoyes: Dict[str, float] = {}
    banque_counts: Dict[str, int] = {}
    dest_recus: Dict[str, float] = {}
    dest_counts: Dict[str, int] = {}

    for tx in transactions:
        iban_envoyes[tx.iban_origine] = iban_envoyes.get(tx.iban_origine, 0.0) + tx.montant
        iban_counts[tx.iban_origine] = iban_counts.get(tx.iban_origine, 0) + 1

        banque_envoyes[tx.banque_source] = banque_envoyes.get(tx.banque_source, 0.0) + tx.montant
        banque_counts[tx.banque_source] = banque_counts.get(tx.banque_source, 0) + 1

        dest_recus[tx.iban_destinataire] = dest_recus.get(tx.iban_destinataire, 0.0) + tx.montant
        dest_counts[tx.iban_destinataire] = dest_counts.get(tx.iban_destinataire, 0) + 1

    # Update or insert iban_origine
    for iban, total in iban_envoyes.items():
        rec = session.get(AggregateIbanOrigine, iban)
        if rec:
            rec.total_envoye = round(rec.total_envoye + total, 2)
            rec.nb_transactions += iban_counts[iban]
            rec.last_updated = datetime.utcnow()
        else:
            session.add(
                AggregateIbanOrigine(
                    iban_origine=iban,
                    total_envoye=round(total, 2),
                    nb_transactions=iban_counts[iban],
                )
            )

    # Update or insert banque_source
    for bq, total in banque_envoyes.items():
        rec = session.get(AggregateBanqueSource, bq)
        if rec:
            rec.total_envoye = round(rec.total_envoye + total, 2)
            rec.nb_transactions += banque_counts[bq]
            rec.last_updated = datetime.utcnow()
        else:
            session.add(
                AggregateBanqueSource(
                    banque_source=bq,
                    total_envoye=round(total, 2),
                    nb_transactions=banque_counts[bq],
                )
            )

    # Update or insert iban_destinataire
    for dest, total in dest_recus.items():
        rec = session.get(AggregateIbanDestinataire, dest)
        if rec:
            rec.total_recu = round(rec.total_recu + total, 2)
            rec.nb_transactions += dest_counts[dest]
            rec.last_updated = datetime.utcnow()
        else:
            session.add(
                AggregateIbanDestinataire(
                    iban_destinataire=dest,
                    total_recu=round(total, 2),
                    nb_transactions=dest_counts[dest],
                )
            )


def insert_processed_data_with_retry(
    engine,
    transactions: List[ProcessedTransaction],
    file_hash: str,
    filename: str,
    max_retries: int = RETRY_MAX_ATTEMPTS,
    initial_delay: float = RETRY_INITIAL_DELAY,
    backoff_factor: float = RETRY_BACKOFF_FACTOR,
    transient_failure_sim: Optional[Any] = None,
) -> Dict[str, Any]:
    """Insert processed transactions and update aggregates with automatic retry and idempotency check.

    Guarantees that files already processed are SKIPPED without duplication.
    In case of database lock or transient failure, retries with exponential backoff.
    """
    session_factory = sessionmaker(bind=engine)

    # 1. Idempotency check outside of insert retry loop
    with session_factory() as check_session:
        if is_file_already_processed(check_session, file_hash):
            logger.info(
                f"[IDEMPOTENCE] Fichier '{filename}' (hash: {file_hash[:8]}...) déjà traité. "
                f"Traitement ignoré pour éviter les doublons."
            )
            return {
                "status": "SKIPPED",
                "filename": filename,
                "file_hash": file_hash,
                "inserted_count": 0,
                "reason": "File already processed",
            }

    # 2. Insertion block protected by retry
    @with_retry(
        max_attempts=max_retries,
        initial_delay=initial_delay,
        backoff_factor=backoff_factor,
        retryable_exceptions=(OperationalError, DatabaseError, RuntimeError),
        operation_name=f"Insertion DB ({filename})",
    )
    def _do_insert():
        if transient_failure_sim:
            transient_failure_sim()

        with session_factory() as session:
            try:
                # Add file tracking record
                file_rec = ProcessedFileRecord(
                    filename=filename,
                    file_hash=file_hash,
                    record_count=len(transactions),
                    status="SUCCESS",
                    processed_at=datetime.utcnow(),
                )
                session.add(file_rec)

                # Add individual transactions
                for tx in transactions:
                    tx_rec = TransactionRecord(
                        datetime_transaction=tx.datetime_transaction,
                        iban_origine=tx.iban_origine,
                        pays_source=tx.pays_source,
                        banque_source=tx.banque_source,
                        iban_destinataire=tx.iban_destinataire,
                        pays_destinataire=tx.pays_destinataire,
                        montant=tx.montant,
                        flag_montant_superieur_5000=tx.flag_montant_superieur_5000,
                        source_file_hash=file_hash,
                    )
                    session.add(tx_rec)

                # Update aggregates atomically
                _update_aggregates(session, transactions)

                session.commit()
                logger.info(
                    f"[DB INSERT OK] Fichier '{filename}' : {len(transactions)} transactions insérées."
                )
                return {
                    "status": "INSERTED",
                    "filename": filename,
                    "file_hash": file_hash,
                    "inserted_count": len(transactions),
                }
            except Exception as err:
                session.rollback()
                logger.error(f"[DB ROLLBACK] Échec d'insertion pour '{filename}' : {err}")
                raise

    return _do_insert()
