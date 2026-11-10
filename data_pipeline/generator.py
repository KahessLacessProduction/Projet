"""CSV Generator for Financial Transactions (Niveau 1.1).
Generates deterministic batches of 5 to 10 financial transactions.
Allows deterministic testability and predictable aggregate calculations.
Supports creating intentionally invalid files for failure simulation (Niveau 1.6).
"""
import csv
from pathlib import Path
from typing import List, Dict, Any
import click
from config import RAW_DATA_DIR

CSV_COLUMNS = [
    "datetime_transaction",
    "iban_origine",
    "pays_source",
    "banque_source",
    "iban_destinataire",
    "pays_destinataire",
    "montant",
]

# Curated deterministic datasets designed for verifiable test assertions
DETERMINISTIC_BATCH_1: List[Dict[str, Any]] = [
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
        "montant": 6200.00,  # > 5000 -> Flag TRUE
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
    {
        "datetime_transaction": "2026-11-01T14:00:00",
        "iban_origine": "FR7610011000229876543210987",
        "pays_source": "FR",
        "banque_source": "SOCIETE_GENERALE",
        "iban_destinataire": "IT60X0542811101000000123456",
        "pays_destinataire": "IT",
        "montant": 7800.00,  # > 5000 -> Flag TRUE
    },
    {
        "datetime_transaction": "2026-11-01T15:20:00",
        "iban_origine": "DE89370400440532013000",
        "pays_source": "DE",
        "banque_source": "DEUTSCHE_BANK",
        "iban_destinataire": "FR7630006000011234567890189",
        "pays_destinataire": "FR",
        "montant": 2300.00,
    },
    {
        "datetime_transaction": "2026-11-01T16:10:00",
        "iban_origine": "DE89370400440532013000",
        "pays_source": "DE",
        "banque_source": "DEUTSCHE_BANK",
        "iban_destinataire": "ES9121000418450200051332",
        "pays_destinataire": "ES",
        "montant": 150.00,
    },
    {
        "datetime_transaction": "2026-11-01T17:50:00",
        "iban_origine": "DE89370400440532013000",
        "pays_source": "DE",
        "banque_source": "DEUTSCHE_BANK",
        "iban_destinataire": "IT60X0542811101000000123456",
        "pays_destinataire": "IT",
        "montant": 5200.00,  # > 5000 -> Flag TRUE
    },
]

DETERMINISTIC_BATCH_2: List[Dict[str, Any]] = [
    {
        "datetime_transaction": "2026-11-02T09:00:00",
        "iban_origine": "FR7630006000011234567890189",
        "pays_source": "FR",
        "banque_source": "BNP_PARIBAS",
        "iban_destinataire": "NL91ABNA0417164300",
        "pays_destinataire": "NL",
        "montant": 3200.00,
    },
    {
        "datetime_transaction": "2026-11-02T10:15:00",
        "iban_origine": "FR7630006000011234567890189",
        "pays_source": "FR",
        "banque_source": "BNP_PARIBAS",
        "iban_destinataire": "DE89370400440532013000",
        "pays_destinataire": "DE",
        "montant": 8500.00,  # > 5000 -> Flag TRUE
    },
    {
        "datetime_transaction": "2026-11-02T11:30:00",
        "iban_origine": "ES9121000418450200051332",
        "pays_source": "ES",
        "banque_source": "BANCO_SANTANDER",
        "iban_destinataire": "FR7610011000229876543210987",
        "pays_destinataire": "FR",
        "montant": 1200.00,
    },
    {
        "datetime_transaction": "2026-11-02T13:45:00",
        "iban_origine": "ES9121000418450200051332",
        "pays_source": "ES",
        "banque_source": "BANCO_SANTANDER",
        "iban_destinataire": "IT60X0542811101000000123456",
        "pays_destinataire": "IT",
        "montant": 950.00,
    },
    {
        "datetime_transaction": "2026-11-02T15:00:00",
        "iban_origine": "FR7610011000229876543210987",
        "pays_source": "FR",
        "banque_source": "SOCIETE_GENERALE",
        "iban_destinataire": "DE89370400440532013000",
        "pays_destinataire": "DE",
        "montant": 5100.00,  # > 5000 -> Flag TRUE
    },
    {
        "datetime_transaction": "2026-11-02T16:30:00",
        "iban_origine": "FR7610011000229876543210987",
        "pays_source": "FR",
        "banque_source": "SOCIETE_GENERALE",
        "iban_destinataire": "ES9121000418450200051332",
        "pays_destinataire": "ES",
        "montant": 200.00,
    },
]

DETERMINISTIC_BATCH_3: List[Dict[str, Any]] = [
    {
        "datetime_transaction": "2026-11-03T08:30:00",
        "iban_origine": "IT60X0542811101000000123456",
        "pays_source": "IT",
        "banque_source": "INTESA_SANPAOLO",
        "iban_destinataire": "FR7630006000011234567890189",
        "pays_destinataire": "FR",
        "montant": 4200.00,
    },
    {
        "datetime_transaction": "2026-11-03T09:40:00",
        "iban_origine": "IT60X0542811101000000123456",
        "pays_source": "IT",
        "banque_source": "INTESA_SANPAOLO",
        "iban_destinataire": "DE89370400440532013000",
        "pays_destinataire": "DE",
        "montant": 10500.00,  # > 5000 -> Flag TRUE
    },
    {
        "datetime_transaction": "2026-11-03T10:50:00",
        "iban_origine": "NL91ABNA0417164300",
        "pays_source": "NL",
        "banque_source": "ABN_AMRO",
        "iban_destinataire": "ES9121000418450200051332",
        "pays_destinataire": "ES",
        "montant": 3100.00,
    },
    {
        "datetime_transaction": "2026-11-03T11:20:00",
        "iban_origine": "NL91ABNA0417164300",
        "pays_source": "NL",
        "banque_source": "ABN_AMRO",
        "iban_destinataire": "FR7610011000229876543210987",
        "pays_destinataire": "FR",
        "montant": 600.00,
    },
    {
        "datetime_transaction": "2026-11-03T14:10:00",
        "iban_origine": "FR7630006000011234567890189",
        "pays_source": "FR",
        "banque_source": "BNP_PARIBAS",
        "iban_destinataire": "IT60X0542811101000000123456",
        "pays_destinataire": "IT",
        "montant": 1800.00,
    },
    {
        "datetime_transaction": "2026-11-03T15:30:00",
        "iban_origine": "DE89370400440532013000",
        "pays_source": "DE",
        "banque_source": "DEUTSCHE_BANK",
        "iban_destinataire": "FR7630006000011234567890189",
        "pays_destinataire": "FR",
        "montant": 9200.00,  # > 5000 -> Flag TRUE
    },
    {
        "datetime_transaction": "2026-11-03T16:45:00",
        "iban_origine": "ES9121000418450200051332",
        "pays_source": "ES",
        "banque_source": "BANCO_SANTANDER",
        "iban_destinataire": "DE89370400440532013000",
        "pays_destinataire": "DE",
        "montant": 5400.00,  # > 5000 -> Flag TRUE
    },
    {
        "datetime_transaction": "2026-11-03T17:15:00",
        "iban_origine": "FR7610011000229876543210987",
        "pays_source": "FR",
        "banque_source": "SOCIETE_GENERALE",
        "iban_destinataire": "NL91ABNA0417164300",
        "pays_destinataire": "NL",
        "montant": 750.00,
    },
]

BATCHES = [
    ("transactions_batch_1.csv", DETERMINISTIC_BATCH_1),
    ("transactions_batch_2.csv", DETERMINISTIC_BATCH_2),
    ("transactions_batch_3.csv", DETERMINISTIC_BATCH_3),
]


def write_csv_file(target_path: Path, transactions: List[Dict[str, Any]]) -> Path:
    """Write list of transaction dicts to a CSV file."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in transactions:
            writer.writerow(row)
    return target_path


def generate_valid_csv_files(output_dir: Path = RAW_DATA_DIR) -> List[Path]:
    """Generate all standard deterministic CSV files in the target directory."""
    created_files: List[Path] = []
    for filename, rows in BATCHES:
        file_path = output_dir / filename
        write_csv_file(file_path, rows)
        created_files.append(file_path)
    return created_files


def generate_corrupted_csv_file(
    output_dir: Path = RAW_DATA_DIR,
    filename: str = "transactions_corrupted.csv",
    corruption_type: str = "invalid_montant",
) -> Path:
    """Generate an intentionally corrupted CSV file for failure testing (Niveau 1.6).

    Supported corruption types:
    - 'invalid_montant': contains negative or non-numeric amount
    - 'missing_columns': missing required column
    - 'invalid_date': malformed date string
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    target_path = output_dir / filename

    if corruption_type == "missing_columns":
        with open(target_path, "w", newline="", encoding="utf-8") as f:
            f.write("datetime_transaction,iban_origine,montant\n")
            f.write("2026-11-01T10:00:00,FR7630006000011234567890189,150.0\n")
        return target_path

    # Default: invalid_montant
    corrupted_data = [
        {
            "datetime_transaction": "2026-11-04T10:00:00",
            "iban_origine": "FR7630006000011234567890189",
            "pays_source": "FR",
            "banque_source": "BNP_PARIBAS",
            "iban_destinataire": "DE89370400440532013000",
            "pays_destinataire": "DE",
            "montant": -999.00,  # Negative amount violates schema
        },
        {
            "datetime_transaction": "2026-11-04T11:00:00",
            "iban_origine": "FR7630006000011234567890189",
            "pays_source": "FR",
            "banque_source": "BNP_PARIBAS",
            "iban_destinataire": "ES9121000418450200051332",
            "pays_destinataire": "ES",
            "montant": "NOT_A_NUMBER",  # Invalid type
        },
    ]

    with open(target_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in corrupted_data:
            writer.writerow(row)

    return target_path


@click.command()
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=RAW_DATA_DIR,
    help="Target directory for generated CSV files.",
)
@click.option(
    "--corrupted",
    is_flag=True,
    help="Generate an intentionally corrupted CSV file for failure testing.",
)
def cli(output_dir: Path, corrupted: bool):
    """CLI to generate financial transaction CSV files."""
    if corrupted:
        p = generate_corrupted_csv_file(output_dir)
        click.secho(f"[WARN] Fichier corrompu généré : {p}", fg="yellow")
    else:
        files = generate_valid_csv_files(output_dir)
        click.secho(f"[OK] {len(files)} fichiers CSV générés dans {output_dir}", fg="green")
        for f in files:
            click.echo(f"  -> {f.name}")


if __name__ == "__main__":
    cli()
