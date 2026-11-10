"""Tests for CSV Generator (Niveau 1.1)."""
from pathlib import Path
import csv
from data_pipeline.generator import (
    generate_valid_csv_files,
    generate_corrupted_csv_file,
    CSV_COLUMNS,
)


def test_generator_creates_batches_between_5_and_10(temp_dir):
    """Verify that generated batches contain between 5 and 10 transactions."""
    files = generate_valid_csv_files(output_dir=temp_dir)
    assert len(files) == 3

    for f in files:
        assert f.is_file()
        with open(f, "r", encoding="utf-8") as csv_f:
            reader = csv.DictReader(csv_f)
            rows = list(reader)
            assert 5 <= len(rows) <= 10, f"File {f.name} contains {len(rows)} rows, expected 5 to 10"


def test_generator_has_required_columns(temp_dir):
    """Verify all required columns are present in generated CSV files."""
    files = generate_valid_csv_files(output_dir=temp_dir)
    for f in files:
        with open(f, "r", encoding="utf-8") as csv_f:
            reader = csv.DictReader(csv_f)
            assert reader.fieldnames == CSV_COLUMNS


def test_generator_contains_values_above_and_below_5000(temp_dir):
    """Verify deterministic dataset has test cases with amount > 5000 and <= 5000."""
    files = generate_valid_csv_files(output_dir=temp_dir)
    all_montants = []
    for f in files:
        with open(f, "r", encoding="utf-8") as csv_f:
            reader = csv.DictReader(csv_f)
            for row in reader:
                all_montants.append(float(row["montant"]))

    has_above_5000 = any(m > 5000 for m in all_montants)
    has_below_5000 = any(m <= 5000 for m in all_montants)
    assert has_above_5000, "Should contain amounts > 5000 for flag testing"
    assert has_below_5000, "Should contain amounts <= 5000 for flag testing"


def test_generate_corrupted_csv(temp_dir):
    """Verify corrupted CSV generation for failure simulation testing."""
    corrupted_path = generate_corrupted_csv_file(output_dir=temp_dir)
    assert corrupted_path.is_file()
    with open(corrupted_path, "r", encoding="utf-8") as f:
        content = f.read()
        assert "-999" in content or "NOT_A_NUMBER" in content
