"""Integration Tests for S3 Storage Pipeline (Niveau 2).
Tests S3 bucket management, upload, download with retry, and idempotency.
"""
from io import BytesIO
from pathlib import Path
import pytest
from botocore.exceptions import ClientError

from data_pipeline.s3_service import (
    ensure_bucket_exists,
    upload_file_to_s3,
    list_s3_files,
    get_s3_file_content_with_retry,
    run_s3_pipeline,
)
from data_pipeline.retry import MaxRetriesExceededError


class MockS3Client:
    """In-memory mock S3 client for isolated test environments."""

    def __init__(self):
        self.buckets = {}

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket"
            )
        return {}

    def create_bucket(self, Bucket):
        if Bucket not in self.buckets:
            self.buckets[Bucket] = {}
        return {}

    def upload_file(self, Filename, Bucket, Key):
        if Bucket not in self.buckets:
            raise ClientError(
                {"Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}},
                "UploadFile",
            )
        with open(Filename, "rb") as f:
            self.buckets[Bucket][Key] = f.read()

    def list_objects_v2(self, Bucket, Prefix=""):
        if Bucket not in self.buckets:
            return {}
        items = [
            {"Key": k}
            for k in self.buckets[Bucket].keys()
            if k.startswith(Prefix)
        ]
        return {"Contents": items} if items else {}

    def get_object(self, Bucket, Key):
        if Bucket not in self.buckets or Key not in self.buckets[Bucket]:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist"}},
                "GetObject",
            )
        data = self.buckets[Bucket][Key]
        return {"Body": BytesIO(data)}


@pytest.fixture
def mock_s3():
    """Fixture providing an in-memory S3 client mock."""
    return MockS3Client()


def test_ensure_bucket_exists(mock_s3):
    """Verify bucket creation when not present."""
    bucket_name = "test-bucket"
    assert bucket_name not in mock_s3.buckets
    ensure_bucket_exists(mock_s3, bucket_name)
    assert bucket_name in mock_s3.buckets


def test_upload_and_list_s3_files(mock_s3, temp_dir):
    """Verify uploading a CSV to S3 and listing bucket contents."""
    bucket_name = "finance-bucket"
    ensure_bucket_exists(mock_s3, bucket_name)

    test_file = temp_dir / "sample.csv"
    test_file.write_text("test,col\n1,2\n", encoding="utf-8")

    upload_file_to_s3(mock_s3, test_file, s3_key="sample.csv", bucket_name=bucket_name)

    files = list_s3_files(mock_s3, bucket_name=bucket_name)
    assert "sample.csv" in files


def test_get_s3_file_content_with_retry_on_transient_failure(mock_s3, temp_dir):
    """Verify that fetching file from S3 retries on transient network/service errors."""
    bucket_name = "finance-bucket"
    ensure_bucket_exists(mock_s3, bucket_name)

    test_file = temp_dir / "tx.csv"
    test_file.write_text("dummy,content\n", encoding="utf-8")
    upload_file_to_s3(mock_s3, test_file, s3_key="tx.csv", bucket_name=bucket_name)

    attempts = {"count": 0}

    def transient_net_error():
        attempts["count"] += 1
        if attempts["count"] <= 2:
            raise ConnectionError(f"Connexion réseau S3 interrompue (essai {attempts['count']})")

    content = get_s3_file_content_with_retry(
        mock_s3,
        s3_key="tx.csv",
        bucket_name=bucket_name,
        max_retries=3,
        initial_delay=0.01,
        transient_failure_hook=transient_net_error,
    )

    assert content.strip() == "dummy,content"
    assert attempts["count"] == 3


def test_get_s3_file_content_fatal_failure_raises(mock_s3, temp_dir):
    """Verify that permanent S3 unavailability raises MaxRetriesExceededError cleanly."""
    bucket_name = "finance-bucket"
    ensure_bucket_exists(mock_s3, bucket_name)

    test_file = temp_dir / "tx2.csv"
    test_file.write_text("data\n", encoding="utf-8")
    upload_file_to_s3(mock_s3, test_file, s3_key="tx2.csv", bucket_name=bucket_name)

    def fatal_net_error():
        raise ConnectionError("Service S3 définitivement inaccessible.")

    with pytest.raises(MaxRetriesExceededError):
        get_s3_file_content_with_retry(
            mock_s3,
            s3_key="tx2.csv",
            bucket_name=bucket_name,
            max_retries=2,
            initial_delay=0.01,
            transient_failure_hook=fatal_net_error,
        )


def test_run_s3_pipeline_end_to_end(mock_s3, temp_dir):
    """Verify end-to-end Level 2 pipeline execution using S3 mock."""
    db_path = temp_dir / "s3_pipeline_test.db"
    db_url = f"sqlite:///{db_path}"

    summary = run_s3_pipeline(
        s3_client=mock_s3,
        bucket_name="finance-transactions",
        db_url=db_url,
        generate_local_first=True,
        local_data_dir=temp_dir,
    )

    assert summary["processed_files"] == 3
    assert summary["total_transactions_inserted"] == 21

    # Re-running the pipeline should find the files in S3 and skip them (idempotency)
    summary_rerun = run_s3_pipeline(
        s3_client=mock_s3,
        bucket_name="finance-transactions",
        db_url=db_url,
        generate_local_first=False,
    )
    assert summary_rerun["processed_files"] == 0
    assert summary_rerun["skipped_files"] == 3
    assert summary_rerun["total_transactions_inserted"] == 0
