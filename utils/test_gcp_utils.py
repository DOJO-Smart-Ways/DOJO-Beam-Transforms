# tests/test_gcp_utils.py
import pytest
from utils.gcp_utils import make_gcs_path

def test_make_gcs_path_with_args():
    assert make_gcs_path("bucket", "path", "to", "file") == "gs://bucket/path/to/file"
    assert make_gcs_path("bucket", "single_file") == "gs://bucket/single_file"
    assert make_gcs_path("bucket") == "gs://bucket"

def test_make_gcs_path_without_args():
    assert make_gcs_path() == ""
