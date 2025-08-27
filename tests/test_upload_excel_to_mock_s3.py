# tests/test_upload_excel_to_mock_s3.py
# Tests 1–5 for upload_excel_to_mock_s3(), without creating a real Excel file.

import os
import builtins
import importlib.util
import pytest


# --- fixture: load crawler.py by file path (avoids import path issues) ---
@pytest.fixture
def crawler_mod(monkeypatch):
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    SRC_FILE = os.path.join(ROOT, "crawler.py")  # change if your file is named differently

    if not os.path.exists(SRC_FILE):
        raise FileNotFoundError(f"Couldn't find crawler.py at: {SRC_FILE}")

    spec = importlib.util.spec_from_file_location("crawler", SRC_FILE)
    crawler = importlib.util.module_from_spec(spec)

    # Avoid interactive input() during import (if present in your module)
    monkeypatch.setattr(builtins, "input", lambda *a, **k: "DUMMY_DATASET_ID")

    spec.loader.exec_module(crawler)
    return crawler


# --- helper: make a tiny dummy file (not a real Excel) ---
@pytest.fixture
def make_file(tmp_path):
    """
    Create a small binary file with a .xlsx filename.
    This is enough for testing because the function only uploads bytes;
    it doesn't parse the Excel contents.
    """
    def _make(name="out.xlsx", content=b"dummy,not,real,excel\nrow2"):
        p = tmp_path / name
        p.write_bytes(content)
        return p
    return _make


# 1) Missing file -> FileNotFoundError
def test_upload_missing_file_raises(crawler_mod, tmp_path, caplog):
    caplog.set_level("ERROR")
    missing = tmp_path / "no_such.xlsx"
    with pytest.raises(FileNotFoundError):
        crawler_mod.upload_excel_to_mock_s3(str(missing))
    # optional/log check
    assert any("not found" in m.lower() or "excel file" in m.lower() for m in caplog.messages)


# 2) Happy path (default bucket/key) — succeeds with a simple dummy file
def test_upload_happy_path_default_bucket_key(crawler_mod, make_file, caplog):
    caplog.set_level("INFO")
    dummy = make_file()  # create out.xlsx with a few bytes

    # Call: should not raise; function uses Moto internally
    crawler_mod.upload_excel_to_mock_s3(str(dummy))

    # We can't inspect S3 after (Moto ctx is inside the function), so rely on logs
    msg = " ".join(caplog.messages).lower()
    assert "upload" in msg or "uploaded" in msg
    assert "size" in msg and ("match" in msg or "matches" in msg)


# 3) Happy path with custom bucket/key — ensure parameters are honored
def test_upload_happy_path_custom_bucket_key(crawler_mod, make_file, caplog):
    caplog.set_level("INFO")
    dummy = make_file("custom.xlsx")

    bucket = "my-test-bucket"
    key = "folder/sub/custom.xlsx"

    crawler_mod.upload_excel_to_mock_s3(str(dummy), bucket=bucket, key=key)

    msg = " ".join(caplog.messages).lower()
    assert "upload" in msg or "uploaded" in msg  # success signal


# 4) General exception path — force boto3 client creation to fail
def test_upload_general_exception_logs_and_raises(crawler_mod, make_file, monkeypatch, caplog):
    caplog.set_level("ERROR")
    dummy = make_file("boom.xlsx")

    import boto3
    def boom_client(*a, **k):
        raise RuntimeError("kaboom")
    monkeypatch.setattr(boto3, "client", boom_client)

    with pytest.raises(RuntimeError):
        crawler_mod.upload_excel_to_mock_s3(str(dummy))

    assert any("failed" in m.lower() or "error" in m.lower() for m in caplog.messages)


# 5) Size mismatch warning — fake local size so the comparison fails
def test_upload_size_mismatch_warns(crawler_mod, make_file, monkeypatch, caplog):
    caplog.set_level("WARNING")
    dummy = make_file("mismatch.xlsx", content=b"1234567890")  # 10 bytes

    # Lie about local file size: report +999 bytes more than actual
    real_getsize = os.path.getsize
    monkeypatch.setattr(os.path, "getsize", lambda p: real_getsize(p) + 999)

    crawler_mod.upload_excel_to_mock_s3(str(dummy))

    assert any("mismatch" in m.lower() for m in caplog.messages)
