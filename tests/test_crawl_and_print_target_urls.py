

import os
import builtins
import importlib.util
import pytest
import logging


# Load crawler.py directly by file path so we avoid PYTHONPATH/module-name issues.
@pytest.fixture
def crawler_mod(monkeypatch):
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_file = os.path.join(root, "crawler.py")  # change if your file is named differently

    if not os.path.exists(src_file):
        raise FileNotFoundError(f"Couldn't find crawler.py at: {src_file}")

    spec = importlib.util.spec_from_file_location("crawler", src_file)
    mod = importlib.util.module_from_spec(spec)

    # If crawler.py ever prompts for input() at import time, feed a dummy value.
    monkeypatch.setattr(builtins, "input", lambda *a, **k: "DUMMY_DATASET_ID")

    spec.loader.exec_module(mod)  # this runs your module
    return mod


# 1) Happy path:
#    - collector returns links
#    - function prints a preview of first N
#    - writes the Excel file
#    - calls upload exactly once
def test_crawl_happy_path(monkeypatch, crawler_mod, tmp_path, capsys):
    # Fake the collected links (no real HTTP).
    links = [
        f"{crawler_mod.LINK_HOST}?altId=L1",
        f"{crawler_mod.LINK_HOST}?altId=L2",
        f"{crawler_mod.LINK_HOST}?altId=L3",
    ]
    monkeypatch.setattr(crawler_mod, "collect_all_links_from_permitnums", lambda: links)

    # Count how many times upload is called.
    calls = {"n": 0}
    def fake_upload(excel_fp, *a, **k):
        calls["n"] += 1
    monkeypatch.setattr(crawler_mod, "upload_excel_to_mock_s3", fake_upload)

    # Patch DataFrame.to_excel to just write some bytes (no real Excel engine needed).
    import pandas as pd
    def fake_to_excel(self, path, *a, **k):
        with open(path, "wb") as f:
            f.write(b"dummy-excel-bytes")
    monkeypatch.setattr(pd.DataFrame, "to_excel", fake_to_excel)

    out_xlsx = tmp_path / "links.xlsx"

    # Run the function.
    crawler_mod.crawl_and_print_target_urls(show_n=2, excel_file=str(out_xlsx))
    printed = capsys.readouterr().out  # capture printed preview

    # Preview should mention "First 2 links" and include the first two URLs.
    assert "First 2 links" in printed
    assert links[0] in printed and links[1] in printed

    # File should exist and upload should have been called once.
    assert out_xlsx.exists() and out_xlsx.stat().st_size > 0
    assert calls["n"] == 1


# 2) Collector raises:
#    - function handles the error (prints something with "error")
#    - no Excel file is written
#    - upload is NOT called
def test_crawl_collector_raises(monkeypatch, crawler_mod, tmp_path, caplog):
    # Make the collector raise
    def boom():
        raise RuntimeError("collect fail")
    monkeypatch.setattr(crawler_mod, "collect_all_links_from_permitnums", boom)

    # Ensure upload is NOT called if collect fails
    called = {"upload": False}
    def guard_upload(*args, **kwargs):
        called["upload"] = True
    monkeypatch.setattr(crawler_mod, "upload_excel_to_mock_s3", guard_upload)

    out_xlsx = tmp_path / "links.xlsx"

    # Capture error logs from your 'crawler' logger
    caplog.set_level(logging.ERROR, logger="crawler")

    # Run: should handle exception internally (no raise)
    crawler_mod.crawl_and_print_target_urls(show_n=3, excel_file=str(out_xlsx))

    # Assert: error was logged
    messages = [rec.message.lower() for rec in caplog.records]
    assert any("loading failed" in m for m in messages)

    # Assert: no file created, upload not attempted
    assert not out_xlsx.exists()
    assert not called["upload"]


# 3) Excel write fails:
#    - DataFrame.to_excel raises
#    - function does not crash
#    - no upload attempt
#    - file is not created
def test_crawl_to_excel_fails(monkeypatch, crawler_mod, tmp_path):
    # Return one link so we try to write an Excel.
    monkeypatch.setattr(
        crawler_mod, "collect_all_links_from_permitnums",
        lambda: [f"{crawler_mod.LINK_HOST}?altId=Y1"]
    )

    # Force to_excel to fail.
    import pandas as pd
    def boom(self, *a, **k):
        raise IOError("disk full")
    monkeypatch.setattr(pd.DataFrame, "to_excel", boom)

    # Upload must NOT be called if writing fails.
    def should_not_upload(*a, **k):
        raise AssertionError("upload should NOT be called when Excel write fails")
    monkeypatch.setattr(crawler_mod, "upload_excel_to_mock_s3", should_not_upload)

    out_xlsx = tmp_path / "links.xlsx"

    # Run it—should swallow the write error internally.
    crawler_mod.crawl_and_print_target_urls(show_n=1, excel_file=str(out_xlsx))

    # No file created since writing failed; upload not reached.
    assert not out_xlsx.exists()


# 4) Upload fails:
#    - Excel writing succeeds
#    - upload raises
#    - function does not crash
#    - Excel file still exists
def test_crawl_upload_fails(monkeypatch, crawler_mod, tmp_path):
    # Return one link so we write an Excel.
    monkeypatch.setattr(
        crawler_mod, "collect_all_links_from_permitnums",
        lambda: [f"{crawler_mod.LINK_HOST}?altId=K1"]
    )

    # Make to_excel write dummy bytes successfully.
    import pandas as pd
    def fake_to_excel(self, path, *a, **k):
        with open(path, "wb") as f:
            f.write(b"dummy-excel-bytes")
    monkeypatch.setattr(pd.DataFrame, "to_excel", fake_to_excel)

    # Upload raises an error.
    def boom(*a, **k):
        raise RuntimeError("S3 down")
    monkeypatch.setattr(crawler_mod, "upload_excel_to_mock_s3", boom)

    out_xlsx = tmp_path / "links.xlsx"

    # Run it—should not raise.
    crawler_mod.crawl_and_print_target_urls(show_n=1, excel_file=str(out_xlsx))

    # Even though upload failed, the Excel file should still exist.
    assert out_xlsx.exists() and out_xlsx.stat().st_size > 0
