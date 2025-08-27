# tests/conftest.py
import sys
import builtins
import pytest

@pytest.fixture
def crawler_mod(monkeypatch):
    """Import crawler.py fresh and make it deterministic."""
    # if already imported in this python process, remove it
    if "crawler" in sys.modules:
        del sys.modules["crawler"]

    # if your crawler.py prompts for input() at import, stub it here:
    monkeypatch.setattr(builtins, "input", lambda *a, **k: "DUMMY_DATASET_ID")

    import crawler  # noqa: F401

    # normalize constants to make tests predictable
    monkeypatch.setattr(crawler, "PAGE_LIMIT", 3, raising=False)   # 3 rows/page
    monkeypatch.setattr(crawler, "MAX_PAGES", 100, raising=False)  # lots of pages
    monkeypatch.setattr(crawler, "TIMEOUT", 1, raising=False)

    return crawler
