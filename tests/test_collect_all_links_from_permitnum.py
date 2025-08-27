# --- drop-in replacement fixture: import your module by filepath ---
import os
import importlib.util
import builtins
import pytest

@pytest.fixture
def crawler_mod(monkeypatch):
    # Project root = parent of this tests/ folder
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    SRC_FILE = os.path.join(ROOT, "crawler.py")

    if not os.path.exists(SRC_FILE):
        raise FileNotFoundError(f"Couldn't find source file at: {SRC_FILE}")

    # Create a module spec from file path and execute it
    spec = importlib.util.spec_from_file_location("crawler", SRC_FILE)
    crawler = importlib.util.module_from_spec(spec)

    # If your module calls input() at import time, stub it so tests don't hang
    monkeypatch.setattr(builtins, "input", lambda *a, **k: "DUMMY_DATASET_ID")

    spec.loader.exec_module(crawler)  # <-- runs your crawler.py

    # Make pagination deterministic for tests
    monkeypatch.setattr(crawler, "PAGE_LIMIT", 3, raising=False)
    monkeypatch.setattr(crawler, "MAX_PAGES", 100, raising=False)
    return crawler



# 1) Happy path: full page (3) -> partial page (2) → stop
def test_happy_pagination_full_then_half(monkeypatch, crawler_mod):
    # Arrange: two pages of fake rows; then no more
    pages = [
        [{"permitnum": "P1"}, {"permitnum": "P2"}, {"permitnum": "P3"}],
        [{"permitnum": "P4"}, {"permitnum": "P5"}],
    ]
    calls = {"i": 0}  # count GET calls

    def fake_get(url, params=None, timeout=None):
        # Return page i; if asked again, return empty
        i = calls["i"]; calls["i"] += 1
        rows = pages[i] if i < len(pages) else []
        class R:
            def raise_for_status(self): pass
            def json(self): return rows
        return R()

    monkeypatch.setattr(crawler_mod.session, "get", fake_get)

    # Act
    links = crawler_mod.collect_all_links_from_permitnums()

    # Assert: 5 links from P1..P5, and only 2 GETs
    assert links == [
        f"{crawler_mod.LINK_HOST}?altId=P1",
        f"{crawler_mod.LINK_HOST}?altId=P2",
        f"{crawler_mod.LINK_HOST}?altId=P3",
        f"{crawler_mod.LINK_HOST}?altId=P4",
        f"{crawler_mod.LINK_HOST}?altId=P5",
    ]
    assert calls["i"] == 2


# 2) Empty first page → []
def test_empty_first_page_returns_empty(monkeypatch, crawler_mod):
    # Arrange: API always returns zero rows
    def fake_get(*a, **k):
        class R:
            def raise_for_status(self): pass
            def json(self): return []
        return R()
    monkeypatch.setattr(crawler_mod.session, "get", fake_get)

    # Act + Assert
    assert crawler_mod.collect_all_links_from_permitnums() == []


# 3) Full page → next page empty → stop after second call
def test_full_then_empty_second_page(monkeypatch, crawler_mod):
    # Arrange
    pages = [
        [{"permitnum": "A1"}, {"permitnum": "A2"}, {"permitnum": "A3"}],
        [],
    ]
    calls = {"i": 0}

    def fake_get(*a, **k):
        i = calls["i"]; calls["i"] += 1
        rows = pages[i] if i < len(pages) else []
        class R:
            def raise_for_status(self): pass
            def json(self): return rows
        return R()
    monkeypatch.setattr(crawler_mod.session, "get", fake_get)

    # Act
    links = crawler_mod.collect_all_links_from_permitnums()

    # Assert: only first page’s links; 2 GETs (full + empty)
    assert links == [
        f"{crawler_mod.LINK_HOST}?altId=A1",
        f"{crawler_mod.LINK_HOST}?altId=A2",
        f"{crawler_mod.LINK_HOST}?altId=A3",
    ]
    assert calls["i"] == 2


# 4) MAX_PAGES respected even if every page is “full”
def test_max_pages_respected(monkeypatch, crawler_mod):
    # Arrange: limit to 2 pages of size 3
    monkeypatch.setattr(crawler_mod, "PAGE_LIMIT", 3, raising=False)
    monkeypatch.setattr(crawler_mod, "MAX_PAGES", 2, raising=False)

    calls = {"n": 0}
    def fake_get(*a, **k):
        calls["n"] += 1
        # Always return a full page of 3 permitnums
        rows = [{"permitnum": f"P{calls['n']}-{i}"} for i in range(crawler_mod.PAGE_LIMIT)]
        class R:
            def raise_for_status(self): pass
            def json(self): return rows
        return R()
    monkeypatch.setattr(crawler_mod.session, "get", fake_get)

    # Act
    links = crawler_mod.collect_all_links_from_permitnums()

    # Assert: exactly 2 pages collected → 6 links total, 2 GETs
    assert len(links) == 2 * crawler_mod.PAGE_LIMIT
    assert calls["n"] == 2


# 5) Error handling: HTTP error on raise_for_status OR request timeout
@pytest.mark.parametrize("mode", ["http_error", "timeout"])
def test_http_error_or_timeout(monkeypatch, crawler_mod, mode):
    # Arrange: simulate the two failure modes
    if mode == "http_error":
        def fake_get(*a, **k):
            class R:
                def raise_for_status(self): raise requests.HTTPError("boom")
            return R()
    else:
        def fake_get(*a, **k): raise requests.Timeout("slow")
    monkeypatch.setattr(crawler_mod.session, "get", fake_get)

    # Act + Assert: function should stop and return what it has (here: [])
    assert crawler_mod.collect_all_links_from_permitnums() == []


# 6) Invalid JSON in response → []
def test_invalid_json_returns_empty(monkeypatch, crawler_mod):
    # Arrange: .json() raises ValueError
    def fake_get(*a, **k):
        class R:
            def raise_for_status(self): pass
            def json(self): raise ValueError("bad json")
        return R()
    monkeypatch.setattr(crawler_mod.session, "get", fake_get)

    # Act + Assert
    assert crawler_mod.collect_all_links_from_permitnums() == []


# 7) Link format check (?altId=<permitnum>)
def test_link_format_correct(monkeypatch, crawler_mod):
    # Arrange: single row with a known permitnum
    def fake_get(*a, **k):
        class R:
            def raise_for_status(self): pass
            def json(self): return [{"permitnum": "XYZ123"}]
        return R()
    monkeypatch.setattr(crawler_mod.session, "get", fake_get)

    # Act + Assert: verify exact URL format
    assert crawler_mod.collect_all_links_from_permitnums() == [
        f"{crawler_mod.LINK_HOST}?altId=XYZ123"
    ]


# 8) Ignore extra fields; only 'permitnum' is used
def test_ignores_extra_fields(monkeypatch, crawler_mod):
    # Arrange: row includes unrelated keys; should still build link from permitnum
    def fake_get(*a, **k):
        class R:
            def raise_for_status(self): pass
            def json(self): return [{"permitnum": "Q9", "foo": 1, "bar": "x"}]
        return R()
    monkeypatch.setattr(crawler_mod.session, "get", fake_get)

    # Act + Assert
    assert crawler_mod.collect_all_links_from_permitnums() == [
        f"{crawler_mod.LINK_HOST}?altId=Q9"
    ]
