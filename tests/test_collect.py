"""End-to-end tests: collecting heartbeats from the local fixture.

These weave together the scenarios the operator cares about — a pile
re-reporting itself, heartbeats that arrive out of order, a crawl that breaks
between pages, and a resumed run — and compare the verdicts reached under
different collection scopes.
"""
import importlib
import sqlite3

import pytest

from chargegrid import framework
from chargegrid.state import StateEngine


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    """A fresh database per test and a freshly imported, empty fixture app."""
    import chargegrid.config as config
    db_path = tmp_path / "chargegrid.db"
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(framework, "SPEC_ROOT", lambda: config.ROOT)
    mockapp = importlib.reload(importlib.import_module("mockapp.main"))
    yield mockapp, db_path
    del mockapp


def connect(db_path):
    con = sqlite3.connect(db_path)
    return con, StateEngine(con)


def reset(con):
    """Wipe every table so a test starts from a genuinely empty database."""
    StateEngine(con)  # ensure schema exists
    tables = {row[0] for row in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    for table in ("observations", "rejected_observations", "records", "meta"):
        if table in tables:
            con.execute("DELETE FROM %s" % table)
    con.commit()


def states_of(result):
    return {pile: verdict["state"] for pile, verdict in result["states"].items()}


# --------------------------------------------------------------------------- #
# A complete crawl.
# --------------------------------------------------------------------------- #
def test_full_crawl_records_every_heartbeat_and_names_the_changes(isolated):
    _, db_path = isolated
    con = sqlite3.connect(db_path)
    try:
        result = framework.run(con=con)
        engine = StateEngine(con)
        observations = con.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    finally:
        con.close()

    assert result["complete"] is True
    assert result["next_page"] is None
    assert result["pages"] == 3
    assert observations == result["observations_accepted"]
    # The fixture repeats its first row on a later page: same pile, same
    # event time, same status, so it is rejected with an explanation.
    assert result["duplicate_observations"] == 1
    assert [r["reason"] for r in result["rejections"]] == ["duplicate_observation"]
    assert result["state_changes"] == len(result["verdict_changes"]) > 0
    for pile, verdict in result["states"].items():
        assert verdict["state"] in {"available", "fault", "unknown", "stopped", "unobserved"}
        assert verdict["observation_id"] or verdict["state"] == "unobserved"


def test_repeated_reports_of_the_same_pile_are_not_duplicated(isolated):
    _, db_path = isolated
    con = sqlite3.connect(db_path)
    try:
        first = framework.run(con=con)
        second = framework.run(con=con)
        total = con.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    finally:
        con.close()

    assert second["observations_accepted"] == 0
    assert second["duplicate_observations"] == first["fetched"]
    assert second["state_changes"] == 0
    assert total == first["observations_accepted"]
    assert states_of(second) == states_of(first)


# --------------------------------------------------------------------------- #
# Out-of-order heartbeats.
# --------------------------------------------------------------------------- #
def test_late_heartbeat_does_not_flip_a_newer_verdict(isolated):
    _, db_path = isolated
    con = sqlite3.connect(db_path)
    try:
        engine = StateEngine(con)
        engine.ingest({"id": "ODD-1", "status": "fault", "ts": "2030-02-02T00:00:00Z"})
        late = engine.ingest({"id": "ODD-1", "status": "active", "ts": "2030-02-01T00:00:00Z"})
        current = engine.current("ODD-1")
        past = engine.verdict_at("ODD-1", "2030-02-01T12:00:00Z")
    finally:
        con.close()

    assert late.accepted and late.late is True
    assert late.disposition == "applied_then_superseded"
    assert current.state == "fault"
    assert past.state == "available"


def test_out_of_order_heartbeats_are_order_independent(isolated):
    _, db_path = isolated
    rows = [
        {"id": "OOO-1", "status": "active", "ts": "2030-03-01T00:00:00Z"},
        {"id": "OOO-1", "status": "fault", "ts": "2030-03-02T00:00:00Z"},
        {"id": "OOO-1", "status": "unknown", "ts": "2030-03-03T00:00:00Z"},
    ]

    def verdict(order):
        con = sqlite3.connect(db_path)
        try:
            reset(con)
            engine = StateEngine(con, clock="2030-03-04T00:00:00Z")
            for row in order:
                engine.ingest(dict(row))
            v = engine.current("OOO-1")
            return v.state, v.reason, v.event_ts, v.observation_id
        finally:
            con.close()

    seen = [verdict(rows), verdict(list(reversed(rows))),
            verdict([rows[1], rows[0], rows[2]])]
    assert seen[0] == seen[1] == seen[2]
    assert seen[0][0] == "unknown"


# --------------------------------------------------------------------------- #
# Interrupted and resumed crawls.
# --------------------------------------------------------------------------- #
def test_interrupted_crawl_keeps_what_it_saw_and_reports_where_to_resume(isolated):
    _, db_path = isolated
    con = sqlite3.connect(db_path)
    try:
        partial = framework.run(con=con, max_pages=2)
        engine = StateEngine(con)
        # Only the heartbeats from the two pages that were actually read.
        assert partial["pages"] == 2
        assert partial["complete"] is False
        assert partial["next_page"] == 3
        assert partial["observations_accepted"] == partial["fetched"] == 8
        assert len(engine.all_current()) == 8
    finally:
        con.close()


def test_failed_page_mid_crawl_keeps_earlier_pages_and_resumes(isolated, monkeypatch):
    """A page that keeps failing stops the crawl but does not lose history."""
    _, db_path = isolated
    real_request = framework.Transport.request
    failing = {"page": 2}

    def flaky(self, method, path, params=None, json_body=None, headers=None):
        if params and str(params.get("page")) == str(failing["page"]):
            return 503, {}, "unavailable"
        return real_request(self, method, path, params, json_body, headers)

    monkeypatch.setattr(framework.Transport, "request", flaky)
    con = sqlite3.connect(db_path)
    try:
        partial = framework.run(con=con)
        engine = StateEngine(con)
        assert partial["interrupted"] is True
        assert partial["interrupt_status"] == 503
        assert partial["complete"] is False
        assert partial["pages"] == 1
        assert partial["next_page"] == 2
        assert partial["observations_accepted"] == 4
        assert len(engine.all_current()) == 4

        # The source recovers; the resumed crawl picks up exactly where the
        # failure left off, without re-recording page 1.
        failing["page"] = None
        resumed = framework.run(con=con, start_page=partial["next_page"])
        assert resumed["complete"] is True
        assert resumed["observations_accepted"] == 5
        # The only duplicate is the fixture's own repeated row on the last
        # page; the resumed run did not re-read page 1.
        assert resumed["duplicate_observations"] == 1
        assert len(engine.all_current()) == 9
    finally:
        con.close()


def test_resuming_a_crawl_matches_one_complete_crawl(isolated):
    _, db_path = isolated
    con = sqlite3.connect(db_path)
    try:
        whole = framework.run(con=con)
        whole_states = states_of(whole)
        reset(con)

        first = framework.run(con=con, max_pages=2)
        second = framework.run(con=con, start_page=first["next_page"])
        engine = StateEngine(con)
        resumed_states = {pile: v.state for pile, v in engine.all_current().items()}
        observations = con.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    finally:
        con.close()

    assert second["complete"] is True
    assert first["observations_accepted"] + second["observations_accepted"] == \
        whole["observations_accepted"]
    assert resumed_states == whole_states
    assert observations == whole["observations_accepted"]


def test_resuming_after_a_lower_of_two_different_ranges(isolated):
    """A one-page range and a two-page range differ, but neither corrupts state."""
    _, db_path = isolated
    con = sqlite3.connect(db_path)
    try:
        one = framework.run(con=con, max_pages=1)
        two = framework.run(con=con, start_page=2, max_pages=1)
        engine = StateEngine(con)
        covered = dict(one["states"])
        covered.update(two["states"])
        current = {pile: v.state for pile, v in engine.all_current().items()}
    finally:
        con.close()

    assert one["fetched"] == 4
    assert two["fetched"] == 4
    # The second range adds piles the first could not have known about.
    assert len(current) == 8
    assert set(covered) == set(current)


# --------------------------------------------------------------------------- #
# Scope comparison.
# --------------------------------------------------------------------------- #
def test_all_collection_ranges_agree_on_piles_they_share(isolated):
    _, db_path = isolated
    con = sqlite3.connect(db_path)
    try:
        full = framework.run(con=con)
        engine = StateEngine(con)
        for start, pages in ((1, 1), (2, 1), (1, 2), (3, 1)):
            reset(con)
            chunk = framework.run(con=con, start_page=start, max_pages=pages)
            for pile, verdict in chunk["states"].items():
                assert verdict["state"] == full["states"][pile]["state"]
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# The RPA engine shares the same judgment path.
# --------------------------------------------------------------------------- #
def test_rpa_engine_records_observations(isolated, monkeypatch):
    import chargegrid.config as config
    monkeypatch.setattr(config.SPEC, "engine", "rpa")
    _, db_path = isolated
    con = sqlite3.connect(db_path)
    try:
        result = framework.run_rpa(con=con)
        engine = StateEngine(con, clock="2030-01-06T00:00:00Z")
        summary = {pile: v.state for pile, v in engine.all_current().items()}
    finally:
        con.close()
    assert result["observations_accepted"] + result["observations_rejected"] == \
        result["collected"]
    assert {pile: v["state"] for pile, v in result["states"].items()} == summary


# --------------------------------------------------------------------------- #
# Everything at once.
# --------------------------------------------------------------------------- #
def test_duplicate_oo_order_interruption_and_resume_interleaved(isolated):
    """Interleave all four phenomena and compare scopes.

    * page 1 is read normally (and contains a pile the fixture re-reports
      later, plus one pile that is permanently stopped),
    * a page-2 failure interrupts the crawl,
    * a late, older heartbeat for a pile already seen arrives afterwards,
    * the crawl resumes from where it broke,
    * the whole source is then read again from the start.

    The final verdict must equal the verdict of one clean complete crawl.
    """
    _, db_path = isolated
    real_request = framework.Transport.request
    failing = {"page": 2}

    def flaky(self, method, path, params=None, json_body=None, headers=None):
        if params and str(params.get("page")) == str(failing["page"]):
            return 503, {}, "unavailable"
        return real_request(self, method, path, params, json_body, headers)

    # Baseline: one clean complete crawl.
    con = sqlite3.connect(db_path)
    try:
        baseline = framework.run(con=con)
        reset(con)
    finally:
        con.close()

    import unittest.mock as mock
    with mock.patch.object(framework.Transport, "request", flaky):
        con = sqlite3.connect(db_path)
        try:
            first = framework.run(con=con)
            assert first["interrupted"] and first["next_page"] == 2

            engine = StateEngine(con)
            seen_pile = sorted(first["states"])[0]
            late = engine.ingest({"id": seen_pile, "status": "fault",
                                  "ts": "2029-12-31T00:00:00Z"})
            assert late.accepted and late.late is True
            assert late.disposition in ("applied_then_superseded",
                                        "confirmed_then_superseded")
            assert {p: v.state for p, v in engine.all_current().items()} == \
                {p: v["state"] for p, v in first["states"].items()}

            failing["page"] = None
            second = framework.run(con=con, start_page=first["next_page"])
            assert second["complete"] is True

            third = framework.run(con=con)  # whole source again
            assert third["observations_accepted"] == 0
            assert third["state_changes"] == 0

            final = {p: v.state for p, v in StateEngine(con).all_current().items()}
        finally:
            con.close()

    expected = {p: v["state"] for p, v in baseline["states"].items()}
    assert final == expected
    # The late heartbeat did not rewrite any present verdict.
    assert final[seen_pile] == baseline["states"][seen_pile]["state"]
