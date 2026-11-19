"""State-judgment tests: late/duplicate/out-of-order heartbeats, point-in-time
reads, terminal states, rejection reasons, rule upgrades and crawl resumption.
"""
import sqlite3
from datetime import datetime, timezone

import pytest

from chargegrid import kernel
from chargegrid.state import (
    DUPLICATE_OBSERVATION,
    INVALID_TIMESTAMP,
    MISSING_PILE_ID,
    MISSING_TIMESTAMP,
    StateEngine,
    observation_id,
    parse_timestamp,
)

T0 = "2030-01-01T00:00:00Z"
T1 = "2030-01-01T01:00:00Z"
T2 = "2030-01-01T02:00:00Z"
T3 = "2030-01-01T03:00:00Z"


@pytest.fixture
def db():
    con = sqlite3.connect(":memory:")
    yield con
    con.close()


@pytest.fixture
def engine(db):
    return StateEngine(db, clock=datetime(2030, 1, 1, 4, 0, tzinfo=timezone.utc))


def ingest(engine, status, ts, pile="P1", **kw):
    return engine.ingest({"id": pile, "status": status, "ts": ts}, **kw)


# --------------------------------------------------------------------------- #
# Classification vocabulary.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("status,state", [
    ("active", kernel.AVAILABLE),
    ("online", kernel.AVAILABLE),
    ("ok", kernel.AVAILABLE),
    ("fault", kernel.FAULT),
    ("error", kernel.FAULT),
    ("offline", kernel.FAULT),
    ("stopped", kernel.STOPPED),
    ("decommissioned", kernel.STOPPED),
    ("retired", kernel.STOPPED),
    ("unknown", kernel.UNKNOWN),
    ("pending", kernel.UNKNOWN),
    ("draft", kernel.UNKNOWN),
    ("something-new", kernel.UNKNOWN),
    ("", kernel.UNKNOWN),
    (None, kernel.UNKNOWN),
])
def test_classification_covers_the_state_vocabulary(status, state):
    assert kernel.classify(status).state == state


def test_only_stopped_is_terminal():
    assert kernel.is_terminal(kernel.STOPPED)
    for state in (kernel.AVAILABLE, kernel.FAULT, kernel.UNKNOWN, kernel.UNOBSERVED):
        assert not kernel.is_terminal(state)


def test_unrecognized_status_explains_itself():
    c = kernel.classify("melted")
    assert c.state == kernel.UNKNOWN
    assert c.reason == "unrecognized_status"
    assert c.recognized is False
    assert kernel.classify("").reason == "missing_status"


# --------------------------------------------------------------------------- #
# Timestamp handling.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("raw", [
    "2030-01-01T00:00:00Z",
    "2030-01-01T00:00:00+00:00",
    "2030-01-01T01:00:00+01:00",
    "2030-01-01 00:00:00",
    "2030-01-01",
    1893456000,
    "1893456000",
])
def test_equivalent_timestamp_shapes_parse_to_the_same_instant(raw):
    assert parse_timestamp(raw) == datetime(2030, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("raw", ["", "   ", None, "not-a-date", True, {}])
def test_unparseable_timestamps_are_rejected(raw):
    assert parse_timestamp(raw) is None


# --------------------------------------------------------------------------- #
# Basic verdicts.
# --------------------------------------------------------------------------- #
def test_pile_without_heartbeat_is_unobserved_not_faulty(engine):
    v = engine.current("never-seen")
    assert v.state == kernel.UNOBSERVED
    assert v.reason == "no_observations"


def test_fault_is_distinguished_from_unknown_and_stopped(engine):
    ingest(engine, "fault", T1)
    assert engine.current("P1").state == kernel.FAULT

    ingest(engine, "unknown", T1, pile="P2")
    assert engine.current("P2").state == kernel.UNKNOWN

    ingest(engine, "retired", T1, pile="P3")
    v = engine.current("P3")
    assert v.state == kernel.STOPPED
    assert v.terminal is True


def test_unrecognized_status_stays_unknown_but_is_recorded(engine):
    result = ingest(engine, "melted", T1)
    assert result.accepted
    v = engine.current("P1")
    assert v.state == kernel.UNKNOWN
    assert v.reason == "unrecognized_status"


# --------------------------------------------------------------------------- #
# Duplicate / late / out-of-order heartbeats.
# --------------------------------------------------------------------------- #
def test_same_heartbeat_reported_twice_is_rejected_with_a_reason(engine):
    first = ingest(engine, "active", T1)
    second = ingest(engine, "active", T1)
    assert first.accepted
    assert not second.accepted
    assert second.reason == DUPLICATE_OBSERVATION
    assert second.state == kernel.AVAILABLE

    diag = engine.diagnostics("P1")["piles"]["P1"]
    assert diag["rejected"][0]["reason"] == DUPLICATE_OBSERVATION
    assert len(diag["observations"]) == 1


def test_duplicate_is_keyed_on_pile_event_time_and_status(engine):
    ingest(engine, "active", T1)
    # Same pile/time, different status: a genuine correction, not a duplicate.
    r = ingest(engine, "fault", T1)
    assert r.accepted
    assert engine.current("P1").state == kernel.FAULT


def test_late_heartbeat_does_not_rewrite_the_current_verdict(engine):
    ingest(engine, "fault", T2)
    late = ingest(engine, "active", T1)  # arrived after the fault, happened before
    assert late.accepted
    assert late.late is True
    assert engine.current("P1").state == kernel.FAULT


def test_out_of_order_arrival_produces_the_same_verdict_as_in_order(engine, db):
    unordered = StateEngine(db, clock=datetime(2030, 1, 1, 4, tzinfo=timezone.utc))
    for status, ts in [("fault", T2), ("active", T1), ("stopped", T3)]:
        unordered.ingest({"id": "P1", "status": status, "ts": ts})
    got = unordered.current("P1")

    other = sqlite3.connect(":memory:")
    ordered = StateEngine(other, clock=datetime(2030, 1, 1, 4, tzinfo=timezone.utc))
    for status, ts in [("active", T1), ("fault", T2), ("stopped", T3)]:
        ordered.ingest({"id": "P1", "status": status, "ts": ts})
    assert got.state == ordered.current("P1").state == kernel.STOPPED
    assert got.event_ts == ordered.current("P1").event_ts
    other.close()


def test_late_heartbeat_fills_in_the_past_without_changing_now(engine):
    ingest(engine, "fault", T2)
    ingest(engine, "active", T1)
    past = engine.verdict_at("P1", "2030-01-01T01:30:00Z")
    assert past.state == kernel.AVAILABLE
    assert engine.current("P1").state == kernel.FAULT


# --------------------------------------------------------------------------- #
# Point-in-time queries are pure reads.
# --------------------------------------------------------------------------- #
def test_past_query_does_not_mutate_current_state(engine, db):
    ingest(engine, "active", T1)
    ingest(engine, "fault", T3)
    before = engine.current("P1").to_dict()
    changes_before = db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]

    for at in (T0, T1, T2, T3):
        engine.verdict_at("P1", at)
        engine.history("P1", at=at)
        engine.diagnostics("P1", at=at)

    assert engine.current("P1").to_dict() == before
    assert db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == changes_before


def test_verdict_at_replays_only_heartbeats_up_to_the_instant(engine):
    ingest(engine, "active", T1)
    ingest(engine, "fault", T3)
    assert engine.verdict_at("P1", T0).state == kernel.UNOBSERVED
    assert engine.verdict_at("P1", T1).state == kernel.AVAILABLE
    assert engine.verdict_at("P1", T2).state == kernel.AVAILABLE
    assert engine.verdict_at("P1", T3).state == kernel.FAULT


def test_knowledge_time_view_hides_heartbeats_not_yet_recorded(engine):
    ingest(engine, "active", T1, recorded_at="2030-01-01T05:00:00Z")
    v = engine.verdict_at("P1", "2030-01-01T06:00:00Z",
                          known_at="2030-01-01T04:00:00Z")
    assert v.state == kernel.UNOBSERVED
    assert v.reason == "not_yet_known"
    asserted = engine.verdict_at("P1", "2030-01-01T06:00:00Z",
                                known_at="2030-01-01T05:30:00Z")
    assert asserted.state == kernel.AVAILABLE


def test_history_lists_only_effective_transitions(engine):
    ingest(engine, "active", T1)
    ingest(engine, "active", T2)   # identical state: no transition
    ingest(engine, "fault", T3)
    history = engine.history("P1")
    assert [(h["to_state"], h["event_ts"]) for h in history] == [
        (kernel.AVAILABLE, "2030-01-01T01:00:00.000000Z"),
        (kernel.FAULT, "2030-01-01T03:00:00.000000Z"),
    ]


# --------------------------------------------------------------------------- #
# Terminal states.
# --------------------------------------------------------------------------- #
def test_stopped_is_permanent(engine):
    ingest(engine, "active", T1)
    ingest(engine, "stopped", T2)
    ingest(engine, "active", T3)
    v = engine.current("P1")
    assert v.state == kernel.STOPPED
    assert v.terminal
    assert v.event_ts == "2030-01-01T02:00:00.000000Z"

    diag = {o["event_ts"]: o for o in engine.diagnostics("P1")["piles"]["P1"]["observations"]}
    assert diag["2030-01-01T03:00:00.000000Z"]["disposition"] == "ignored_after_terminal"


def test_stopped_history_remains_readable(engine):
    ingest(engine, "active", T1)
    ingest(engine, "stopped", T2)
    assert engine.verdict_at("P1", "2030-01-01T01:30:00Z").state == kernel.AVAILABLE
    assert engine.verdict_at("P1", T3).state == kernel.STOPPED


def test_late_retirement_marker_corrects_contradicting_later_heartbeats(engine):
    """A permanent-stop heartbeat that arrives late still wins over later
    'available' reports: those reports describe a pile that no longer exists."""
    ingest(engine, "active", T1)
    ingest(engine, "active", T2)
    assert engine.current("P1").state == kernel.AVAILABLE

    late = ingest(engine, "stopped", "2030-01-01T00:30:00Z")
    assert late.late is True
    assert engine.current("P1").state == kernel.STOPPED
    diag = {o["event_ts"]: o for o in engine.diagnostics("P1")["piles"]["P1"]["observations"]}
    assert diag["2030-01-01T01:00:00.000000Z"]["disposition"] == "ignored_after_terminal"
    # Before the retirement instant nothing had been observed yet.
    assert engine.verdict_at("P1", "2030-01-01T00:15:00Z").state == kernel.UNOBSERVED
    assert engine.verdict_at("P1", "2030-01-01T00:45:00Z").state == kernel.STOPPED


# --------------------------------------------------------------------------- #
# Freshness: silence is unobserved, never a fault.
# --------------------------------------------------------------------------- #
def test_stale_available_heartbeat_degrades_to_unobserved_not_fault(db):
    engine = StateEngine(db, freshness=3600,
                         clock=datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc))
    ingest(engine, "active", T1)
    v = engine.current("P1")
    assert v.state == kernel.UNOBSERVED
    assert v.stale is True
    assert v.reason == "stale_heartbeat"
    # Point-in-time inside the freshness window still says available.
    assert engine.verdict_at("P1", "2030-01-01T01:30:00Z").state == kernel.AVAILABLE


def test_stale_fault_is_still_a_fault(db):
    engine = StateEngine(db, freshness=60,
                         clock=datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc))
    ingest(engine, "fault", T1)
    assert engine.current("P1").state == kernel.FAULT


# --------------------------------------------------------------------------- #
# Rejections are explainable.
# --------------------------------------------------------------------------- #
def test_missing_pile_id_is_rejected_with_reason(engine):
    r = engine.ingest({"status": "active", "ts": T1})
    assert not r.accepted
    assert r.reason == MISSING_PILE_ID
    assert engine.diagnostics("P1")["piles"]["P1"]["rejected"] == []
    unattributed = engine.diagnostics()["unattributed_rejections"]
    assert [row["reason"] for row in unattributed] == [MISSING_PILE_ID]


def test_missing_and_invalid_timestamps_are_distinguished(engine):
    missing = engine.ingest({"id": "P1", "status": "active", "ts": ""})
    invalid = engine.ingest({"id": "P1", "status": "active", "ts": "yesterday"})
    assert missing.reason == MISSING_TIMESTAMP
    assert invalid.reason == INVALID_TIMESTAMP
    diag = engine.diagnostics("P1")["piles"]["P1"]
    assert {r["reason"] for r in diag["rejected"]} == {MISSING_TIMESTAMP, INVALID_TIMESTAMP}
    assert diag["verdict"]["state"] == kernel.UNOBSERVED


def test_rejection_keeps_the_original_payload_for_audit(engine):
    engine.ingest({"id": "P1", "status": "active", "ts": "bad"})
    row = engine.diagnostics("P1")["piles"]["P1"]["rejected"][0]
    assert row["payload"] == {"id": "P1", "status": "active", "ts": "bad"}


# --------------------------------------------------------------------------- #
# Diagnostics name the deciding and ignored observations.
# --------------------------------------------------------------------------- #
def test_diagnostics_name_the_observation_that_changed_state(engine):
    ingest(engine, "active", T1)
    ingest(engine, "fault", T2)
    diag = engine.diagnostics("P1")["piles"]["P1"]
    assert diag["verdict"]["observation_id"] == observation_id(
        "P1", "2030-01-01T02:00:00.000000Z", "fault")
    by_ts = {o["event_ts"]: o for o in diag["observations"]}
    assert by_ts["2030-01-01T02:00:00.000000Z"]["disposition"] == "applied"
    assert by_ts["2030-01-01T01:00:00.000000Z"]["disposition"] == "applied_then_superseded"
    assert [c["observation_id"] for c in diag["changes"]] == [
        observation_id("P1", "2030-01-01T01:00:00.000000Z", "active"),
        observation_id("P1", "2030-01-01T02:00:00.000000Z", "fault"),
    ]


def test_conflicting_reports_at_the_same_instant_prefer_the_worse_one(engine):
    ingest(engine, "active", T1)
    ingest(engine, "fault", T1)
    assert engine.current("P1").state == kernel.FAULT
    diag = engine.diagnostics("P1")["piles"]["P1"]
    dispositions = {o["status"]: o["disposition"] for o in diag["observations"]}
    assert dispositions["fault"] == "applied"
    assert dispositions["active"] == "conflict_not_adopted"


# --------------------------------------------------------------------------- #
# Rule upgrades keep history readable.
# --------------------------------------------------------------------------- #
def test_rule_update_reclassifies_history_without_rewriting_it(engine, monkeypatch):
    ingest(engine, "melted", T1)            # unknown under the original rules
    assert engine.current("P1").state == kernel.UNKNOWN

    monkeypatch.setitem(kernel.STATUS_STATE, "melted", kernel.FAULT)
    v = engine.current("P1")
    assert v.state == kernel.FAULT
    assert v.stored_state == kernel.UNKNOWN
    assert kernel.classify("melted").reason == "status_melted"
    # And the original verdict is still reachable at an earlier rule version.
    diag = engine.diagnostics("P1")["piles"]["P1"]["observations"][0]
    assert diag["stored_state"] == kernel.UNKNOWN
    assert diag["state"] == kernel.FAULT


def test_legacy_record_table_can_be_backfilled(db):
    db.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, id TEXT,"
               " ts TEXT, payload TEXT NOT NULL)")
    db.execute("INSERT INTO records VALUES('h1','P1',?,'{\"id\":\"P1\","
               "\"status\":\"active\",\"ts\":\"%s\"}')" % T1, (T1,))
    db.commit()
    engine = StateEngine(db, clock=datetime(2030, 1, 1, 4, tzinfo=timezone.utc))
    stats = engine.backfill_from_records()
    assert stats["imported"] == 1
    assert engine.current("P1").state == kernel.AVAILABLE
    again = engine.backfill_from_records()
    assert again["imported"] == 0 and again["skipped"] == 1
