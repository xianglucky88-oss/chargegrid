"""Command-line surface: current state, point-in-time reads, diagnostics."""
import json
import sqlite3

import pytest

from chargegrid import cli, pipeline
from chargegrid.state import StateEngine

T1 = "2030-01-01T00:00:00Z"
T2 = "2030-01-01T01:00:00Z"
T3 = "2030-01-01T02:00:00Z"


@pytest.fixture
def db(tmp_path, monkeypatch):
    import chargegrid.config as config
    path = tmp_path / "chargegrid.db"
    monkeypatch.setattr(config, "DB_PATH", path)
    con = sqlite3.connect(path)
    engine = StateEngine(con, clock="2030-01-01T03:00:00Z")
    engine.ingest({"id": "P1", "status": "active", "ts": T1})
    engine.ingest({"id": "P1", "status": "fault", "ts": T2})
    engine.ingest({"id": "P2", "status": "stopped", "ts": T1})
    engine.ingest({"id": "P1", "status": "active", "ts": "not-a-date"})
    con.commit()
    yield con
    con.close()


def run_cli(capsys, *argv):
    assert cli.main(list(argv)) == 0
    return json.loads(capsys.readouterr().out)


def test_current_lists_every_pile(db, capsys):
    out = run_cli(capsys, "current")
    assert out["P1"]["state"] == "fault"
    assert out["P2"]["state"] == "stopped"


def test_state_at_is_a_pure_read(db, capsys):
    def snapshot():
        return {pile: (v["state"], v["observation_id"], v["event_ts"])
                for pile, v in pipeline.current().items()}
    before = snapshot()
    out = run_cli(capsys, "state-at", "--pile", "P1", "--at", T1)
    assert out["state"] == "available"
    assert snapshot() == before


def test_history_shows_transitions(db, capsys):
    out = run_cli(capsys, "history", "--pile", "P1")
    assert [step["to_state"] for step in out] == ["available", "fault"]


def test_diagnose_names_adopted_and_rejected_observations(db, capsys):
    out = run_cli(capsys, "diagnose", "--pile", "P1")
    pile = out["piles"]["P1"]
    assert pile["verdict"]["observation_id"]
    assert [o["disposition"] for o in pile["observations"]] == [
        "applied_then_superseded", "applied"]
    assert pile["rejected"][0]["reason"] == "invalid_timestamp"


def test_options_work_before_or_after_the_subcommand(db, capsys):
    after = run_cli(capsys, "state-at", "--pile", "P1", "--at", T1)
    before = run_cli(capsys, "--pile", "P1", "--at", T1, "state-at")
    assert after == before


def test_missing_required_option_is_a_usage_error(db, capsys):
    with pytest.raises(SystemExit):
        cli.main(["state-at", "--pile", "P1"])  # no --at
    with pytest.raises(SystemExit):
        cli.main(["history"])                   # no --pile
