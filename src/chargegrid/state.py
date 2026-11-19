"""Event-sourced state judgment for charging-pile heartbeats.

Design notes
------------
* Observations are append-only.  Re-ingesting the same observation is
  rejected (with an explanation) instead of overwriting history, so restoring
  an old database snapshot or re-running a partially completed crawl is safe.
* Every observation carries an *event* timestamp (when the heartbeat was
  taken) and a *record* timestamp (when we learned about it).  The current
  verdict is replayed in event-time order, which makes late and out-of-order
  heartbeats harmless: an old heartbeat can fill in the past without
  rewriting the present.
* Point-in-time queries are pure reads.  ``verdict_at`` replays the event log
  up to the requested instant and never writes anything, so asking about the
  past cannot change the current conclusion.
* A heartbeat that is simply missing does not imply a fault.  It yields
  ``unobserved``; a heartbeat that arrives but cannot be interpreted yields
  ``unknown``; an explicit failure yields ``fault``; a retirement marker
  yields ``stopped`` and is terminal.
* Rules are versioned.  The classification that was stored when the
  observation was ingested is kept for audit, but every query re-derives the
  state from the raw ``status`` with the current rules, which keeps history
  readable after a rule update.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from . import kernel
from .config import STATE_FRESHNESS_SECONDS


# --------------------------------------------------------------------------- #
# Timestamps.
# --------------------------------------------------------------------------- #
_NUMERIC = re.compile(r"[+-]?\d+(?:\.\d+)?\Z")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """Canonical UTC representation: fixed width, lexicographically sorted."""
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_timestamp(value) -> datetime | None:
    """Parse the heartbeat timestamp shapes seen in the wild.

    Accepts ISO-8601 (with ``Z`` or an offset), naive datetimes (treated as
    UTC), common ``YYYY-mm-dd[ HH:MM:SS]`` forms, and epoch seconds.  Returns
    ``None`` for anything that cannot be understood.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None \
            else value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        return _from_epoch(value)
    text = str(value).strip()
    if not text:
        return None
    if _NUMERIC.match(text):
        try:
            return _from_epoch(float(text))
        except ValueError:
            return None
    raw = text[:-1] + "+00:00" if text[-1] in ("Z", "z") else text
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _from_epoch(value) -> datetime | None:
    try:
        return datetime.fromtimestamp(float(value), timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Rejection reasons.
# --------------------------------------------------------------------------- #
MISSING_PILE_ID = "missing_pile_id"
MISSING_TIMESTAMP = "missing_timestamp"
INVALID_TIMESTAMP = "invalid_timestamp"
DUPLICATE_OBSERVATION = "duplicate_observation"


# --------------------------------------------------------------------------- #
# Value objects.
# --------------------------------------------------------------------------- #
@dataclass
class IngestResult:
    accepted: bool
    reason: str
    pile_id: str
    observation_id: str | None = None
    status: str | None = None
    event_ts: str | None = None
    recorded_at: str | None = None
    state: str = kernel.UNOBSERVED
    previous_state: str = kernel.UNOBSERVED
    changed: bool = False
    late: bool = False
    disposition: str = "rejected"
    rules_version: str = kernel.RULES_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "reason": self.reason,
            "pile_id": self.pile_id,
            "observation_id": self.observation_id,
            "status": self.status,
            "event_ts": self.event_ts,
            "recorded_at": self.recorded_at,
            "state": self.state,
            "previous_state": self.previous_state,
            "changed": self.changed,
            "late": self.late,
            "disposition": self.disposition,
            "rules_version": self.rules_version,
        }


@dataclass
class Verdict:
    pile_id: str
    state: str
    reason: str
    as_of: str | None = None
    known_at: str | None = None
    event_ts: str | None = None
    recorded_at: str | None = None
    observed_at: str | None = None
    age_seconds: float | None = None
    stale: bool = False
    terminal: bool = False
    observation_id: str | None = None
    status: str | None = None
    rules_version: str = kernel.RULES_VERSION
    stored_state: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pile_id": self.pile_id,
            "state": self.state,
            "reason": self.reason,
            "as_of": self.as_of,
            "known_at": self.known_at,
            "event_ts": self.event_ts,
            "recorded_at": self.recorded_at,
            "observed_at": self.observed_at,
            "age_seconds": None if self.age_seconds is None else round(self.age_seconds, 3),
            "stale": self.stale,
            "terminal": self.terminal,
            "observation_id": self.observation_id,
            "status": self.status,
            "rules_version": self.rules_version,
            "stored_state": self.stored_state,
        }


@dataclass
class _Obs:
    id: int
    obs_id: str
    pile_id: str
    status: str | None
    event_ts: str
    recorded_at: str
    source: str | None
    payload: dict[str, Any]
    stored_state: str
    stored_reason: str
    rules_version: str
    event_dt: datetime = field(default=None)  # type: ignore[assignment]
    recorded_dt: datetime = field(default=None)  # type: ignore[assignment]

    @property
    def classification(self) -> kernel.Classification:
        # Re-derive with the current rules so a rule update is reflected in
        # history without rewriting the stored observation.
        return kernel.classify(self.status)

    @property
    def state(self) -> str:
        return self.classification.state


@dataclass
class _Resolved:
    verdict: Verdict
    observations: list[_Obs]
    dispositions: dict[int, tuple[str, str | None]]
    changes: list[dict[str, Any]]


# --------------------------------------------------------------------------- #
# Engine.
# --------------------------------------------------------------------------- #
class StateEngine:
    """Append-only observation log plus deterministic replay."""

    def __init__(self, con, freshness: timedelta | float | None = None,
                 rules_version: str | None = None, clock=None):
        self.con = con
        self.freshness = _coerce_freshness(freshness)
        self.rules_version = rules_version or kernel.RULES_VERSION
        self._clock = _coerce_clock(clock)
        self._init()

    # -- schema ----------------------------------------------------------- #
    def _init(self):
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS observations("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " obs_id TEXT NOT NULL UNIQUE,"
            " pile_id TEXT NOT NULL,"
            " status TEXT,"
            " event_ts TEXT NOT NULL,"
            " recorded_at TEXT NOT NULL,"
            " source TEXT,"
            " payload TEXT NOT NULL,"
            " stored_state TEXT NOT NULL,"
            " stored_reason TEXT NOT NULL,"
            " rules_version TEXT NOT NULL)")
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_observations_pile_ts"
            " ON observations(pile_id, event_ts)")
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_observations_recorded"
            " ON observations(recorded_at)")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS rejected_observations("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " pile_id TEXT,"
            " obs_id TEXT,"
            " event_ts TEXT,"
            " recorded_at TEXT NOT NULL,"
            " reason TEXT NOT NULL,"
            " detail TEXT,"
            " payload TEXT NOT NULL)")
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_rejected_pile"
            " ON rejected_observations(pile_id)")
        self.con.commit()

    # -- ingestion -------------------------------------------------------- #
    def ingest(self, record: dict[str, Any], source: str = "crawl",
               recorded_at: datetime | str | None = None,
               detail: str | None = None) -> IngestResult:
        """Validate and append one observation.

        The record is rejected when it cannot identify a pile or a moment in
        time, or when the identical observation is already stored.  Rejected
        observations are logged with a reason so an operator can explain why
        a heartbeat was not adopted.
        """
        record = dict(record)
        pile_id = str(record.get("id", "") if record.get("id") is not None else "").strip()
        status_raw = record.get("status")
        status = "" if status_raw is None else str(status_raw).strip()
        recorded_dt = self._coerce_recorded(recorded_at)
        recorded_iso = iso(recorded_dt)

        if not pile_id:
            return self._reject(MISSING_PILE_ID, None, None, None, recorded_iso,
                                record, detail or "record has no pile id")

        raw_ts = record.get("ts")
        event_dt = parse_timestamp(raw_ts)
        if event_dt is None:
            reason = MISSING_TIMESTAMP if raw_ts in (None, "") else INVALID_TIMESTAMP
            return self._reject(reason, pile_id, None, None, recorded_iso, record,
                                detail or ("missing event timestamp"
                                           if reason == MISSING_TIMESTAMP
                                           else "event timestamp is not parseable"))
        event_iso = iso(event_dt)
        obs_id = observation_id(pile_id, event_iso, status)

        existing = self.con.execute(
            "SELECT id FROM observations WHERE obs_id=?", (obs_id,)).fetchone()
        if existing is not None:
            result = self._reject(
                DUPLICATE_OBSERVATION, pile_id, obs_id, event_iso, recorded_iso, record,
                detail or "same pile, event time and status already recorded")
            current = self.current(pile_id)
            result.state = current.state
            result.previous_state = current.state
            result.status = status or None
            result.event_ts = event_iso
            return result

        classification = kernel.classify(status_raw)
        newest = self.con.execute(
            "SELECT MAX(event_ts) FROM observations WHERE pile_id=?", (pile_id,)).fetchone()[0]
        newest_dt = parse_timestamp(newest)
        late = newest_dt is not None and event_dt < newest_dt

        previous = self.current(pile_id)
        payload = _json_safe(record)
        try:
            cur = self.con.execute(
                "INSERT INTO observations"
                "(obs_id,pile_id,status,event_ts,recorded_at,source,payload,"
                " stored_state,stored_reason,rules_version)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (obs_id, pile_id, status or None, event_iso, recorded_iso, source,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 classification.state, classification.reason, self.rules_version))
            self.con.commit()
        except Exception:
            # A concurrent writer may have inserted the same observation.
            self.con.rollback()
            current = self.current(pile_id)
            return IngestResult(False, DUPLICATE_OBSERVATION, pile_id, obs_id,
                                status or None, event_iso, recorded_iso,
                                state=current.state, previous_state=current.state,
                                disposition="rejected")

        resolved = self._resolve(pile_id, self._reference(), None, None)
        current = resolved.verdict
        disposition, _note = resolved.dispositions.get(cur.lastrowid, ("recorded", None))
        return IngestResult(
            True, classification.reason, pile_id, obs_id, status or None,
            event_iso, recorded_iso, state=current.state,
            previous_state=previous.state,
            changed=current.state != previous.state,
            late=late, disposition=disposition,
            rules_version=self.rules_version)

    def _reject(self, reason, pile_id, obs_id, event_iso, recorded_iso, payload,
                detail) -> IngestResult:
        self.con.execute(
            "INSERT INTO rejected_observations"
            "(pile_id,obs_id,event_ts,recorded_at,reason,detail,payload)"
            " VALUES(?,?,?,?,?,?,?)",
            (pile_id, obs_id, event_iso, recorded_iso, reason, detail,
             json.dumps(_json_safe(payload), ensure_ascii=False, sort_keys=True)))
        self.con.commit()
        return IngestResult(False, reason, pile_id or "", obs_id, None, event_iso,
                            recorded_iso, state=kernel.UNOBSERVED,
                            previous_state=kernel.UNOBSERVED,
                            disposition="rejected")

    def _coerce_recorded(self, recorded_at) -> datetime:
        if recorded_at is None:
            return self._clock()
        if isinstance(recorded_at, datetime):
            return recorded_at if recorded_at.tzinfo else recorded_at.replace(tzinfo=timezone.utc)
        parsed = parse_timestamp(recorded_at)
        if parsed is None:
            raise ValueError("recorded_at is not a timestamp: %r" % (recorded_at,))
        return parsed

    # -- migration -------------------------------------------------------- #
    def is_empty(self) -> bool:
        return self.con.execute(
            "SELECT 1 FROM observations LIMIT 1").fetchone() is None

    def _get_meta(self, key: str):
        try:
            row = self.con.execute(
                "SELECT v FROM meta WHERE k=?", (key,)).fetchone()
        except Exception:
            return None
        return row[0] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value))
        self.con.commit()

    def backfill_from_records(self, table: str = "records") -> dict[str, int]:
        """Import legacy payloads from the record table into the event log.

        Older databases (or a snapshot restored from one) only contain the
        deduplicated ``records`` table.  Their payloads already carry the
        original heartbeat ``ts``, so replaying them reconstructs the same
        history.  The operation is idempotent: a ``backfill_done`` marker
        keeps a repeated run from re-logging rejections, and ``obs_id``
        rejects anything already present.
        """
        if self._get_meta("backfill_done") is not None:
            return {"imported": 0, "duplicates": 0, "rejected": 0, "skipped": 1}
        try:
            rows = self.con.execute(
                "SELECT payload FROM %s ORDER BY ts, content_hash" % table).fetchall()
        except Exception:
            return {"imported": 0, "duplicates": 0, "rejected": 0, "skipped": 0}
        imported = duplicates = rejected = 0
        for (payload,) in rows:
            try:
                record = json.loads(payload)
            except (TypeError, ValueError):
                rejected += 1
                continue
            recorded_at = None
            if isinstance(record, dict):
                recorded_at = record.get("recorded_at") or record.get("ingested_at")
            result = self.ingest(record if isinstance(record, dict) else {},
                                 source="backfill", recorded_at=recorded_at)
            if result.accepted:
                imported += 1
            elif result.reason == DUPLICATE_OBSERVATION:
                duplicates += 1
            else:
                rejected += 1
        self._set_meta("backfill_done", iso(self._clock()))
        return {"imported": imported, "duplicates": duplicates, "rejected": rejected,
                "skipped": 0}

    # -- queries ---------------------------------------------------------- #
    def current(self, pile_id: str, now: datetime | str | None = None) -> Verdict:
        """Verdict based on everything recorded so far.

        ``now`` overrides the wall clock for freshness; by default the clock is
        the reference instant, so a future-dated heartbeat simply shows a
        negative age instead of silently moving the reference.
        """
        ref = self._reference(now)
        return self._resolve(pile_id, ref, None, None).verdict

    def verdict_at(self, pile_id: str, at: datetime | str,
                   known_at: datetime | str | None = None) -> Verdict:
        """State in effect at ``at``, computed as a pure read.

        ``known_at`` optionally restricts the replay to observations that had
        already been recorded by that instant (knowledge-time view).  Calling
        this function never writes to the database.
        """
        at_dt = self._require_time(at, "at")
        known_dt = None if known_at is None else self._require_time(known_at, "known_at")
        return self._resolve(pile_id, at_dt, at_dt, known_dt).verdict

    def history(self, pile_id: str, at: datetime | str | None = None,
                known_at: datetime | str | None = None) -> list[dict[str, Any]]:
        at_dt = None if at is None else self._require_time(at, "at")
        known_dt = None if known_at is None else self._require_time(known_at, "known_at")
        ref = at_dt if at_dt is not None else self._reference()
        return self._resolve(pile_id, ref, at_dt, known_dt).changes

    def all_current(self, now: datetime | str | None = None) -> dict[str, Verdict]:
        ref = self._reference(now)
        return {pile: self._resolve(pile, ref, None, None).verdict
                for pile in self._pile_ids()}

    def diagnostics(self, pile_id: str | None = None,
                    at: datetime | str | None = None,
                    known_at: datetime | str | None = None) -> dict[str, Any]:
        """Explain the verdict: which observation decided, which was not used."""
        at_dt = None if at is None else self._require_time(at, "at")
        known_dt = None if known_at is None else self._require_time(known_at, "known_at")
        ref = at_dt if at_dt is not None else self._reference()
        piles = [pile_id] if pile_id is not None else self._pile_ids()
        out: dict[str, Any] = {
            "rules_version": self.rules_version,
            "as_of": None if at_dt is None else iso(at_dt),
            "known_at": None if known_dt is None else iso(known_dt),
            "piles": {},
        }
        for pile in piles:
            resolved = self._resolve(pile, ref, at_dt, known_dt)
            observations = []
            for obs in resolved.observations:
                disposition, note = resolved.dispositions.get(obs.id, ("recorded", None))
                observations.append({
                    "observation_id": obs.obs_id,
                    "status": obs.status,
                    "event_ts": obs.event_ts,
                    "recorded_at": obs.recorded_at,
                    "source": obs.source,
                    "state": obs.state,
                    "stored_state": obs.stored_state,
                    "stored_reason": obs.stored_reason,
                    "rules_version": obs.rules_version,
                    "disposition": disposition,
                    "note": note,
                    "changed": any(c["observation_id"] == obs.obs_id
                                   for c in resolved.changes),
                })
            out["piles"][pile] = {
                "verdict": resolved.verdict.to_dict(),
                "observations": observations,
                "changes": resolved.changes,
                "rejected": self._rejected(pile),
            }
        if pile_id is None:
            # Rejections that could not even be attributed to a pile are still
            # explainable; keep them separate so they are never silently lost.
            out["unattributed_rejections"] = self._rejected(None)
        return out

    def _pile_ids(self) -> list[str]:
        ids = [r[0] for r in self.con.execute(
            "SELECT DISTINCT pile_id FROM observations ORDER BY pile_id")]
        extra = [r[0] for r in self.con.execute(
            "SELECT DISTINCT pile_id FROM rejected_observations"
            " WHERE pile_id IS NOT NULL AND pile_id <> '' ORDER BY pile_id")]
        for pile in extra:
            if pile not in ids:
                ids.append(pile)
        return ids

    def _rejected(self, pile_id: str | None) -> list[dict[str, Any]]:
        if pile_id is None:
            rows = self.con.execute(
                "SELECT obs_id,event_ts,recorded_at,reason,detail,payload"
                " FROM rejected_observations"
                " WHERE pile_id IS NULL OR pile_id = '' ORDER BY id")
        else:
            rows = self.con.execute(
                "SELECT obs_id,event_ts,recorded_at,reason,detail,payload"
                " FROM rejected_observations WHERE pile_id=? ORDER BY id", (pile_id,))
        return [{"observation_id": obs_id, "event_ts": event_ts,
                 "recorded_at": recorded_at, "reason": reason, "detail": detail,
                 "disposition": "rejected", "payload": json.loads(payload)}
                for obs_id, event_ts, recorded_at, reason, detail, payload in rows]

    # -- replay ----------------------------------------------------------- #
    def _reference(self, now=None) -> datetime:
        """Instant used to judge freshness.

        Defaults to the wall clock.  Future-dated heartbeats simply have a
        negative age (clock skew is reported, not hidden).
        """
        if now is None:
            ref = self._clock()
        elif isinstance(now, datetime):
            ref = now
        else:
            return self._require_time(now, "now")
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)
        return ref.astimezone(timezone.utc)

    @staticmethod
    def _require_time(value, name) -> datetime:
        parsed = parse_timestamp(value)
        if parsed is None:
            raise ValueError("%s is not a timestamp: %r" % (name, value))
        return parsed

    def _load(self, pile_id: str) -> list[_Obs]:
        rows = self.con.execute(
            "SELECT id,obs_id,pile_id,status,event_ts,recorded_at,source,payload,"
            " stored_state,stored_reason,rules_version FROM observations"
            " WHERE pile_id=? ORDER BY event_ts, id", (pile_id,))
        out = []
        for (oid, obs_id, pid, status, event_ts, recorded_at, source, payload,
             stored_state, stored_reason, rules_version) in rows:
            event_dt = parse_timestamp(event_ts)
            recorded_dt = parse_timestamp(recorded_at)
            if event_dt is None or recorded_dt is None:  # defensive: corrupt row
                continue
            out.append(_Obs(oid, obs_id, pid, status, iso(event_dt), iso(recorded_dt),
                            source, json.loads(payload), stored_state, stored_reason,
                            rules_version, event_dt, recorded_dt))
        return out

    def _resolve(self, pile_id: str, ref_dt: datetime,
                 at_dt: datetime | None, known_dt: datetime | None) -> _Resolved:
        observations = self._load(pile_id)
        dispositions: dict[int, tuple[str, str | None]] = {}
        changes: list[dict[str, Any]] = []

        if not observations:
            return _Resolved(
                Verdict(pile_id, kernel.UNOBSERVED, "no_observations",
                        as_of=None if at_dt is None else iso(at_dt),
                        known_at=None if known_dt is None else iso(known_dt),
                        rules_version=self.rules_version),
                [], dispositions, changes)

        considered: list[_Obs] = []
        for obs in observations:
            if at_dt is not None and obs.event_dt > at_dt:
                dispositions[obs.id] = ("future_beyond_as_of", "event happened after as-of")
            elif known_dt is not None and obs.recorded_dt > known_dt:
                dispositions[obs.id] = ("not_known_at_time",
                                        "recorded after the knowledge cutoff")
            else:
                considered.append(obs)

        if not considered:
            reason = "not_yet_known" if known_dt is not None else "no_observations_in_window"
            return _Resolved(
                Verdict(pile_id, kernel.UNOBSERVED, reason,
                        as_of=None if at_dt is None else iso(at_dt),
                        known_at=None if known_dt is None else iso(known_dt),
                        rules_version=self.rules_version),
                observations, dispositions, changes)

        state = kernel.UNOBSERVED
        deciding: _Obs | None = None
        applied: list[_Obs] = []
        changed_ids: set[int] = set()
        terminal_obs: _Obs | None = None

        for _event_ts, group in _group_by_event_ts(considered):
            winner = max(group, key=lambda o: (kernel.severity(o.state), o.status or ""))
            losers = [o for o in group if o.id != winner.id]

            if terminal_obs is not None:
                # A retirement marker closed the log; nothing later counts.
                for obs in group:
                    dispositions[obs.id] = ("ignored_after_terminal",
                                            "pile was already %s at %s"
                                            % (kernel.STOPPED, terminal_obs.event_ts))
                continue

            # Explain every non-winning report at this instant.
            for obs in losers:
                if obs.state == winner.state:
                    dispositions[obs.id] = ("consistent",
                                            "same state as the winning report")
                elif kernel.is_terminal(winner.state):
                    dispositions[obs.id] = ("conflict_not_adopted",
                                            "terminal report wins at this timestamp")
                else:
                    dispositions[obs.id] = ("conflict_not_adopted",
                                            "lower-severity report at the same timestamp")

            previous = state
            state = winner.state
            deciding = winner
            applied.append(winner)
            if kernel.is_terminal(winner.state):
                terminal_obs = winner
                dispositions[winner.id] = ("applied", "terminal observation")
            else:
                dispositions[winner.id] = ("applied", None)
            if previous != state:
                changed_ids.add(winner.id)
                changes.append({
                    "event_ts": winner.event_ts,
                    "observation_id": winner.obs_id,
                    "from_state": previous,
                    "to_state": state,
                    "status": winner.status,
                    "reason": winner.classification.reason,
                    "terminal": kernel.is_terminal(state),
                    "rules_version": self.rules_version,
                })

        for obs in considered:
            dispositions.setdefault(obs.id, ("recorded", None))

        # The final verdict is decided by the newest applied observation; all
        # earlier applied ones still shaped history, but were superseded.
        # Repeat reports that confirmed an unchanged state are called out as
        # such, because they did not move the verdict.
        for obs in applied[:-1]:
            disposition, _note = dispositions[obs.id]
            if disposition != "applied":
                continue
            if obs.id in changed_ids:
                dispositions[obs.id] = (
                    "applied_then_superseded",
                    "changed state at %s, later heartbeat decided the verdict"
                    % obs.event_ts)
            else:
                dispositions[obs.id] = (
                    "confirmed_then_superseded",
                    "restated %s at %s, later heartbeat decided the verdict"
                    % (obs.state, obs.event_ts))
        if deciding is not None and deciding.id not in changed_ids:
            disposition, _note = dispositions[deciding.id]
            if disposition == "applied":
                dispositions[deciding.id] = (
                    "applied_unchanged",
                    "restates %s; the verdict was already this state" % deciding.state)

        stale = False
        age = None
        if deciding is not None:
            age = (ref_dt - deciding.event_dt).total_seconds()
            if (self.freshness is not None and state == kernel.AVAILABLE
                    and age > self.freshness.total_seconds()):
                # A heartbeat that has gone quiet is not a fault.  The
                # transition list stays observation-driven; freshness only
                # changes the verdict returned for "now".
                state = kernel.UNOBSERVED
                stale = True
                dispositions[deciding.id] = (
                    "applied_stale",
                    "available report is %ss old, beyond the %ss freshness window"
                    % (int(age), int(self.freshness.total_seconds())))

        if deciding is None:
            verdict = Verdict(pile_id, kernel.UNOBSERVED, "no_observations_in_window",
                              as_of=None if at_dt is None else iso(at_dt),
                              known_at=None if known_dt is None else iso(known_dt),
                              rules_version=self.rules_version)
        else:
            reason = "stale_heartbeat" if stale else deciding.classification.reason
            verdict = Verdict(
                pile_id, state, reason,
                as_of=None if at_dt is None else iso(at_dt),
                known_at=None if known_dt is None else iso(known_dt),
                event_ts=deciding.event_ts,
                recorded_at=deciding.recorded_at,
                observed_at=deciding.event_ts,
                age_seconds=age,
                stale=stale,
                terminal=kernel.is_terminal(state),
                observation_id=deciding.obs_id,
                status=deciding.status,
                rules_version=self.rules_version,
                stored_state=deciding.stored_state)
        return _Resolved(verdict, observations, dispositions, changes)


# --------------------------------------------------------------------------- #
# Helpers.
# --------------------------------------------------------------------------- #
def observation_id(pile_id: str, event_iso: str, status: str) -> str:
    blob = "\x1f".join((pile_id, event_iso, status or ""))
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def _group_by_event_ts(observations: Iterable[_Obs]):
    groups: list[tuple[str, list[_Obs]]] = []
    index: dict[str, list[_Obs]] = {}
    for obs in observations:
        bucket = index.get(obs.event_ts)
        if bucket is None:
            bucket = []
            index[obs.event_ts] = bucket
            groups.append((obs.event_ts, bucket))
        bucket.append(obs)
    groups.sort(key=lambda pair: parse_timestamp(pair[0]))
    return groups


def _json_safe(record: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for key, value in record.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            out[key] = value
        elif isinstance(value, datetime):
            out[key] = iso(value)
        else:
            out[key] = str(value)
    return out


def _coerce_freshness(freshness) -> timedelta | None:
    """Accept a window in seconds (int/float/timedelta) or ``None``."""
    if freshness is None:
        freshness = STATE_FRESHNESS_SECONDS
    if freshness is None:
        return None
    if isinstance(freshness, timedelta):
        return freshness
    return timedelta(seconds=float(freshness))


def _coerce_clock(clock):
    """A clock is either a datetime, an ISO string, or a zero-arg callable."""
    if clock is None:
        return utcnow
    if isinstance(clock, datetime):
        fixed = clock if clock.tzinfo else clock.replace(tzinfo=timezone.utc)
        return lambda: fixed
    if isinstance(clock, str):
        parsed = parse_timestamp(clock)
        if parsed is None:
            raise ValueError("clock is not a timestamp: %r" % (clock,))
        return lambda: parsed
    return clock
