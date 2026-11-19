"""Judgment rules shared by the collection pipeline and the state engine.

The module keeps its original transform entry points (``process`` / ``reduce``)
and adds a small, explicit vocabulary for charging-pile observations:

    available   a heartbeat confirms the pile can serve
    fault       a heartbeat reports a real malfunction
    unknown     a heartbeat was received but its meaning is unclear
    stopped     the pile was permanently decommissioned / withdrawn
    unobserved  no *fresh* heartbeat is available (not a fault by itself)

Only ``stopped`` is terminal: once a pile is retired it never becomes
available again.  Everything else is derived from the event log, so the same
set of observations always produces the same state regardless of arrival
order.
"""
from __future__ import annotations

from typing import NamedTuple

# --------------------------------------------------------------------------- #
# Legacy status vocabulary (kept for backwards compatibility).
# --------------------------------------------------------------------------- #
TRANS = {"new": "pending", "draft": "pending", "active": "active",
         "pending": "pending", "sold": "closed", "withdrawn": "closed",
         "deleted": "archived"}
ORDER = {"unknown": 0, "pending": 1, "active": 2, "closed": 3, "archived": 4}

# --------------------------------------------------------------------------- #
# State vocabulary.
# --------------------------------------------------------------------------- #
RULES_VERSION = "2026-11-18.1"

AVAILABLE = "available"
FAULT = "fault"
UNKNOWN = "unknown"
STOPPED = "stopped"
UNOBSERVED = "unobserved"

STATES = (AVAILABLE, FAULT, UNKNOWN, STOPPED, UNOBSERVED)
OBSERVED_STATES = (AVAILABLE, FAULT, UNKNOWN, STOPPED)

# Same timestamp / conflicting reports are resolved by "safety first": a
# report that claims something worse wins over a report that claims all is
# well.  The order must be stable because it is part of the replay.
SEVERITY = {AVAILABLE: 1, UNKNOWN: 2, FAULT: 3, STOPPED: 4}
TERMINAL_STATES = frozenset({STOPPED})

# ``status`` -> classified state.  Heartbeat spellings and the vocabulary
# used by the bundled fixture are both covered.
STATUS_STATE = {
    # healthy / serving
    "active": AVAILABLE,
    "online": AVAILABLE,
    "ok": AVAILABLE,
    "normal": AVAILABLE,
    "available": AVAILABLE,
    "idle": AVAILABLE,
    "charging": AVAILABLE,
    "new": AVAILABLE,
    "in_service": AVAILABLE,
    # real failures
    "fault": FAULT,
    "error": FAULT,
    "failed": FAULT,
    "failure": FAULT,
    "alarm": FAULT,
    "offline": FAULT,
    "unavailable": FAULT,
    "out_of_service": FAULT,
    # permanently stopped / retired
    "stopped": STOPPED,
    "decommissioned": STOPPED,
    "retired": STOPPED,
    "removed": STOPPED,
    "scrapped": STOPPED,
    "sold": STOPPED,
    "withdrawn": STOPPED,
    "deleted": STOPPED,
    "terminated": STOPPED,
    # explicit ambiguity
    "unknown": UNKNOWN,
    "indeterminate": UNKNOWN,
    # equipment that is not commissioned yet: we know about it but cannot
    # claim it is available, and it is certainly not a fault.
    "draft": UNKNOWN,
    "pending": UNKNOWN,
}


class Classification(NamedTuple):
    state: str
    reason: str
    recognized: bool
    terminal: bool


def classify(status) -> Classification:
    """Map a raw heartbeat status to a state class.

    An empty status is still an observation: it tells us the pile reported in
    without telling us anything usable, hence ``unknown``.  An unrecognised
    value is also ``unknown`` and carries a reason that explains why it was
    not interpreted.
    """
    if status is None:
        raw = ""
    elif isinstance(status, str):
        raw = status.strip().lower()
    else:
        raw = str(status).strip().lower()

    if not raw:
        return Classification(UNKNOWN, "missing_status", False, False)

    state = STATUS_STATE.get(raw)
    if state is None:
        return Classification(UNKNOWN, "unrecognized_status", False, False)
    if state == UNKNOWN:
        return Classification(UNKNOWN, "explicit_unknown", True, False)
    return Classification(state, "status_" + raw, True, state in TERMINAL_STATES)


def severity(state: str) -> int:
    return SEVERITY.get(state, 0)


def is_terminal(state: str) -> bool:
    return state in TERMINAL_STATES


# --------------------------------------------------------------------------- #
# Original transform entry points.
# --------------------------------------------------------------------------- #
def process(record):
    s = str(record.get("status", "")).lower()
    state = TRANS.get(s, "unknown")
    c = classify(record.get("status"))
    recognized = s in TRANS or c.recognized
    return {
        "state_norm": state,
        "invalid_status": int(not recognized),
        # New fields are additive; existing consumers of state_norm are
        # unaffected.
        "state_class": c.state,
        "state_reason": c.reason,
        "state_terminal": int(c.terminal),
        "rules_version": RULES_VERSION,
    }


def reduce(records):
    last = {}
    for r in sorted(records, key=lambda x: (str(x.get("id")), str(x.get("ts", "")))):
        rid = r.get("id")
        st = r.get("state_norm", "unknown")
        r["regression"] = int(rid in last and ORDER.get(st, 0) < ORDER.get(last[rid], 0))
        if ORDER.get(st, 0) >= ORDER.get(last.get(rid, "unknown"), 0):
            last[rid] = st
    return records
