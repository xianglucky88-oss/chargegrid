"""Local transport, parsing, storage, and session helpers."""
from __future__ import annotations
import hashlib
import importlib
import json

from lxml import html as _lh

from .config import SPEC
from .store import Store


# --------------------------------------------------------------------------- #
# Transport helpers.
# --------------------------------------------------------------------------- #
class Transport:
    """Client for the bundled local service."""

    def __init__(self):
        import sys
        for p in (str(SPEC_ROOT()), str(SPEC_ROOT() / "src")):
            if p not in sys.path:
                sys.path.insert(0, p)
        from starlette.testclient import TestClient
        app = importlib.import_module("mockapp.main").app
        self.client = TestClient(app)
        self.cookies: dict[str, str] = {}

    def request(self, method, path, params=None, json_body=None, headers=None):
        q = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        resp = self.client.request(method, path, params=q, json=json_body,
                                   headers=headers or {}, cookies=self.cookies)
        for k, v in resp.cookies.items():
            self.cookies[k] = v
        return resp.status_code, dict(resp.headers), resp.text


def SPEC_ROOT():
    from .config import ROOT
    return ROOT


class Sleeper:
    """Records requested delays without blocking the caller."""

    def __init__(self):
        self.slept = 0.0
        self.calls = 0

    def sleep(self, seconds):
        self.slept += seconds
        self.calls += 1


class Http:
    """HTTP client with bounded retry support."""

    def __init__(self, transport: Transport, sleeper: Sleeper,
                 max_retries=4, base_backoff=0.5, politeness=0.25):
        self.t = transport
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.politeness = politeness

    def get(self, path, params=None, headers=None):
        attempt = 0
        while True:
            self.sleeper.sleep(self.politeness)
            status, hdr, body = self.t.request("GET", path, params, headers=headers)
            if status in (429, 500, 502, 503) and attempt < self.max_retries:
                self.sleeper.sleep(self.base_backoff * (2 ** attempt))
                attempt += 1
                continue
            return status, hdr, body, attempt


# --------------------------------------------------------------------------- #
# Response parsing.
# --------------------------------------------------------------------------- #
def _coerce(name, val):
    if name in SPEC.int_fields:
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def parse(resp_body, fmt):
    cols = SPEC.columns
    if fmt == "json":
        items = json.loads(resp_body).get("items", [])
        return [{c: _coerce(c, it.get(c)) for c in cols} for it in items]
    if fmt == "html":
        doc = _lh.fromstring(resp_body)
        rows = []
        for tr in doc.xpath("//table//tr[./td]"):
            cells = [td.text_content().strip() for td in tr.xpath("./td")]
            if len(cells) != len(cols):
                continue
            rows.append({c: _coerce(c, v) for c, v in zip(cols, cells)})
        return rows
    if fmt == "text":
        out = []
        for line in resp_body.splitlines():
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("\t")]
            d = {c: _coerce(c, parts[i]) if i < len(parts) else _coerce(c, "")
                 for i, c in enumerate(cols)}
            out.append(d)
        return out
    raise ValueError("unknown fmt: %s" % fmt)


def fetch_all(http: Http, max_pages=None):
    """Collect records from the local source (see :func:`fetch_pages`)."""
    records, attempts = [], 0
    for page_recs, att, _page, _more in fetch_pages(http, max_pages=max_pages):
        records.extend(page_recs)
        attempts += att
    return records, attempts


class FetchInterrupted(RuntimeError):
    """Raised when pagination could not be completed.

    Pages that were yielded before the failure have already been handed to
    the caller (and therefore already ingested); ``next_page`` says where a
    later run should continue, and ``status`` preserves the cause.
    """

    def __init__(self, message, next_page, status, attempts):
        super().__init__(message)
        self.next_page = next_page
        self.status = status
        self.attempts = attempts


def fetch_pages(http: Http, max_pages=None, start_page=1):
    """Yield ``(rows, attempts, page_number, has_more)`` one page at a time.

    ``has_more`` is true when the source can supply another page, so a
    truncated crawl knows exactly where to resume.  Field extraction stays in
    :func:`parse` and retry/backoff stays in :class:`Http`; this function is
    only responsible for walking the pages.
    """
    page = start_page
    while True:
        status, _hdr, body, att = http.get("/" + SPEC.endpoint, params={"page": page})
        if status != 200:
            raise FetchInterrupted(
                "fetch failed status=%s page=%s" % (status, page), page, status,
                att + 1)
        page_recs = parse(body, SPEC.fmt)
        has_more = len(page_recs) >= SPEC.page_size and page < 200
        yield page_recs, att + 1, page, has_more
        if not has_more:
            break
        page += 1
        if max_pages is not None and page - start_page >= max_pages:
            break



# --------------------------------------------------------------------------- #
# Record identity helpers.
# --------------------------------------------------------------------------- #
def canonical(d):
    return json.dumps(d, sort_keys=True, ensure_ascii=False)


def content_hash(d):
    return hashlib.sha256(canonical(d).encode()).hexdigest()[:24]


def dedup(records):
    seen, out, dup = set(), [], 0
    for r in records:
        h = content_hash(r)
        if h in seen:
            dup += 1
            continue
        seen.add(h)
        out.append(r)
    return out, dup


def normalize(r):
    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
    if isinstance(d.get("status"), str):
        d["status"] = d["status"].lower()
    return d


def _apply_kernel(records):
    k = importlib.import_module(".kernel", __package__)
    for r in records:
        if hasattr(k, "process"):
            r.update(k.process(r))
    if hasattr(k, "reduce"):
        records = k.reduce(records)
    return records


# --------------------------------------------------------------------------- #
# Collection pipeline.
# --------------------------------------------------------------------------- #
def run(mode="full", con=None, max_pages=None, start_page=1,
        freshness=None, now=None):
    """Collect heartbeats and fold them into the pile state.

    Parameters
    ----------
    mode:
        ``"full"`` re-reads every page; ``"incremental"`` keeps the watermark
        for reporting but never discards an older heartbeat, because a late
        or out-of-order observation still has to be recorded.
    max_pages, start_page:
        Bound the crawl.  A crawl that stopped after ``max_pages`` reports
        ``next_page`` so a later call can resume without re-reading what it
        already ingested.
    freshness:
        Override the freshness window used to decide whether an "available"
        heartbeat is still meaningful.
    """
    from .state import StateEngine

    own = con is None
    con = con or Store().connect()
    store = Store(con)
    engine = StateEngine(con, freshness=freshness, clock=now)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    previous_watermark = store.watermark()
    all_records, results = [], []
    attempts = pages = 0
    interrupted = None
    has_more = False
    last_page = start_page - 1
    try:
        # A database restored from an older snapshot may only carry the
        # legacy record table.  Rebuild its event log before adding new
        # heartbeats so history stays readable across the upgrade.
        backfilled = engine.backfill_from_records() if engine.is_empty() else \
            {"imported": 0, "duplicates": 0, "rejected": 0}
        try:
            for page_recs, att, page_no, more in fetch_pages(
                    http, max_pages=max_pages, start_page=start_page):
                attempts += att
                pages += 1
                last_page = page_no
                has_more = more
                norm = [normalize(dict(r)) for r in page_recs]
                all_records.extend(norm)
                # Ingest immediately: a later page failure must not throw
                # away the heartbeats that were already observed.
                for r in norm:
                    results.append(engine.ingest(r, source=mode))
        except FetchInterrupted as exc:
            interrupted = exc
            has_more = True
            attempts += exc.attempts

        # Legacy record table keeps its existing dedup + watermark behaviour.
        kernelized = _apply_kernel([dict(r) for r in all_records])
        kernelized, dup_in_batch = dedup(kernelized)
        wm = previous_watermark
        if mode == "incremental" and wm:
            # Report what the old filter would have dropped, but do not drop
            # it: the state engine has already recorded those observations.
            behind = [r for r in kernelized if str(r.get("ts", "")) <= wm]
        else:
            behind = []
        ingested = sum(1 for r in kernelized if store.upsert(r))
        total = store.count()
        new_wm = max([str(r.get("ts", "")) for r in kernelized] + [wm or ""])
        if new_wm:
            store.set_watermark(new_wm)

        accepted = [r for r in results if r.accepted]
        rejected = [r for r in results if not r.accepted]
        duplicate_obs = [r for r in rejected
                         if r.reason == "duplicate_observation"]
        changed = [r for r in accepted if r.changed]
        states = {pile: v.to_dict()
                  for pile, v in engine.all_current(now).items()}
        next_page = interrupted.next_page if interrupted is not None else (
            last_page + 1 if has_more else None)
        return {
            "engine": "crawl",
            "mode": mode,
            "fetched": len(all_records),
            "duplicates": dup_in_batch,
            "ingested": ingested,
            "count": total,
            "http_attempts": attempts,
            "slept": round(sleeper.slept, 4),
            "watermark": store.watermark(),
            # -- state judgment ------------------------------------------- #
            "pages": pages,
            "last_page": last_page if pages else None,
            "complete": interrupted is None and not has_more,
            "interrupted": interrupted is not None,
            "interrupt_status": None if interrupted is None else interrupted.status,
            "next_page": next_page,
            "observations_accepted": len(accepted),
            "observations_rejected": len(rejected),
            "duplicate_observations": len(duplicate_obs),
            "late_observations": sum(1 for r in accepted if r.late),
            "behind_watermark": len(behind),
            "state_changes": len(changed),
            "states": states,
            "backfilled": backfilled,
            "rejections": [r.to_dict() for r in rejected],
            # Observations from this run that moved a pile's verdict.
            "verdict_changes": [r.to_dict() for r in changed],
        }
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Session pipeline.
# --------------------------------------------------------------------------- #
class Rpa:
    def __init__(self):
        self.t = Transport()
        self.sleeper = Sleeper()
        self.log = []
        self.captcha_solved = False

    def login(self):
        status, _h, _b = self.t.request("POST", "/login",
                                        json_body={"u": "operator", "p": "local"})
        if status != 200:
            raise RuntimeError("login failed status=%s" % status)
        self.log.append("login")
        return True

    def _csrf(self):
        return {"x-csrf": self.t.cookies.get("csrf", "")}

    def _query_page(self, page):
        status, _h, body = self.t.request("GET", "/query", params={"page": page},
                                          headers=self._csrf())
        if status != 200:
            raise RuntimeError("query failed status=%s" % status)
        return json.loads(body)

    def collect(self):
        out, page = [], 1
        while True:
            self.sleeper.sleep(0.1)
            data = self._query_page(page)
            out.extend(data.get("items", []))
            if data.get("captcha_required") and not self.captcha_solved:
                ok = self._solve_captcha()
                if not ok:
                    raise RuntimeError("captcha solve rejected")
                self.captcha_solved = True
                page += 1
                continue
            if not data.get("has_more"):
                break
            page += 1
            if page > 200:
                break
        self.log.append("collect:%d" % len(out))
        return out

    def _solve_captcha(self):
        status, _h, body = self.t.request("POST", "/captcha",
                                          json_body={"code": self.t.cookies.get("challenge", "")},
                                          headers=self._csrf())
        ok = status == 200 and json.loads(body).get("ok") is True
        self.log.append("captcha:%s" % ("solved" if ok else "failed"))
        return ok

    def submit(self, record):
        token = content_hash(record)
        status, _h, body = self.t.request("POST", "/submit",
                                          json_body={"token": token, "record": record},
                                          headers=self._csrf())
        data = json.loads(body)
        self.log.append("submit:%s" % ("created" if data.get("created") else "dedup"))
        return bool(data.get("created"))


def run_rpa(con=None, freshness=None, now=None):
    from .state import StateEngine

    own = con is None
    con = con or Store().connect()
    store = Store(con)
    engine = StateEngine(con, freshness=freshness, clock=now)
    r = Rpa()
    try:
        logged = r.login()
        items = r.collect()
        norm = [normalize(dict(it)) for it in items]
        observations = [engine.ingest(dict(it), source="rpa") for it in norm]
        norm, dup = dedup(norm)
        norm = _apply_kernel(norm)
        ingested = sum(1 for x in norm if store.upsert(x))
        total = store.count()
        created = dedup_submit = 0
        for x in norm[:2]:
            if r.submit(x):
                created += 1
            else:
                dedup_submit += 1
        for x in norm[:1]:
            if not r.submit(x):
                dedup_submit += 1
        wm = max([str(x.get("ts", "")) for x in norm] + [""])
        if wm:
            store.set_watermark(wm)
        rejected = [o for o in observations if not o.accepted]
        return {"engine": "rpa", "logged_in": logged, "captcha_solved": r.captcha_solved,
                "collected": len(items), "fetched": len(items), "duplicates": dup,
                "ingested": ingested, "count": total,
                "submit_created": created, "submit_dedup": dedup_submit, "log": r.log,
                "observations_accepted": len(observations) - len(rejected),
                "observations_rejected": len(rejected),
                "duplicate_observations": sum(
                    1 for o in rejected if o.reason == "duplicate_observation"),
                "state_changes": sum(1 for o in observations if o.accepted and o.changed),
                "states": {pile: v.to_dict()
                           for pile, v in engine.all_current(now).items()},
                "rejections": [o.to_dict() for o in rejected]}
    finally:
        if own:
            con.close()
