# chargegrid

A small offline data-processing service with a local HTTP fixture and SQLite
storage. It is self-contained and does not require network access.

The service collects charging-pile heartbeats and turns them into a verdict
about each pile. Heartbeats can arrive late, out of order, or not at all, so
the verdict is derived by replaying an append-only observation log rather than
by mutating a row in place.

## Usage

```text
make run       # process the bundled source
make run_inc   # process using the incremental command-line mode
make test      # run the test suite
make mock      # start the local fixture service
```

The package exposes a Python entry point under `src/chargegrid`. The fixture
service supports the source formats used by the package, and the command-line
entry point writes results to the local SQLite database. Re-running commands is
safe for an existing database.

## State model

Each pile ends up in exactly one of these states:

| state       | meaning                                                        |
|-------------|----------------------------------------------------------------|
| `available` | a heartbeat confirms the pile can serve                        |
| `fault`     | a heartbeat reports a real malfunction                         |
| `unknown`   | a heartbeat was received but its meaning is unclear            |
| `stopped`   | the pile is permanently retired (terminal)                     |
| `unobserved`| no usable heartbeat is available; this is *not* a fault        |

* Only `stopped` is terminal. A retirement marker is permanent; a later
  `available` report is recorded but not adopted, and the diagnostics say so.
* A *missing* heartbeat produces `unobserved`, never `fault`. An optional
  freshness window (`--freshness`, seconds) downgrades an old `available`
  report to `unobserved`; it never turns silence into a fault.
* Contradicting reports for the same pile and instant are resolved towards the
  worse state (`available` < `unknown` < `fault` < `stopped`), so a pile is not
  declared healthy while another report says otherwise.

## Querying history

```text
chargegrid run [--incremental] [--max-pages N] [--start-page N]
chargegrid current [--now ISO] [--pile ID]
chargegrid state-at --pile ID --at ISO [--known-at ISO]
chargegrid history --pile ID [--at ISO] [--known-at ISO]
chargegrid diagnose [--pile ID] [--at ISO] [--known-at ISO]
```

`state-at` and `history` are pure reads: they replay the observation log up to
the requested instant and never write anything, so querying the past cannot
change the current verdict. `--known-at` additionally hides heartbeats that had
not been recorded yet at that instant (the view an operator had at the time).

`diagnose` explains the verdict: which observation decided it, which
observation changed the state at which moment, and why each observation that
was *not* adopted was skipped (`superseded`, `conflict_not_adopted`,
`ignored_after_terminal`, `future_beyond_as_of`, `not_known_at_time`, ...).
Rejected records are listed with their reason (`duplicate_observation`,
`missing_pile_id`, `missing_timestamp`, `invalid_timestamp`) and their original
payload, so a heartbeat can always be accounted for.

## Reliability properties

* **Duplicates** are keyed on pile, event time and status. Re-ingesting the
  same heartbeat is rejected with a reason; re-running a whole crawl leaves the
  verdict unchanged.
* **Late / out-of-order heartbeats** are appended and replayed in event-time
  order. A late heartbeat can fill in the past, but does not rewrite the
  present. The result is independent of arrival order.
* **Interrupted crawls** ingest page by page. A page failure keeps the pages
  already read and reports `next_page`; `--start-page` resumes without
  re-reading. A resumed sequence reaches the same verdicts as a single
  complete crawl.
* **Restored snapshots / rule updates** stay readable. Observations keep the
  classification that was stored when they were ingested, but queries
  re-derive state from the raw `status` with the current rules; each verdict
  reports both. A database that only contains the legacy `records` table is
  backfilled into the observation log on the first run.

Field extraction remains in `framework.parse` and retry/backoff remains in
`framework.Http`; the state engine consumes already-parsed records.
