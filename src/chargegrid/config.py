
from types import SimpleNamespace

ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "chargegrid.db"

SPEC = SimpleNamespace(
    slug="chargegrid",
    fmt="json",
    engine="crawl",
    endpoint="items",
    columns=["id", "name", "value", "status", "ts"],
    page_size=4,
    int_fields=["value"],
    ts_field="ts",
)

# Optional freshness policy: a heartbeat older than this no longer proves the
# pile is available, so the verdict degrades from "available" to "unobserved"
# (never to "fault").  ``None`` disables the policy; operators opt in by
# setting this value or passing ``freshness=`` to the pipeline.
STATE_FRESHNESS_SECONDS = None
