
import argparse
import json
import sys
from . import pipeline


def _dump(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _parser():
    """Build the CLI parser.

    Every option is accepted before *and* after the subcommand, so both
    ``chargegrid --pile P1 diagnose`` and ``chargegrid diagnose --pile P1``
    work.  Defaults are suppressed so the subparser cannot blank a value that
    was already supplied before the subcommand.
    """
    def common():
        p = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS)
        p.add_argument("--incremental", action="store_true",
                       help="collect in incremental mode")
        p.add_argument("--full", action="store_true", help="collect everything")
        p.add_argument("--max-pages", type=int,
                       help="stop after this many pages; prints next_page")
        p.add_argument("--start-page", type=int,
                       help="resume pagination from this page")
        p.add_argument("--freshness", type=float,
                       help="override the heartbeat freshness window (seconds)")
        p.add_argument("--at",
                       help="point-in-time query instant (state-at/history/diagnose)")
        p.add_argument("--known-at",
                       help="knowledge cutoff for point-in-time queries")
        p.add_argument("--pile", help="restrict to one pile id")
        p.add_argument("--now",
                       help="evaluate 'now' as this instant instead of the clock")
        return p

    parser = argparse.ArgumentParser(prog="chargegrid", parents=[common()],
                                     argument_default=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command")
    for name, help_text in (
            ("run", "collect heartbeats (default)"),
            ("current", "current verdict for every pile"),
            ("state-at", "verdict at --at (pure read)"),
            ("history", "state transitions, optionally up to --at"),
            ("diagnose", "explain which observation decided")):
        sub.add_parser(name, parents=[common()], help=help_text,
                       argument_default=argparse.SUPPRESS)
    return parser


def main(argv=None):
    parser = _parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else list(argv))
    command = getattr(args, "command", None) or "run"
    freshness = getattr(args, "freshness", None)
    now = getattr(args, "now", None)
    pile = getattr(args, "pile", None)
    at = getattr(args, "at", None)
    known_at = getattr(args, "known_at", None)
    start_page = getattr(args, "start_page", None) or 1

    if command == "run":
        mode = "incremental" if getattr(args, "incremental", False) else "full"
        _dump(pipeline.run(mode=mode, max_pages=getattr(args, "max_pages", None),
                           start_page=start_page, freshness=freshness, now=now))
        return 0
    if command == "current":
        _dump(pipeline.current(now=now, freshness=freshness))
        return 0
    if command in ("state-at", "history") and not pile:
        parser.error("--pile is required for %s" % command)
    if command == "state-at":
        if not at:
            parser.error("--at is required for state-at")
        _dump(pipeline.state_at(pile, at, known_at=known_at,
                                freshness=freshness))
        return 0
    if command == "history":
        _dump(pipeline.history(pile, at=at, known_at=known_at,
                               freshness=freshness))
        return 0
    if command == "diagnose":
        _dump(pipeline.diagnose(pile, at=at, known_at=known_at,
                                freshness=freshness))
        return 0
    parser.error("unknown command %r" % command)
    return 2


if __name__ == "__main__":
    sys.exit(main())
