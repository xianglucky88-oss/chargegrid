
"""Public orchestration entry point."""
from . import framework


def run(mode="full", con=None, max_pages=None, start_page=1, freshness=None,
        now=None):
    from .config import SPEC
    if SPEC.engine == "rpa":
        return framework.run_rpa(con=con, freshness=freshness, now=now)
    return framework.run(mode=mode, con=con, max_pages=max_pages,
                         start_page=start_page, freshness=freshness, now=now)


def _engine(con=None, freshness=None):
    from .store import Store
    from .state import StateEngine
    own = con is None
    con = con or Store().connect()
    return StateEngine(con, freshness=freshness), con, own


def current(con=None, now=None, freshness=None):
    """Current verdict for every pile."""
    engine, con, own = _engine(con, freshness)
    try:
        return {pile: v.to_dict() for pile, v in engine.all_current(now).items()}
    finally:
        if own:
            con.close()


def state_at(pile_id, at, con=None, known_at=None, freshness=None):
    """Pure point-in-time read; never mutates current state."""
    engine, con, own = _engine(con, freshness)
    try:
        return engine.verdict_at(pile_id, at, known_at=known_at).to_dict()
    finally:
        if own:
            con.close()


def history(pile_id, con=None, at=None, known_at=None, freshness=None):
    engine, con, own = _engine(con, freshness)
    try:
        return engine.history(pile_id, at=at, known_at=known_at)
    finally:
        if own:
            con.close()


def diagnose(pile_id=None, con=None, at=None, known_at=None, freshness=None):
    engine, con, own = _engine(con, freshness)
    try:
        return engine.diagnostics(pile_id, at=at, known_at=known_at)
    finally:
        if own:
            con.close()
