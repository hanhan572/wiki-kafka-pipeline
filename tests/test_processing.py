from pipeline.anomaly import RlsArModel, SeriesScorer, anscombe, inv_anscombe
from pipeline.processing import AlertRules, WindowAggregator
from pipeline.producer import to_event


def ev(i, t="2026-01-01T00:00:%02dZ", sec=0, wiki="enwiki", title="Page", user="u", bot=False, delta=10, typ="edit"):
    return {"event_id": f"e{i}", "event_time": t % sec, "wiki": wiki, "title": title, "user_name": user,
            "is_bot": bot, "byte_delta": delta, "event_type": typ}


def test_window_aggregation():
    agg = WindowAggregator()
    agg.add(ev(1, sec=1, delta=100)); agg.add(ev(2, sec=2, bot=True, delta=-40, user="b")); agg.add(ev(3, sec=3, typ="new", user="u"))
    (row,) = agg.pop_dirty_rows()
    assert row[0] == "enwiki" and row[2:] == (3, 1, 2, 1, 110, 40, 2)     # edits, bots, humans, new, +bytes, -bytes, users
    assert agg.pop_dirty_rows() == []                                      # dirty set cleared


def test_windows_split_by_minute_and_wiki():
    agg = WindowAggregator()
    agg.add(ev(1, t="2026-01-01T00:00:%02dZ", sec=59)); agg.add(ev(2, t="2026-01-01T00:01:%02dZ", sec=0)); agg.add(ev(3, sec=5, wiki="dewiki"))
    assert len(agg.pop_dirty_rows()) == 3


def test_mass_deletion_alert_only_for_humans_and_has_cooldown():
    rules = AlertRules(mass_delete_bytes=10_000)
    assert rules.process(ev(1, delta=-20_000, bot=True)) == []
    first = rules.process(ev(2, sec=1, delta=-20_000))
    assert [a["rule"] for a in first] == ["MASS_DELETION"]
    assert rules.process(ev(3, sec=2, delta=-20_000)) == []                # cooldown for same page


def test_edit_burst_needs_several_users():
    rules = AlertRules()
    out = []
    for i, u in enumerate(["a", "a", "a", "a", "a"]):
        out += rules.process(ev(i, sec=i, user=u))
    assert out == []                                                       # 5 edits but one user
    for i, u in enumerate(["b", "c"], start=10):
        out += rules.process(ev(i, sec=i, user=u))
    assert [a["rule"] for a in out] == ["EDIT_BURST"]


def test_alert_ids_are_deterministic():
    a = AlertRules().process(ev(1, delta=-50_000))[0]["alert_id"]
    b = AlertRules().process(ev(1, delta=-50_000))[0]["alert_id"]
    assert a == b


def test_producer_flattening():
    raw = {"type": "edit", "id": 7, "wiki": "enwiki", "title": "T", "user": "U", "bot": False, "minor": True, "namespace": 0,
           "length": {"old": 100, "new": 160}, "comment": "c", "meta": {"id": "uuid", "dt": "2026-01-01T00:00:00Z", "domain": "en.wikipedia.org"}}
    e = to_event(raw)
    assert e["byte_delta"] == 60 and e["event_id"] == "uuid" and e["is_minor"] is True
    assert to_event({**raw, "type": "log"}) is None
    assert to_event({**raw, "meta": {**raw["meta"], "domain": "www.wikidata.org"}}) is None


def test_anscombe_roundtrip():
    for x in (0, 1, 7, 50):
        assert abs(inv_anscombe(anscombe(x)) - x) < 1e-9


def test_ar_model_learns_constant_level_and_flags_spike():
    s = SeriesScorer(warmup=10)
    import random
    random.seed(1)
    for _ in range(200):
        s.score(max(0, int(random.gauss(20, 4))))
    normal = s.score(21)
    spike = s.score(70)
    assert normal["z_ar"] < 3 and spike["z_ar"] > 5


def test_detector_skips_outage_gaps():
    from pipeline.anomaly_detector import Detector
    det = Detector()
    base = "2026-01-01T00:%02d:%02dZ"
    for sec in range(0, 60):                                       # one minute of steady traffic, 3 events / second
        for _ in range(3):
            det.add({"event_time": base % (0, sec), "wiki": "enwiki"})
    before = det.closed_results()
    assert len(before) > 0
    for sec in range(0, 60):                                       # 30 minutes later (outage in between), traffic resumes
        for _ in range(3):
            det.add({"event_time": base % (30, sec), "wiki": "enwiki"})
    after = det.closed_results()
    assert len(after) > 0
    assert all(r["observed"] > 0 for r in after if r["series"] == "ALL")   # no "0 edits" buckets scored for the outage
