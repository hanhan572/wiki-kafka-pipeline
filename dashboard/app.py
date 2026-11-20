"""Streamlit dashboard — live view of the Wikipedia edit pipeline (Kafka -> Postgres -> here).

Everything on this page is read from Postgres and refreshes itself every 5 seconds (st.fragment run_every).
Self-contained on purpose (no imports from `pipeline/`) so it can be deployed on Streamlit Community Cloud.
"""
import os
from decimal import Decimal
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg
import streamlit as st

REFRESH_SECONDS = 5
HUMAN, BOT, ACCENT, ALERT = "#3b6ea8", "#d9822b", "#4f9d69", "#d64545"

try:
    st.set_page_config(page_title="Wikipedia Live Edits", page_icon="📡", layout="wide")
except Exception:      # already set by streamlit_app.py (the cloud entry point)
    pass


def database_url() -> str:
    """DATABASE_URL: environment first, then Streamlit secrets (only touched when a secrets file exists, otherwise
    Streamlit renders a red error box), then the local docker-compose default."""
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    try:
        if any(os.path.exists(p) for p in (os.path.expanduser("~/.streamlit/secrets.toml"), ".streamlit/secrets.toml", "/mount/src/.streamlit/secrets.toml")):
            if "DATABASE_URL" in st.secrets:
                return st.secrets["DATABASE_URL"]
    except Exception:
        pass
    return "postgresql://wiki:wiki@localhost:5433/wiki"


@st.cache_resource
def _holder() -> dict:
    return {"conn": None}


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run a read-only query, reconnecting if the connection died."""
    holder = _holder()
    for attempt in (1, 2):
        try:
            if holder["conn"] is None or holder["conn"].closed:
                holder["conn"] = psycopg.connect(database_url(), autocommit=True, connect_timeout=10, prepare_threshold=None)
            with holder["conn"].cursor() as cur:
                cur.execute(sql, params)
                cols = [d.name for d in cur.description]
                df = pd.DataFrame(cur.fetchall(), columns=cols)
                for col in df.columns:                      # Postgres returns sum(bigint) as Decimal
                    if df[col].dtype == object and len(df) and isinstance(df[col].iloc[0], Decimal):
                        df[col] = df[col].astype(float)
                return df
        except psycopg.Error:
            try:
                holder["conn"].close()
            except Exception:
                pass
            holder["conn"] = None
            if attempt == 2:
                raise
    return pd.DataFrame()


# ----------------------------------------------------------------------------- header / controls
st.title("📡 Wikipedia live edits")
st.caption("Real-time pipeline: **Wikimedia EventStreams → Apache Kafka → stream processors → PostgreSQL → this dashboard**. "
           "Every number below updates automatically every 5 seconds.")
WINDOWS = {"Last 15 minutes": 15, "Last 30 minutes": 30, "Last hour": 60, "Last 3 hours": 180}
window_label = st.sidebar.radio("Time window", list(WINDOWS), index=1)
minutes = WINDOWS[window_label]
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Topics**  \n`wikipedia.edits` (raw)  \n`wikipedia.edit-alerts` (rules)  \n`wikipedia.rate-scores` (forecasts)\n\n"
    "**Consumer groups**  \n`edits-to-postgres`  \n`edit-stats-processor`  \n`rate-anomaly-detector`  \n`derived-to-postgres`")


def fmt_int(n) -> str:
    return f"{int(n):,}"


@st.fragment(run_every=REFRESH_SECONDS)
def live() -> None:
    try:
        latest = query("SELECT max(event_time) AS t, count(*) AS n FROM edits")
        if latest.empty or pd.isna(latest.t[0]):
            if os.environ.get("PIPELINE_MANAGED_BY_APP"):
                st.info("⏳ The pipeline is starting up (Kafka boots inside this app on the first visit). The first events appear in about a minute — this page refreshes by itself.")
            else:
                st.warning("Connected to the database, but no events have arrived yet. Is the producer running?")
            return
        lag = (datetime.now(timezone.utc) - latest.t[0]).total_seconds()
        offline = lag > 120
        # While the pipeline is live every window is relative to now(); if it is offline, relative to the last event received,
        # so the charts still show the most recent data instead of going blank.
        ref = f"'{latest.t[0].isoformat()}'::timestamptz" if offline else "now()"
        since = f"{ref} - make_interval(mins => {minutes})"
        if offline:
            age = f"{lag / 3600:.1f} h" if lag > 3600 else f"{lag / 60:.0f} min"
            starting = " The pipeline is (re)starting in this app — live data should return within a minute or two." if os.environ.get("PIPELINE_MANAGED_BY_APP") else ""
            st.warning(f"⚠️ Pipeline offline — showing the most recent data, received {latest.t[0]:%Y-%m-%d %H:%M} UTC ({age} ago).{starting}")
        rate = query(f"SELECT count(*) AS n FROM edits WHERE event_time > {ref} - interval '60 seconds'").n[0] / 60
        totals = query("SELECT coalesce(sum(edits), 0) AS edits FROM edit_stats_minute").edits[0]
        win = query(f"""SELECT coalesce(sum(edits),0) AS edits, coalesce(sum(bot_edits),0) AS bots, coalesce(sum(human_edits),0) AS humans,
                               coalesce(sum(new_pages),0) AS new_pages, coalesce(sum(bytes_added),0) AS added,
                               coalesce(sum(bytes_removed),0) AS removed, count(DISTINCT wiki) AS wikis
                        FROM edit_stats_minute WHERE minute_start > {since}""").iloc[0]
        n_alerts = query(f"SELECT count(*) AS n FROM alerts WHERE alert_time > {since}").n[0]
        n_anom = query(f"SELECT count(*) AS n FROM rate_scores WHERE is_anomaly AND bucket_time > {since}").n[0]

        # ------------------------------------------------------------------ summary numbers
        c = st.columns(7)
        c[0].metric("Edits / sec", f"{rate:.1f}" if not offline else "–")
        c[1].metric(f"Edits ({window_label.lower()})", fmt_int(win.edits))
        c[2].metric("Bot share", f"{100 * win.bots / max(win.edits, 1):.0f}%")
        c[3].metric("Active wikis", fmt_int(win.wikis))
        c[4].metric("Net bytes", f"{(win.added - win.removed) / 1e6:+.1f} MB")
        c[5].metric("Alerts · anomalies", f"{fmt_int(n_alerts)} · {fmt_int(n_anom)}")
        c[6].metric("Data latency", (f"{lag:.0f} s" if lag < 3600 else f"{lag / 3600:.1f} h"), delta="live" if lag < 60 else "offline" if offline else "lagging", delta_color="normal" if lag < 60 else "inverse")
        st.caption(f"{fmt_int(totals)} edits aggregated since the pipeline started · {fmt_int(latest.n[0])} raw events currently stored · "
                   f"refreshed {datetime.now(timezone.utc):%H:%M:%S} UTC")

        # ------------------------------------------------------------------ row 1: throughput + top wikis
        left, right = st.columns([3, 2])
        per_min = query(f"""SELECT minute_start, sum(human_edits) AS human, sum(bot_edits) AS bot
                            FROM edit_stats_minute WHERE minute_start > {since} AND minute_start < date_trunc('minute', {ref})
                            GROUP BY minute_start ORDER BY minute_start""")   # skip the still-filling current minute
        with left:
            st.subheader("Edits per minute (stream-processor output)")
            st.caption("tumbling 1-minute windows, computed by the `edit-stats-processor` consumer")
            if len(per_min) > 1:
                long = per_min.melt("minute_start", var_name="editor", value_name="edits")
                fig = px.area(long, x="minute_start", y="edits", color="editor", color_discrete_map={"human": HUMAN, "bot": BOT})
                fig.update_layout(height=330, margin=dict(l=0, r=0, t=10, b=0), legend_title_text="", xaxis_title=None, yaxis_title="edits / min")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Collecting the first full minute …")
        top = query(f"""SELECT wiki, sum(edits) AS edits, sum(bot_edits) AS bot_edits FROM edit_stats_minute
                        WHERE minute_start > {since} GROUP BY wiki ORDER BY edits DESC LIMIT 10""")
        with right:
            st.subheader("Busiest wikis")
            if not top.empty:
                fig = px.bar(top.iloc[::-1], x="edits", y="wiki", orientation="h", color_discrete_sequence=[ACCENT])
                fig.update_layout(height=330, margin=dict(l=0, r=0, t=10, b=0), xaxis_title="edits", yaxis_title=None)
                st.plotly_chart(fig, use_container_width=True)

        # ------------------------------------------------------------------ row 2: composition + edit sizes
        a, b = st.columns(2)
        with a:
            st.subheader("Who is editing? Humans vs bots")
            if win.edits:
                pie = go.Figure(go.Pie(labels=["Humans", "Bots"], values=[win.humans, win.bots], hole=0.55,
                                       marker=dict(colors=[HUMAN, BOT]), sort=False))
                pie.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), legend=dict(orientation="h"))
                pie.add_annotation(text=f"{fmt_int(win.new_pages)}<br>new pages", showarrow=False, font_size=14)
                st.plotly_chart(pie, use_container_width=True)
        with b:
            st.subheader("How big are the edits? (bytes added / removed)")
            sizes = query(f"SELECT byte_delta FROM edits WHERE event_time > {since} ORDER BY event_time DESC LIMIT 30000")
            if not sizes.empty:
                clipped = sizes.byte_delta.clip(-3000, 3000)
                fig = px.histogram(clipped, nbins=60, color_discrete_sequence=[HUMAN])
                fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), showlegend=False,
                                  xaxis_title="byte change (clipped at ±3000)", yaxis_title="edits")
                st.plotly_chart(fig, use_container_width=True)

        # ------------------------------------------------------------------ row 3: sequential model
        st.subheader("Edit-rate forecast and anomaly detection (online AR model on the stream)")
        rs = query(f"""SELECT bucket_time, observed, forecast, z_ar, is_anomaly FROM rate_scores
                       WHERE series = 'ALL' AND bucket_time > {since} ORDER BY bucket_time""")
        if len(rs) > 3:
            g1, g2 = st.columns([3, 2])
            with g1:
                fig = go.Figure()
                fig.add_scatter(x=rs.bucket_time, y=rs.observed, name="observed", mode="lines", line=dict(color=HUMAN, width=1.5))
                fig.add_scatter(x=rs.bucket_time, y=rs.forecast, name="AR(3) forecast", mode="lines", line=dict(color=BOT, dash="dot", width=1.5))
                an = rs[rs.is_anomaly]
                fig.add_scatter(x=an.bucket_time, y=an.observed, name="anomaly", mode="markers",
                                marker=dict(color=ALERT, size=11, symbol="x"))
                fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="edits / 10 s", legend=dict(orientation="h"))
                st.plotly_chart(fig, use_container_width=True)
            with g2:
                fig = px.line(rs, x="bucket_time", y="z_ar", color_discrete_sequence=[ACCENT])
                fig.add_hline(y=4.0, line_dash="dash", line_color=ALERT, annotation_text="threshold")
                fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="z-score of forecast error", xaxis_title=None)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("The anomaly detector needs ~3 minutes of stream to warm up its model.")

        # ------------------------------------------------------------------ row 4: derived tables
        t1, t2 = st.columns(2)
        with t1:
            st.subheader("🚨 Latest alerts (rules over the stream)")
            al = query(f"""SELECT alert_time AS time, rule, severity, wiki, title, user_name AS "user", detail FROM alerts
                           WHERE alert_time > {since} ORDER BY alert_time DESC LIMIT 15""")
            if al.empty:
                st.info("No alerts in this window.")
            else:
                al["time"] = al["time"].dt.strftime("%H:%M:%S")
                st.dataframe(al, hide_index=True, use_container_width=True, height=300)
        with t2:
            st.subheader("📈 Detected rate anomalies (any wiki)")
            an = query(f"""SELECT bucket_time AS time, series, observed, round(forecast::numeric, 1) AS expected, round(z_ar::numeric, 1) AS z
                           FROM rate_scores WHERE is_anomaly AND bucket_time > {since} ORDER BY bucket_time DESC LIMIT 15""")
            if an.empty:
                st.info("No anomalies in this window — the edit rate is behaving normally.")
            else:
                an["time"] = an["time"].dt.strftime("%H:%M:%S")
                st.dataframe(an, hide_index=True, use_container_width=True, height=300)

        st.subheader("Latest raw events (table `edits`)")
        recent = query("""SELECT event_time AS time, wiki, event_type AS type, title, user_name AS "user", is_bot AS bot, byte_delta AS "bytes"
                          FROM edits ORDER BY event_time DESC LIMIT 12""")
        recent["time"] = recent["time"].dt.strftime("%H:%M:%S")
        st.dataframe(recent, hide_index=True, use_container_width=True, height=300)
    except psycopg.Error as exc:
        st.error(f"Database not reachable: {str(exc)[:200]}")


live()
