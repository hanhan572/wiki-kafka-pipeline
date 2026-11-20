"""Entry point for the public deployment on Streamlit Community Cloud.

Starts the whole streaming pipeline (Kafka + producer + consumers + processors) as background processes — once per
server process — and then renders the dashboard. Postgres is external (Supabase): set the secret DATABASE_URL, and
RUN_PIPELINE = "1" to switch the background pipeline on (leave it off to run the dashboard only, e.g. next to docker-compose).
"""
import os
import pathlib
import runpy
import subprocess
import sys

import streamlit as st

st.set_page_config(page_title="Wikipedia Live Edits", page_icon="📡", layout="wide")   # must be the first Streamlit call

ROOT = pathlib.Path(__file__).resolve().parent


def _has_secrets_file() -> bool:
    return any(p.exists() for p in (pathlib.Path.home() / ".streamlit" / "secrets.toml", ROOT / ".streamlit" / "secrets.toml",
                                    pathlib.Path("/mount/src/.streamlit/secrets.toml"), *pathlib.Path("/mount/src").glob("*/.streamlit/secrets.toml")))


def secret(name: str, default: str = "") -> str:
    """Environment variable first (Streamlit Cloud also exposes secrets that way); st.secrets only if a secrets file exists
    (touching st.secrets without one makes Streamlit render a red error box)."""
    if os.environ.get(name):
        return os.environ[name]
    if _has_secrets_file():
        try:
            if name in st.secrets:
                return str(st.secrets[name])
        except Exception:
            pass
    return default


@st.cache_resource(show_spinner=False)
def _holder() -> dict:
    return {"proc": None}


def ensure_pipeline():
    """Make sure the background pipeline is running: start it on the first visit, and start it again if it died
    (e.g. Kafka crashed) — checked on every page load. Returns the process, or None when the pipeline is switched off."""
    if secret("RUN_PIPELINE").lower() not in ("1", "true", "yes") or not secret("DATABASE_URL"):
        return None
    holder = _holder()
    proc = holder["proc"]
    if proc is None or proc.poll() is not None:
        env = {**os.environ, "DATABASE_URL": secret("DATABASE_URL"), "PIPELINE_PYTHON": sys.executable,
               "WIKI_USER_AGENT": secret("WIKI_USER_AGENT", "wiki-kafka-pipeline/1.0 (student project)")}
        holder["proc"] = subprocess.Popen(["bash", str(ROOT / "deploy" / "cloud" / "run_pipeline.sh")], env=env, cwd=ROOT,
                                          stdout=open("/tmp/pipeline.log", "ab"), stderr=subprocess.STDOUT, start_new_session=True)
    return holder["proc"]


PIPELINE = ensure_pipeline()
os.environ["PIPELINE_MANAGED_BY_APP"] = "1" if PIPELINE is not None else ""
runpy.run_path(str(ROOT / "dashboard" / "app.py"), run_name="__main__")
