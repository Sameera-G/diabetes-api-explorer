import json
import hashlib
import math
import os
import tempfile
import uuid
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import altair as alt
import pandas as pd
import requests
import streamlit as st
from sseclient import SSEClient

BASE = "https://diabetes-app-lab-terveyssovellusten-ohjelmistotuotanto.2.rahtiapp.fi"
TEAM_KEY = ""

TEAM_CONFIG = {
    "team_name": "API Simulation Explorer",
    "team_code": "api-simulation-explorer-2026",
    "scenario_ids": [],
    "domains": [],
    "event_types": [],
    "cohort_size": 20,
    "speed": 3600,
    "start_day": 1,
    "loop": True,
    "view": "full",
}

BACKFILL_HOURS = 6
MAX_EVENTS_PER_TYPE = 500
MAX_LOG_EVENTS = 5000

EVENT_DEFS = {
    "observation.glucose_cgm": {
        "title": "CGM glucose",
        "domain": "glucose",
        "fields": [("value", "Glucose", "mg/dL")],
    },
    "observation.glucose_capillary": {
        "title": "Fingerstick glucose",
        "domain": "glucose",
        "fields": [("value", "Glucose", "mg/dL")],
    },
    "insulin.basal_delivery": {
        "title": "Pump basal delivery",
        "domain": "insulin",
        "fields": [
            ("rate_units_per_hour", "Basal rate", "U/hour"),
            ("delivered_units", "Delivered", "U"),
        ],
    },
    "insulin.bolus_delivery": {
        "title": "Pump bolus delivery",
        "domain": "insulin",
        "fields": [("delivered_units", "Delivered", "U")],
    },
    "insulin.reservoir": {
        "title": "Pump reservoir",
        "domain": "insulin / device",
        "fields": [("remaining_units", "Remaining", "U")],
    },
    "insulin.reservoir_change": {
        "title": "Reservoir change",
        "domain": "insulin / device",
        "fields": [("new_reservoir_units", "New reservoir", "U")],
    },
    "insulin.pump_alert": {
        "title": "Pump alert",
        "domain": "insulin / device",
        "fields": [],
    },
    "insulin.pump_status": {
        "title": "Pump status",
        "domain": "insulin / device",
        "fields": [],
    },
    "medication.administration": {
        "title": "Medication / insulin log",
        "domain": "medication / insulin",
        "fields": [("dose", "Dose", None)],
    },
    "patient.meal": {
        "title": "Meal",
        "domain": "nutrition",
        "fields": [("carbohydrate_g", "Carbohydrates", "g")],
    },
    "patient.activity": {
        "title": "Activity",
        "domain": "activity",
        "fields": [("duration_minutes", "Duration", "minutes")],
    },
    "patient.sleep": {
        "title": "Sleep",
        "domain": "activity",
        "fields": [("hours", "Sleep", "hours")],
    },
    "patient.symptom": {
        "title": "Symptom",
        "domain": "patient_reported",
        "fields": [],
    },
    "encounter.start": {
        "title": "Visit / admission start",
        "domain": "encounters",
        "fields": [],
    },
    "encounter.end": {
        "title": "Visit / discharge",
        "domain": "encounters",
        "fields": [],
    },
    "observation.hba1c": {
        "title": "HbA1c",
        "domain": "laboratory",
        "fields": [("value", "HbA1c", "%")],
    },
    "observation.creatinine": {
        "title": "Creatinine",
        "domain": "laboratory",
        "fields": [("value", "Creatinine", "mg/dL")],
    },
    "observation.blood_pressure": {
        "title": "Blood pressure",
        "domain": "vitals",
        "fields": [
            ("systolic", "Systolic", "mmHg"),
            ("diastolic", "Diastolic", "mmHg"),
        ],
    },
    "observation.heart_rate": {
        "title": "Heart rate",
        "domain": "vitals",
        "fields": [("value", "Heart rate", "beats/min")],
    },
    "device.status": {
        "title": "CGM connection status",
        "domain": "device",
        "fields": [],
    },
}

# A browser session subscribes to a shared connection for at most five minutes.
STREAM_SECONDS = 300
REFRESH_SECONDS = 5
VIEWER_TIMEOUT = 30
MAX_CONSECUTIVE_FAILURES = 3
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 10
SAVE_INTERVAL = 30
MAX_DEDUP_KEYS = 15000

APP_DIR = Path(__file__).resolve().parent
HISTORY_PATH = Path(os.environ.get(
    "DIABETES_HISTORY_PATH", str(APP_DIR / ".stream_history.json")
))
SEED_PATH = APP_DIR / "sample_data.json"


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def event_key(event):
    """Suppress identical replayed/backfilled events within a simulation cycle."""
    raw = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.blake2b(raw.encode("utf-8"), digest_size=16).digest()


class RollingHistory:
    """Bounded buffers for a single simulation cycle. Caller holds manager.lock."""

    def __init__(self, cycle=1):
        self.cycle = cycle
        self.by_type = {}
        self.log = deque(maxlen=MAX_LOG_EVENTS)
        self.seen = set()
        self.seen_order = deque()

    def remember(self, key):
        if key in self.seen:
            return False
        if len(self.seen_order) >= MAX_DEDUP_KEYS:
            self.seen.remove(self.seen_order.popleft())
        self.seen.add(key)
        self.seen_order.append(key)
        return True

    def append(self, event):
        if not self.remember(event_key(event)):
            return False
        self.log.append(event)
        event_type = event.get("event_type")
        # Unknown types remain visible in the raw inspector, but cannot
        # create an unlimited number of per-type buffers.
        if isinstance(event_type, str) and event_type in EVENT_DEFS:
            self.by_type.setdefault(event_type, deque(maxlen=MAX_EVENTS_PER_TYPE)).append(event)
        return True

    def snapshot(self):
        return {
            "cycle": self.cycle,
            "by_type": {kind: list(events) for kind, events in self.by_type.items()},
            "log": list(self.log),
        }

    @classmethod
    def restore(cls, data):
        history = cls(max(1, int(data.get("cycle", 1))))
        for kind, events in data.get("by_type", {}).items():
            if kind in EVENT_DEFS and isinstance(events, list):
                recent = [e for e in events[-MAX_EVENTS_PER_TYPE:] if isinstance(e, dict)]
                history.by_type[kind] = deque(recent, maxlen=MAX_EVENTS_PER_TYPE)
                for event in recent:
                    history.remember(event_key(event))
        for event in data.get("log", [])[-MAX_LOG_EVENTS:]:
            if isinstance(event, dict):
                history.log.append(event)
                history.remember(event_key(event))
        return history


class SnapshotStore:
    """Best-effort local snapshots, plus an optional durable PostgreSQL copy."""

    def __init__(self, database_url=""):
        self.database_url = database_url

    @staticmethod
    def valid(data):
        return (isinstance(data, dict) and data.get("schema_version") == 1
                and isinstance(data.get("current"), dict))

    def connect(self):
        # Optional dependency. No database is needed for normal operation.
        import psycopg
        return psycopg.connect(
            self.database_url, connect_timeout=5,
            options="-c statement_timeout=5000",
        )

    @staticmethod
    def ensure_table(conn):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS diabetes_explorer_history (
                id TEXT PRIMARY KEY,
                snapshot JSONB NOT NULL,
                saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    def load(self):
        candidates = []
        errors = []
        try:
            if HISTORY_PATH.is_file():
                data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
                if self.valid(data):
                    candidates.append((data, "Local saved history"))
                else:
                    errors.append("Local snapshot has an unsupported format.")
        except Exception:
            errors.append("Local snapshot could not be read.")
        if self.database_url:
            try:
                with self.connect() as conn:
                    self.ensure_table(conn)
                    row = conn.execute(
                        "SELECT snapshot FROM diabetes_explorer_history WHERE id = %s",
                        (TEAM_CONFIG["team_code"],),
                    ).fetchone()
                    if row and self.valid(row[0]):
                        candidates.append((row[0], "Database saved history"))
            except Exception:
                errors.append("Database snapshot unavailable; using local history if possible.")
        if candidates:
            data, source = max(candidates, key=lambda item: item[0].get("saved_at") or "")
            return data, source, "; ".join(errors)
        try:
            if SEED_PATH.is_file():
                data = json.loads(SEED_PATH.read_text(encoding="utf-8"))
                if self.valid(data):
                    return data, "Bundled historical snapshot (sample_data.json)", "; ".join(errors)
                errors.append("sample_data.json has an unsupported format.")
        except Exception:
            errors.append("Bundled history could not be read.")
        return None, "No retained data yet", "; ".join(errors)

    def save(self, snapshot):
        errors = []
        # Atomic replace: a crash during the write does not corrupt the old file.
        temp_name = None
        try:
            HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=HISTORY_PATH.parent,
                prefix=".diabetes-history-", suffix=".tmp", delete=False,
            ) as out:
                temp_name = out.name
                json.dump(snapshot, out, ensure_ascii=False, separators=(",", ":"))
            os.replace(temp_name, HISTORY_PATH)
        except Exception:
            errors.append("Local snapshot could not be saved.")
        finally:
            if temp_name and os.path.exists(temp_name):
                try:
                    os.unlink(temp_name)
                except OSError:
                    pass
        if self.database_url:
            try:
                from psycopg.types.json import Jsonb
                with self.connect() as conn:
                    self.ensure_table(conn)
                    conn.execute("""
                        INSERT INTO diabetes_explorer_history (id, snapshot, saved_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            snapshot = EXCLUDED.snapshot, saved_at = EXCLUDED.saved_at
                    """, (TEAM_CONFIG["team_code"], Jsonb(snapshot)))
            except Exception:
                errors.append("Database snapshot could not be saved.")
        return "; ".join(errors)


class StreamManager:
    """One thread-safe manager shared by all browser sessions in this process."""

    def __init__(self, database_url=""):
        self.lock = threading.RLock()
        self.store = SnapshotStore(database_url)
        self.current = RollingHistory()
        self.previous = None
        self.pending_cycle = False
        self.clock = {"sim_time": None, "speed_label": None, "loop": None, "lagging": False}
        self.last_event_at = None
        self.saved_at = None
        self.dirty = False
        self.last_save_attempt = time.monotonic()
        self.thread = None
        self.stop_event = threading.Event()
        self.viewers = {}  # viewer_id -> most recent monotonic heartbeat
        self.run_id = None
        self.deadline = 0.0
        self.team_key = TEAM_KEY.strip() or None
        self.connected = False
        self.phase = "paused"
        self.reason = "Click Start live stream when you want to receive new events."
        self.error = ""
        self.failures = 0
        self.total_received = 0
        data, self.source, self.storage_error = self.store.load()
        if data:
            try:
                self.current = RollingHistory.restore(data["current"])
                if isinstance(data.get("previous"), dict):
                    self.previous = RollingHistory.restore(data["previous"])
                self.clock.update(data.get("clock") or {})
                self.last_event_at = data.get("last_event_at")
                self.saved_at = data.get("saved_at")
                # A new server connection may start a different simulation.
                # Keep restored data visible until the first new event arrives.
                self.pending_cycle = bool(self.current.log)
            except Exception:
                self.current = RollingHistory()
                self.previous = None
                self.source = "No retained data yet"
                self.storage_error = "Saved history could not be restored."

    def _request_stop_locked(self, reason):
        if not self.stop_event.is_set():
            self.reason = reason
            self.phase = "stopping"
            self.stop_event.set()

    def _can_continue_locked(self):
        if self.stop_event.is_set():
            return False
        now = time.monotonic()
        if now >= self.deadline:
            self._request_stop_locked("Five-minute live session ended. History retained.")
            return False
        stale = [viewer for viewer, seen in self.viewers.items() if now - seen >= VIEWER_TIMEOUT]
        for viewer in stale:
            del self.viewers[viewer]
        if not self.viewers:
            self._request_stop_locked("No live viewers remain. History retained.")
            return False
        return True

    def can_continue(self):
        with self.lock:
            return self._can_continue_locked()

    def join(self, viewer):
        """Start a new bounded run, or join the existing shared run."""
        with self.lock:
            if self.thread is not None and self.thread.is_alive():
                if not self._can_continue_locked():
                    return None
                self.viewers[viewer] = time.monotonic()
                return self.run_id
            self.run_id = uuid.uuid4().hex
            self.stop_event = threading.Event()
            self.viewers = {viewer: time.monotonic()}
            self.deadline = time.monotonic() + STREAM_SECONDS
            self.phase = "connecting"
            self.reason = "Connecting to the simulation API..."
            self.connected = False
            self.error = ""
            self.failures = 0
            self.thread = threading.Thread(target=self._worker, name="diabetes-sse", daemon=True)
            try:
                self.thread.start()
            except RuntimeError:
                self.thread = None
                self.viewers.clear()
                self.phase = "paused"
                self.error = "Could not start the background worker."
                self.reason = "Please try Start live stream again."
                return None
            return self.run_id

    def heartbeat(self, viewer, run_id):
        with self.lock:
            if run_id != self.run_id or self.phase in {"paused", "stopping"}:
                return False
            if not self._can_continue_locked() or viewer not in self.viewers:
                return False
            self.viewers[viewer] = time.monotonic()
            return True

    def leave(self, viewer, run_id):
        with self.lock:
            if run_id == self.run_id:
                self.viewers.pop(viewer, None)
                if not self.viewers and self.phase not in {"paused", "stopping"}:
                    self._request_stop_locked("Live stream stopped. History retained.")

    def _new_team_key(self):
        # Preserve the team key across pauses and normal reconnects.
        if self.team_key:
            return self.team_key
        with requests.post(
            f"{BASE}/api/v1/team-streams", json=TEAM_CONFIG,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        ) as response:
            response.raise_for_status()
            key = response.json()["team_key"]
            if not isinstance(key, str) or not key:
                raise ValueError("Missing team key")
        with self.lock:
            self.team_key = key
            if self.current.log:
                self.pending_cycle = True
        return key

    def _chunks(self, response):
        # Check the stop condition on network chunks, not only clinical events.
        # A blocked socket is additionally bounded by the requests read timeout.
        for chunk in response.iter_content(chunk_size=128):
            if not self.can_continue():
                return
            if chunk:
                yield chunk

    def _accept_event(self, kind, data):
        with self.lock:
            if not self._can_continue_locked():
                return False
            if kind == "clinical_event" and isinstance(data, dict):
                if self.pending_cycle:
                    if self.current.log:
                        self.previous = self.current
                        self.current = RollingHistory(self.current.cycle + 1)
                    self.pending_cycle = False
                if self.current.append(data):
                    self.total_received += 1
                    self.last_event_at = utc_now()
                    self.source = "Received from the simulation API"
                    self.dirty = True
            elif kind == "clock" and isinstance(data, dict):
                # Only keep the expected clock fields.
                for key in self.clock:
                    if key in data:
                        self.clock[key] = data[key]
                if self.current.log:
                    self.dirty = True
            elif kind == "loop_restart":
                # Do not clear the dashboard. Keep the previous cycle visible
                # until a new clinical event arrives, then archive that cycle.
                self.pending_cycle = True
            return True

    def _snapshot_locked(self):
        return {
            "schema_version": 1,
            "saved_at": utc_now(),
            "last_event_at": self.last_event_at,
            "clock": dict(self.clock),
            "current": self.current.snapshot(),
            "previous": self.previous.snapshot() if self.previous else None,
        }

    def _save_if_needed(self, force=False):
        # Only the worker writes snapshots; never hold the manager lock during I/O.
        with self.lock:
            now = time.monotonic()
            if not self.dirty or (not force and now - self.last_save_attempt < SAVE_INTERVAL):
                return
            self.last_save_attempt = now
            snapshot = self._snapshot_locked()
        error = self.store.save(snapshot)
        with self.lock:
            self.storage_error = error
            # If any configured copy failed, leave dirty so the next run can retry.
            self.dirty = bool(error)
            if not error:
                self.saved_at = snapshot["saved_at"]

    def _worker(self):
        """No Streamlit commands here. One worker owns all network I/O."""
        failures = 0
        try:
            while self.can_continue():
                try:
                    key = self._new_team_key()
                    if not self.can_continue():
                        break
                    url = f"{BASE}/api/v1/team-streams/{quote(key, safe='')}/stream"
                    with requests.get(
                        url, params={"backfill_hours": BACKFILL_HOURS}, stream=True,
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                        headers={"Accept": "text/event-stream"},
                    ) as response:
                        response.raise_for_status()
                        if not self.can_continue():
                            break
                        with self.lock:
                            self.connected = True
                            self.phase = "live"
                            self.reason = "Receiving live events."
                            self.error = ""
                        client = SSEClient(self._chunks(response))
                        for message in client.events():
                            if not self.can_continue():
                                break
                            if message.event == "stream_complete":
                                with self.lock:
                                    self._request_stop_locked("Simulation completed. History retained.")
                                break
                            if message.event in {"clinical_event", "clock"}:
                                try:
                                    data = json.loads(message.data)
                                except (ValueError, TypeError):
                                    continue
                                if not isinstance(data, dict):
                                    continue
                            elif message.event == "loop_restart":
                                data = {}
                            else:
                                continue
                            if not self._accept_event(message.event, data):
                                break
                            if message.event in {"clinical_event", "clock"}:
                                failures = 0
                                with self.lock:
                                    self.failures = 0
                            self._save_if_needed()
                    if not self.can_continue():
                        break
                    # A normal EOF without stream_complete still needs backoff.
                    raise ConnectionError("Stream closed by API")
                except Exception as exc:
                    with self.lock:
                        self.connected = False
                    if not self.can_continue():
                        break
                    failures += 1
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    # Do not expose team keys embedded in request error URLs.
                    if status in {401, 403, 404} and not TEAM_KEY.strip():
                        with self.lock:
                            self.team_key = None
                    with self.lock:
                        self.failures = failures
                        self.error = f"API HTTP {status}" if status else type(exc).__name__
                        if failures >= MAX_CONSECUTIVE_FAILURES:
                            self._request_stop_locked("Connection failed three consecutive times. Click Reconnect to try again.")
                        elif not self.stop_event.is_set():
                            self.phase = "connecting"
                            self.reason = f"Connection interrupted. Retrying ({failures}/{MAX_CONSECUTIVE_FAILURES})..."
                    self._save_if_needed()
                    if not self.can_continue():
                        break
                    # Interruptible retry delay; no forever-reconnect loop.
                    self.stop_event.wait(min(2 ** failures, 8))
        finally:
            with self.lock:
                self.connected = False
                if not self.stop_event.is_set():
                    self._request_stop_locked("Live stream ended. History retained.")
            try:
                self._save_if_needed(force=True)
            finally:
                with self.lock:
                    self.viewers.clear()
                    self.phase = "paused"

    def view(self, use_previous=False):
        """A consistent snapshot for rendering, not references to mutable deques."""
        with self.lock:
            history = self.previous if use_previous and self.previous else self.current
            return {
                "history": history.snapshot(),
                "has_previous": self.previous is not None,
                "clock": dict(self.clock),
                "phase": self.phase,
                "connected": self.connected,
                "reason": self.reason,
                "error": self.error,
                "failures": self.failures,
                "remaining": max(0, math.ceil(self.deadline - time.monotonic())) if self.phase in {"connecting", "live"} else 0,
                "viewers": len(self.viewers),
                "thread_alive": bool(self.thread and self.thread.is_alive()),
                "team_created": bool(self.team_key),
                "source": self.source,
                "storage_error": self.storage_error,
                "saved_at": self.saved_at,
                "last_event_at": self.last_event_at,
                "total_received": self.total_received,
                "pending_cycle": self.pending_cycle,
                "database_configured": bool(self.store.database_url),
            }

    def export(self):
        with self.lock:
            data = self._snapshot_locked()
        return json.dumps(data, ensure_ascii=False, indent=2)


def history_database_url():
    # Optional: configure in Streamlit Secrets, never commit database passwords.
    url = os.environ.get("HISTORY_DATABASE_URL", "")
    if not url:
        try:
            url = st.secrets.get("HISTORY_DATABASE_URL", "")
        except Exception:
            pass  # App also works with no secrets file.
    return str(url or "").strip()


@st.cache_resource
def get_manager(database_url):
    return StreamManager(database_url)


def python_sse_example(event_type):
    return f'''import json
import requests
from sseclient import SSEClient

BASE = "{BASE}"
TEAM_KEY = "YOUR_TEAM_KEY"

response = requests.get(
    f"{{BASE}}/api/v1/team-streams/{{TEAM_KEY}}/stream?backfill_hours=6",
    stream=True,
    headers={{"Accept": "text/event-stream"}}
)
response.raise_for_status()

client = SSEClient(response)

for message in client.events():
    if message.event != "clinical_event":
        continue

    event = json.loads(message.data)

    if event["event_type"] == "{event_type}":
        print(event)

        # Main fields available to your app:
        patient_id = event["patient_id"]
        event_time = event["event_time"]
        available_at = event["available_at"]
        payload = event["payload"]
        quality = event["quality"]
'''


def javascript_sse_example(event_type):
    return f'''const BASE = "{BASE}";
const TEAM_KEY = "YOUR_TEAM_KEY";

const stream = new EventSource(
  `${{BASE}}/api/v1/team-streams/${{TEAM_KEY}}/stream?backfill_hours=6`
);

stream.addEventListener("clinical_event", (message) => {{
  const event = JSON.parse(message.data);

  if (event.event_type === "{event_type}") {{
    console.log(event);

    // Feed the event into your web/mobile state:
    // updateAppState(event);
  }}
}});

stream.addEventListener("clock", (message) => {{
  const clock = JSON.parse(message.data);
  console.log("simulated time:", clock.sim_time);
}});

stream.addEventListener("loop_restart", () => {{
  console.log("simulation restarted from day 1");
}});

stream.onerror = (error) => {{
  console.error("stream error", error);
}};
'''


def rest_example(event_type, patient_id):
    encoded = quote(patient_id, safe="")
    return f'''# Historical REST endpoint for this same event type:
# GET {BASE}/api/v1/patients/{encoded}/events?event_types={event_type}&limit=100&view=full

# Python example:
import requests

BASE = "{BASE}"
PATIENT_ID = "{patient_id}"

r = requests.get(
    f"{{BASE}}/api/v1/patients/{{PATIENT_ID}}/events",
    params={{
        "event_types": "{event_type}",
        "limit": 100,
        "view": "full",
    }},
)
r.raise_for_status()

events = r.json()
print(events)
'''


def rows_for_event_type(event_type, patient_filter, events_by_type):
    definition = EVENT_DEFS[event_type]
    rows = []

    for ev in events_by_type.get(event_type, []):
        if patient_filter != "All patients" and ev.get("patient_id") != patient_filter:
            continue

        payload = ev.get("payload") or {}

        for field_name, label, default_unit in definition["fields"]:
            value = payload.get(field_name)

            if value is None or isinstance(value, bool):
                continue

            try:
                value = float(value)
            except (TypeError, ValueError):
                continue

            rows.append({
                "time": ev.get("event_time"),
                "available_at": ev.get("available_at"),
                "patient_id": ev.get("patient_id"),
                "series": label,
                "value": value,
                "unit": payload.get("unit") or default_unit or "",
                "quality": (ev.get("quality") or {}).get("status"),
            })

    return rows


def make_chart(event_type, patient_filter, events_by_type):
    rows = rows_for_event_type(event_type, patient_filter, events_by_type)

    if not rows:
        return None

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")

    if df.empty:
        return None

    return (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("time:T", title="Simulated event time"),
            y=alt.Y("value:Q", title="Value", scale=alt.Scale(zero=False)),
            color=alt.Color("series:N", title="Measurement"),
            detail="patient_id:N",
            tooltip=[
                alt.Tooltip("time:T", title="Event time"),
                alt.Tooltip("available_at:N", title="Available at"),
                alt.Tooltip("patient_id:N", title="Patient"),
                alt.Tooltip("series:N", title="Measurement"),
                alt.Tooltip("value:Q", title="Value"),
                alt.Tooltip("unit:N", title="Unit"),
                alt.Tooltip("quality:N", title="Quality"),
            ],
        )
        .properties(height=330)
        .interactive()
    )


def latest_for_type(event_type, patient_filter, events_by_type):
    if patient_filter == "All patients":
        events = events_by_type.get(event_type, [])
        return events[-1] if events else None

    for ev in reversed(events_by_type.get(event_type, [])):
        if ev.get("patient_id") == patient_filter:
            return ev

    return None


def status_table(event_type, patient_filter, events_by_type):
    rows = []

    for ev in reversed(events_by_type.get(event_type, [])):
        if patient_filter != "All patients" and ev.get("patient_id") != patient_filter:
            continue

        rows.append({
            "event_time": ev.get("event_time"),
            "patient_id": ev.get("patient_id"),
            "payload": json.dumps(ev.get("payload"), ensure_ascii=False),
        })

        if len(rows) >= 30:
            break

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Streamlit UI: idle by default; only the live fragment refreshes automatically.
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Diabetes API Simulation Explorer",
    page_icon="📡",
    layout="wide",
)

manager = get_manager(history_database_url())
if "viewer_id" not in st.session_state:
    st.session_state.viewer_id = uuid.uuid4().hex
if "live_run_id" not in st.session_state:
    st.session_state.live_run_id = None

st.title("📡 Diabetes App Lab — API Simulation Explorer")
st.caption("Educational synthetic simulation. Not medical advice or a clinical monitoring device.")
st.info(
    "Explore previously received events, graphs, JSON and integration code without "
    "running a live stream. Select Start live stream to receive new data for up to "
    "five minutes. Multiple students share one API connection."
)

with st.sidebar:
    st.header("Simulation settings")
    st.write(f"**Simulated cohort:** {TEAM_CONFIG['cohort_size']} patients")
    st.write(f"**Configured speed:** {TEAM_CONFIG['speed']}")
    st.write(f"**Live session:** {STREAM_SECONDS // 60} minutes")
    st.write(f"**Live refresh:** every {REFRESH_SECONDS} seconds")
    st.write(f"**Viewer timeout:** {VIEWER_TIMEOUT} seconds")
    st.caption("No login is required. Open the dashboard and click Start live stream when needed.")


def show_event_type(event_type, patient_filter, events_by_type):
    definition = EVENT_DEFS[event_type]
    count = sum(
        1 for ev in events_by_type.get(event_type, [])
        if patient_filter == "All patients" or ev.get("patient_id") == patient_filter
    )
    st.divider()
    st.subheader(f"{definition['title']} · `{event_type}` · {count} recent events")
    st.caption(f"Domain: {definition['domain']}")

    graph_col, json_col = st.columns([1.45, 1], gap="large")
    with graph_col:
        if definition["fields"]:
            chart = make_chart(event_type, patient_filter, events_by_type)
            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No numeric events retained for this selection yet.")
        else:
            df = status_table(event_type, patient_filter, events_by_type)
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True, height=330)
            else:
                st.info("No events retained for this selection yet.")
    with json_col:
        st.markdown("#### Latest event JSON")
        latest = latest_for_type(event_type, patient_filter, events_by_type)
        if latest is not None:
            st.code(json.dumps(latest, indent=2, ensure_ascii=False), language="json")
        else:
            st.code(json.dumps({"waiting_for": event_type, "message": "No event received yet."}, indent=2), language="json")

    st.markdown("#### How your application gets this data")
    py_tab, js_tab, rest_tab = st.tabs(["Python SSE", "JavaScript / Web SSE", "REST history"])
    with py_tab:
        st.code(python_sse_example(event_type), language="python")
    with js_tab:
        st.code(javascript_sse_example(event_type), language="javascript")
    with rest_tab:
        example_patient = patient_filter if patient_filter != "All patients" else "synthetic-diabetes-v1:P00001"
        st.code(rest_example(event_type, example_patient), language="python")


# A full app rerun after Start/Stop changes the fragment's timer. In historical
# mode run_every=None, so there are no automatic app reruns at all.
@st.fragment(run_every=REFRESH_SECONDS if st.session_state.live_run_id else None)
def dashboard():
    viewer = st.session_state.viewer_id
    subscription = st.session_state.live_run_id
    if subscription and not manager.heartbeat(viewer, subscription):
        st.session_state.live_run_id = None
        st.rerun()  # Turn off the fragment timer, including on auto-expiry.
    watching = bool(st.session_state.live_run_id)
    state = manager.view()

    st.subheader("Live stream controls")
    start_col, stop_col, refresh_col = st.columns(3)
    start_label = "Reconnect" if state["phase"] == "paused" and state["error"] else (
        "Join live stream" if state["phase"] in {"connecting", "live"} and not watching else "Start live stream"
    )
    with start_col:
        if st.button(start_label, type="primary", disabled=watching, use_container_width=True, key="start_live"):
            run_id = manager.join(viewer)
            if run_id:
                st.session_state.live_run_id = run_id
                st.rerun()
            else:
                st.warning("The previous connection is closing or could not start. Refresh and try again.")
    with stop_col:
        if st.button("Stop my live view", disabled=not watching, use_container_width=True, key="stop_live"):
            manager.leave(viewer, st.session_state.live_run_id)
            st.session_state.live_run_id = None
            st.rerun()
    with refresh_col:
        if st.button("Refresh dashboard", use_container_width=True, key="refresh_history"):
            st.rerun()

    if watching and state["phase"] == "live":
        st.success("Live stream connected. Your dashboard updates automatically.")
    elif watching:
        st.info("Connecting or reconnecting to the simulation API...")
    elif state["phase"] in {"live", "connecting"}:
        st.info("Historical view. Other students are using the shared live stream. Join to receive automatic updates.")
    elif state["phase"] == "stopping":
        st.info("Historical view. The shared connection is closing; refresh or try Start again shortly.")
    else:
        st.info("Historical view — streaming is paused. Your retained data remains visible.")

    if state["phase"] in {"connecting", "live"}:
        remaining = state["remaining"]
        st.caption(f"Shared stream auto-stops in {remaining // 60:02d}:{remaining % 60:02d} · {state['viewers']} live viewer(s)")
        st.progress(min(1.0, remaining / STREAM_SECONDS))
    st.caption(state["reason"])
    if state["error"]:
        st.warning(f"Last connection error: {state['error']}. {state['failures']} consecutive failure(s).")
    if state["storage_error"]:
        st.warning(state["storage_error"])

    # A distinct snapshot is kept for the most recent completed simulation cycle.
    cycle_options = ["Latest available cycle"]
    if state["has_previous"]:
        cycle_options.append("Previous cycle")
    if st.session_state.get("cycle_choice") not in cycle_options:
        st.session_state.cycle_choice = cycle_options[0]
    cycle_choice = st.selectbox("Simulation history", cycle_options, key="cycle_choice")
    if cycle_choice == "Previous cycle":
        state = manager.view(use_previous=True)
    history = state["history"]
    event_log = history["log"]
    events_by_type = history["by_type"]
    clock_state = state["clock"]

    st.caption(f"Showing cycle {history['cycle']} · {state['source']}")
    if state["pending_cycle"] and cycle_choice == "Latest available cycle":
        st.caption("A new cycle is starting. The existing history stays visible until the next clinical event arrives.")
    if cycle_choice == "Previous cycle":
        st.caption("This is the retained previous simulation cycle. The live clock below refers to the shared connection.")
    st.caption(f"Last event received (UTC): {state['last_event_at'] or 'Not yet received'} · Last saved (UTC): {state['saved_at'] or 'Not yet saved'}")
    if not event_log and not events_by_type:
        st.info("No retained history yet. Click Start live stream to populate the dashboard for the first time.")

    # Build the patient list from both rolling buffers (rare event types can
    # remain in the per-type history even after leaving the overall raw log).
    patients = sorted({ev.get("patient_id") for ev in event_log if isinstance(ev.get("patient_id"), str)} |
                      {ev.get("patient_id") for values in events_by_type.values() for ev in values
                       if isinstance(ev.get("patient_id"), str)})
    patient_options = ["All patients"] + patients
    if st.session_state.get("patient_filter") not in patient_options:
        st.session_state.patient_filter = patient_options[0]
    patient_filter = st.selectbox("Patient shown in graphs", patient_options, key="patient_filter")

    metrics = st.columns(4)
    metrics[0].metric("Recent events", len(event_log))
    metrics[1].metric("Active event types", sum(bool(events) for events in events_by_type.values()))
    metrics[2].metric("Patients observed", len(patients))
    metrics[3].metric("Lagging (live clock)", "YES" if clock_state.get("lagging") else "No")
    if clock_state.get("sim_time"):
        st.caption(f"Shared simulated time: {clock_state['sim_time']} · Speed: {clock_state.get('speed_label') or TEAM_CONFIG['speed']}")
    if clock_state.get("lagging"):
        st.warning("The API reports lagging. Consider lowering the simulation speed or cohort size if this persists.")

    # Only one event type is rendered during live streaming. All 20 event
    # types and their original code examples remain accessible from the menu.
    show_only_active = st.checkbox("Only show event types already received", value=False, key="only_active")
    available_types = [kind for kind in EVENT_DEFS if not show_only_active or events_by_type.get(kind)]
    show_all = False
    if not watching:
        show_all = st.checkbox("Show all event types (historical view)", value=False, key="show_all_history")
    if available_types:
        if show_all:
            for event_type in available_types:
                show_event_type(event_type, patient_filter, events_by_type)
        else:
            if st.session_state.get("event_type_choice") not in available_types:
                st.session_state.event_type_choice = available_types[0]
            selected = st.selectbox(
                "Data type to explore", available_types,
                format_func=lambda kind: f"{EVENT_DEFS[kind]['title']} — {kind}",
                key="event_type_choice",
            )
            show_event_type(selected, patient_filter, events_by_type)
    else:
        st.info("No received event types match the selected history. Uncheck the active-types filter to see API examples.")

    st.divider()
    st.header("Raw stream inspector")
    # Rendering the raw JSON table is opt-in, to avoid repeated serialization
    # of 100 records for every student every five seconds.
    if st.checkbox("Show recent raw events", value=False, key="raw_inspector"):
        recent = [ev for ev in reversed(event_log)
                  if patient_filter == "All patients" or ev.get("patient_id") == patient_filter][:100]
        if recent:
            rows = [{
                "event_time": ev.get("event_time"),
                "available_at": ev.get("available_at"),
                "patient_id": ev.get("patient_id"),
                "event_type": ev.get("event_type"),
                "quality": (ev.get("quality") or {}).get("status"),
                "payload": json.dumps(ev.get("payload"), ensure_ascii=False),
            } for ev in recent]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("No retained events match this selection.")

    with st.expander("Connection diagnostics", expanded=False):
        st.write("Connected:", state["connected"])
        st.write("Team key created:", state["team_created"])
        st.write("Background thread alive:", state["thread_alive"])
        st.write("Shared stream state:", state["phase"])
        st.write("Live viewers:", state["viewers"])
        st.write("Unique events received since this process started:", state["total_received"])
        st.write("Queue:", "None — events are written directly to bounded rolling buffers")
        st.write("Snapshot storage:", "Local + PostgreSQL" if state["database_configured"] else "Local (best effort on Community Cloud)")
        st.code(f"{BASE}/api/v1/team-streams/YOUR_TEAM_KEY/stream?backfill_hours={BACKFILL_HOURS}", language=None)
        st.caption("The shared team key is not displayed. Use your own team key in your application.")

    if not watching and (event_log or events_by_type):
        st.divider()
        # Preparing the export only on request avoids retaining large per-user
        # downloads during normal historical browsing.
        if st.button("Prepare retained history download", key="prepare_history"):
            st.download_button(
                "Download retained history JSON", data=manager.export(),
                file_name="sample_data.json", mime="application/json", key="download_history",
                on_click="ignore",
            )
            st.caption("Includes the latest and previous retained cycles. Only commit it to GitHub if the data is suitable for public sharing.")


dashboard()
