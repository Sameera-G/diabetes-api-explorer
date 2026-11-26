
import json
import queue
import threading
import time
from collections import defaultdict, deque
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

class StreamManager:
    """Persistent state that survives Streamlit reruns."""

    def __init__(self):
        self.incoming = queue.Queue()
        self.events_by_type = defaultdict(
            lambda: deque(maxlen=MAX_EVENTS_PER_TYPE)
        )
        self.latest_by_type = {}
        self.event_log = deque(maxlen=MAX_LOG_EVENTS)

        self.clock_state = {
            "sim_time": None,
            "speed_label": None,
            "loop": None,
            "lagging": False,
        }

        self.stream_state = {
            "connected": False,
            "error": None,
            "team_key": None,
        }

        self.thread = None
        self.lock = threading.Lock()


@st.cache_resource
def get_manager():
    # cache_resource keeps this same Python object across Streamlit reruns.
    return StreamManager()


manager = get_manager()

incoming = manager.incoming
events_by_type = manager.events_by_type
latest_by_type = manager.latest_by_type
event_log = manager.event_log
clock_state = manager.clock_state
stream_state = manager.stream_state


def get_or_create_team_key():
    if TEAM_KEY.strip():
        return TEAM_KEY.strip()

    r = requests.post(
        f"{BASE}/api/v1/team-streams",
        json=TEAM_CONFIG,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["team_key"]


def stream_worker(team_key):
    url = (
        f"{BASE}/api/v1/team-streams/{team_key}/stream"
        f"?backfill_hours={BACKFILL_HOURS}"
    )

    while True:
        try:
            stream_state["connected"] = False
            stream_state["error"] = None

            response = requests.get(
                url,
                stream=True,
                timeout=(10, 90),
                headers={"Accept": "text/event-stream"},
            )
            response.raise_for_status()

            client = SSEClient(response)
            stream_state["connected"] = True

            for msg in client.events():
                if msg.event == "clinical_event":
                    incoming.put(("clinical_event", json.loads(msg.data)))
                elif msg.event == "clock":
                    incoming.put(("clock", json.loads(msg.data)))
                elif msg.event == "loop_restart":
                    incoming.put(("loop_restart", {}))
                elif msg.event == "stream_complete":
                    incoming.put(("stream_complete", {}))
                    return

        except Exception as exc:
            stream_state["connected"] = False
            stream_state["error"] = str(exc)
            time.sleep(2)


def drain_incoming():
    while True:
        try:
            kind, data = incoming.get_nowait()
        except queue.Empty:
            break

        if kind == "clinical_event":
            event_type = data.get("event_type", "unknown")
            events_by_type[event_type].append(data)
            latest_by_type[event_type] = data
            event_log.append(data)

        elif kind == "clock":
            clock_state.update(data)

        elif kind == "loop_restart":
            events_by_type.clear()
            latest_by_type.clear()
            event_log.clear()

        elif kind == "stream_complete":
            stream_state["connected"] = False


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
GET {BASE}/api/v1/patients/{encoded}/events?event_types={event_type}&limit=100&view=full

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


def rows_for_event_type(event_type, patient_filter):
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


def make_chart(event_type, patient_filter):
    rows = rows_for_event_type(event_type, patient_filter)

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


def latest_for_type(event_type, patient_filter):
    if patient_filter == "All patients":
        return latest_by_type.get(event_type)

    for ev in reversed(events_by_type.get(event_type, [])):
        if ev.get("patient_id") == patient_filter:
            return ev

    return None


def status_table(event_type, patient_filter):
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


st.set_page_config(
    page_title="Diabetes API Simulation Explorer",
    page_icon="📡",
    layout="wide",
)

st.title("📡 Diabetes App Lab — API Simulation Explorer")
st.caption(
    "Educational synthetic simulation. Not medical advice or a clinical monitoring device."
)

st.info(
    "Every data type below has a live visualization, the latest raw JSON "
    "received from the API, and copyable Python/JavaScript integration code."
)

st.caption(
    "The first connection can take a few seconds. Once connected, the "
    "Received events counter should begin increasing. If it stays at 0, "
    "check the Connection diagnostics section below."
)

# Start exactly one SSE thread for this Streamlit server process.
# The thread and its queues live inside the cached StreamManager.
if manager.thread is None or not manager.thread.is_alive():
    try:
        team_key = get_or_create_team_key()
        stream_state["team_key"] = team_key

        manager.thread = threading.Thread(
            target=stream_worker,
            args=(team_key,),
            daemon=True,
        )
        manager.thread.start()

    except Exception as exc:
        st.error(f"Could not start stream: {exc}")

drain_incoming()

with st.sidebar:
    st.header("Simulation")

    if stream_state["connected"]:
        st.success("Live stream connected")
    else:
        st.warning("Connecting / reconnecting")

    if stream_state["error"]:
        st.error(stream_state["error"])

    st.write("**Simulated time**")
    st.code(clock_state.get("sim_time") or "waiting...")

    st.write("**Speed**")
    st.write(clock_state.get("speed_label") or TEAM_CONFIG["speed"])

    if clock_state.get("lagging"):
        st.warning(
            "Stream is lagging. Reduce patient count, speed, or domains."
        )

    patients = sorted({
        ev.get("patient_id")
        for ev in event_log
        if ev.get("patient_id")
    })

    patient_filter = st.selectbox(
        "Patient shown in graphs",
        ["All patients"] + patients,
    )

    show_only_active = st.checkbox(
        "Only show event types already received",
        value=False,
    )

    st.write("**Events in memory**")
    st.write(len(event_log))

with st.expander("Connection diagnostics", expanded=not stream_state["connected"]):
    st.write("Connected:", stream_state["connected"])
    st.write("Team key created:", bool(stream_state["team_key"]))
    st.write("Background thread alive:", bool(manager.thread and manager.thread.is_alive()))
    st.write("Pending events in queue:", incoming.qsize())
    st.write("Events processed by dashboard:", len(event_log))
    st.write("Simulated clock:", clock_state.get("sim_time"))
    if stream_state["error"]:
        st.error(stream_state["error"])
    if stream_state["team_key"]:
        st.code(
            f"{BASE}/api/v1/team-streams/{stream_state['team_key']}/stream?backfill_hours={BACKFILL_HOURS}",
            language=None,
        )

metrics = st.columns(4)
metrics[0].metric("Events", len(event_log))
metrics[1].metric("Active event types", len(events_by_type))
metrics[2].metric(
    "Patients observed",
    len({e.get("patient_id") for e in event_log if e.get("patient_id")}),
)
metrics[3].metric(
    "Lagging",
    "YES" if clock_state.get("lagging") else "No",
)

event_types_to_show = list(EVENT_DEFS.keys())

if show_only_active:
    event_types_to_show = [
        event_type
        for event_type in event_types_to_show
        if event_type in events_by_type
    ]

for event_type in event_types_to_show:
    definition = EVENT_DEFS[event_type]

    count = sum(
        1
        for ev in events_by_type.get(event_type, [])
        if patient_filter == "All patients"
        or ev.get("patient_id") == patient_filter
    )

    st.divider()
    st.subheader(
        f"{definition['title']} · `{event_type}` · {count} recent events"
    )
    st.caption(f"Domain: {definition['domain']}")

    graph_col, json_col = st.columns([1.45, 1], gap="large")

    with graph_col:
        if definition["fields"]:
            chart = make_chart(event_type, patient_filter)

            if chart is not None:
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("Waiting for numeric events of this type...")
        else:
            df = status_table(event_type, patient_filter)

            if not df.empty:
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    height=330,
                )
            else:
                st.info("Waiting for events of this type...")

    with json_col:
        st.markdown("#### Latest event JSON")
        latest = latest_for_type(event_type, patient_filter)

        if latest:
            st.code(
                json.dumps(latest, indent=2, ensure_ascii=False),
                language="json",
            )
        else:
            st.code(
                json.dumps(
                    {
                        "waiting_for": event_type,
                        "message": "No event received yet.",
                    },
                    indent=2,
                ),
                language="json",
            )

    st.markdown("#### How your application gets this data")

    py_tab, js_tab, rest_tab = st.tabs(
        ["Python SSE", "JavaScript / Web SSE", "REST history"]
    )

    with py_tab:
        st.code(
            python_sse_example(event_type),
            language="python",
        )

    with js_tab:
        st.code(
            javascript_sse_example(event_type),
            language="javascript",
        )

    with rest_tab:
        example_patient = (
            patient_filter
            if patient_filter != "All patients"
            else "synthetic-diabetes-v1:P00001"
        )

        st.code(
            rest_example(event_type, example_patient),
            language="python",
        )

st.divider()
st.header("Raw live stream inspector")

recent = list(reversed(event_log))[:100]

if recent:
    rows = []

    for ev in recent:
        rows.append({
            "event_time": ev.get("event_time"),
            "available_at": ev.get("available_at"),
            "patient_id": ev.get("patient_id"),
            "event_type": ev.get("event_type"),
            "quality": (ev.get("quality") or {}).get("status"),
            "payload": json.dumps(ev.get("payload"), ensure_ascii=False),
        })

    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("Waiting for the first clinical event...")

time.sleep(0.8)
st.rerun()
