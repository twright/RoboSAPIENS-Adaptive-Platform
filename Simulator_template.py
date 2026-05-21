# ============================================================
# Managed System Simulator Template
# ============================================================
# A Dash app that simulates the managed system and interfaces
# with the MAPE-K loop via the same broker defined in config.yaml.
#
# What it does:
#   - Publishes synthetic sensor readings to the sensor topic
#   - Subscribes to the action topic (command from Execute node)
#   - Provides Start / Stop / Toggle-Anomaly controls
#   - Shows real-time charts for all sensor fields
#   - Displays the most recent control command received
#
# Usage:
#   cd <case_name>
#   python simulator.py
# ============================================================

import json
import time
import threading
import math
import random

import yaml
import dash
from dash import dcc, html, Input, Output, ctx
import plotly.graph_objs as go
from rpclpy.CommunicationManager import CommunicationManager

# Load infrastructure settings from the generated config
try:
    with open('config.yaml') as f:
        _config = yaml.safe_load(f)
except FileNotFoundError:
    raise Exception("config.yaml not found — run generate_maplek.py first")

_infra = _config.get('Monitor_Config', {}).get('Event_Manager_Config', {})
_PROTOCOL = _infra.get('protocol', 'redis')
_HOST = _infra.get('host', 'localhost')
_PORT = _infra.get('port', 6379)

# TODO: replace with actual topic strings from the spec
_SENSOR_TOPIC = '/SensorData'     # topic to PUBLISH sensor readings on
_ACTION_TOPIC = '/ActionCommand'  # topic to SUBSCRIBE to for control commands
_SENSOR_ID = 'sensor_1'          # static identifier published with every reading

_PUBLISH_INTERVAL = 0.5          # seconds between readings


# ── Simulation State ──────────────────────────────────────────────────────────

class SimulationState:
    """Thread-safe container for runtime state and sensor history."""

    def __init__(self, max_history: int = 300):
        self._lock = threading.Lock()
        self.running = False
        self.anomaly_mode = False
        self._history = []          # list of {"t": float, **sensor_fields}
        self._max_history = max_history
        self._last_command = None   # most recent dict from Execute

    def add(self, reading: dict):
        with self._lock:
            self._history.append(reading)
            if len(self._history) > self._max_history:
                self._history.pop(0)

    def snapshot(self):
        with self._lock:
            return list(self._history)

    def set_command(self, cmd: dict):
        with self._lock:
            self._last_command = cmd

    def get_command(self):
        with self._lock:
            return self._last_command


_sim = SimulationState()


# ── Sensor Model ──────────────────────────────────────────────────────────────

def _generate_reading(t: float, anomaly: bool) -> dict:
    """
    Return one synthetic sensor reading as a dict.

    Normal mode  — healthy, low-noise operation.
    Anomaly mode — degraded behaviour: higher noise, drift, or fault patterns.

    TODO: Replace the placeholder fields and formulas with realistic models
          that match your spec's sensor.fields.  Use math.sin / math.cos for
          periodic signals; add random.gauss(mu, sigma) for sensor noise.
    """
    # ── Normal mode ───────────────────────────────────────────
    # TODO: replace "sensor_id", "value" with your actual field names
    reading = {
        "sensor_id": _SENSOR_ID,
        "value": math.sin(t) + random.gauss(0, 0.05),
    }

    # ── Anomaly mode — amplify noise, add bias, inject spikes ─
    if anomaly:
        # TODO: model your specific fault mode, e.g.:
        reading["value"] = reading["value"] * 5.0 + random.gauss(0, 1.0)

    reading["_ts"] = time.time()
    return reading


# ── Communication ─────────────────────────────────────────────────────────────

_cm = CommunicationManager({"protocol": _PROTOCOL, "host": _HOST, "port": _PORT})


def _on_command(msg: str):
    """Handle a control command published by the MAPE-K Execute node."""
    try:
        _sim.set_command(json.loads(msg))
    except Exception:
        pass


_cm.subscribe(_ACTION_TOPIC, _on_command)
_cm.start()


def _publisher_loop():
    """Background daemon: generate and publish sensor readings at fixed rate."""
    t = 0.0
    while True:
        if _sim.running:
            reading = _generate_reading(t, _sim.anomaly_mode)
            _sim.add({"t": time.time(), **reading})
            try:
                _cm.publish(_SENSOR_TOPIC, json.dumps(reading))
            except Exception:
                pass
            t += _PUBLISH_INTERVAL
        time.sleep(_PUBLISH_INTERVAL)


threading.Thread(target=_publisher_loop, daemon=True).start()


# ── Dash App ──────────────────────────────────────────────────────────────────

_CHART_FIELDS = ["value"]  # TODO: one entry per numeric sensor field to chart

app = dash.Dash(__name__)
app.title = "Managed System Simulator"

_BTN_BASE = {
    "border": "none", "borderRadius": "6px", "cursor": "pointer",
    "fontSize": "14px", "padding": "10px 20px", "color": "white",
}

app.layout = html.Div([
    html.H1("Managed System Simulator",
            style={"textAlign": "center", "color": "#2c3e50",
                   "fontWeight": "300", "marginBottom": "5px"}),
    html.P("Simulates the managed system and feeds data to the MAPE-K loop.",
           style={"textAlign": "center", "color": "#7f8c8d",
                  "marginBottom": "20px", "fontSize": "0.9rem"}),

    # ── Control bar ──────────────────────────────────────────
    html.Div([
        html.Button("▶ Start",          id="btn-start",   n_clicks=0,
                    style={**_BTN_BASE, "backgroundColor": "#27ae60", "marginRight": "8px"}),
        html.Button("■ Stop",           id="btn-stop",    n_clicks=0,
                    style={**_BTN_BASE, "backgroundColor": "#c0392b", "marginRight": "8px"}),
        html.Button("⚠ Inject Anomaly", id="btn-anomaly", n_clicks=0,
                    style={**_BTN_BASE, "backgroundColor": "#e67e22"}),
        html.Span(id="status-label",
                  style={"marginLeft": "20px", "fontWeight": "600",
                         "fontSize": "14px", "color": "#2c3e50"}),
    ], style={
        "display": "flex", "alignItems": "center",
        "backgroundColor": "#ecf0f1", "padding": "14px 20px",
        "borderRadius": "8px", "marginBottom": "20px",
        "boxShadow": "0 1px 4px rgba(0,0,0,0.07)",
    }),

    # ── Sensor charts (one per numeric field) ────────────────
    html.Div([
        html.Div([
            dcc.Graph(id=f"chart-{field}", style={"height": "230px"})
        ], style={
            "backgroundColor": "#fff", "borderRadius": "8px",
            "border": "1px solid #e9ecef", "padding": "10px",
            "marginBottom": "15px",
            "boxShadow": "0 1px 4px rgba(0,0,0,0.06)",
        })
        for field in _CHART_FIELDS
    ]),

    # ── Last command panel ───────────────────────────────────
    html.Div([
        html.H3("Latest Control Command (from Execute Node)",
                style={"color": "#2c3e50", "fontWeight": "400",
                       "fontSize": "1rem", "marginBottom": "8px"}),
        html.Pre(id="last-command",
                 style={
                     "backgroundColor": "#f8f9fa", "padding": "12px",
                     "borderRadius": "6px", "border": "1px solid #dee2e6",
                     "fontFamily": "monospace", "fontSize": "12px",
                     "minHeight": "60px", "whiteSpace": "pre-wrap",
                 }),
    ], style={
        "backgroundColor": "#fff", "padding": "16px",
        "borderRadius": "8px", "border": "1px solid #e9ecef",
        "boxShadow": "0 1px 4px rgba(0,0,0,0.06)",
    }),

    dcc.Interval(id="ticker", interval=500, n_intervals=0),
], style={
    "backgroundColor": "#f5f6fa", "minHeight": "100vh",
    "padding": "24px",
    "fontFamily": '"Segoe UI", Tahoma, Geneva, Verdana, sans-serif',
})


# ── Callbacks ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("status-label", "children"),
    Output("btn-anomaly", "style"),
    Input("btn-start",   "n_clicks"),
    Input("btn-stop",    "n_clicks"),
    Input("btn-anomaly", "n_clicks"),
    prevent_initial_call=True,
)
def handle_controls(*_):
    triggered = ctx.triggered_id
    if triggered == "btn-start":
        _sim.running = True
    elif triggered == "btn-stop":
        _sim.running = False
    elif triggered == "btn-anomaly":
        _sim.anomaly_mode = not _sim.anomaly_mode

    state_txt = "Running" if _sim.running else "Stopped"
    mode_txt  = "ANOMALY ⚠" if _sim.anomaly_mode else "Normal"
    btn_color = "#c0392b" if _sim.anomaly_mode else "#e67e22"
    return (
        f"State: {state_txt}  |  Mode: {mode_txt}",
        {**_BTN_BASE, "backgroundColor": btn_color},
    )


# TODO: generate one callback per chart field (chart-<field>)
@app.callback(
    Output("chart-value", "figure"),
    Input("ticker", "n_intervals"),
)
def update_chart_value(_):
    history = _sim.snapshot()
    now = time.time()
    xs = [h["t"] - now for h in history]
    ys = [h.get("value", 0) for h in history]
    anomaly_color = "#e74c3c" if _sim.anomaly_mode else "#3498db"
    fig = go.Figure(go.Scatter(
        x=xs, y=ys, mode="lines",
        line=dict(color=anomaly_color, width=2),
        name="value",
    ))
    fig.update_layout(
        title={"text": "value", "x": 0.5, "font": {"size": 13}},
        xaxis={"title": "Time (s ago)", "range": [-60, 0]},
        yaxis={"title": "value"},  # TODO: add units
        template="plotly_white",
        margin=dict(l=50, r=20, t=35, b=30),
        height=220,
    )
    return fig


@app.callback(
    Output("last-command", "children"),
    Input("ticker", "n_intervals"),
)
def update_command(_):
    cmd = _sim.get_command()
    return "No command received yet." if cmd is None else json.dumps(cmd, indent=2)


def main():
    app.run(host="127.0.0.1", port=8060, debug=False)


if __name__ == "__main__":
    main()
