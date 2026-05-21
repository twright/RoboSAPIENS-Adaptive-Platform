import json
import math
import time
import threading

import yaml
import dash
from dash import dcc, html, Input, Output, ctx
import plotly.graph_objs as go
from rpclpy.CommunicationManager import CommunicationManager

try:
    with open('config.yaml') as f:
        _config = yaml.safe_load(f)
except FileNotFoundError:
    raise Exception("config.yaml not found — run generate_maplek.py first")

_infra     = _config.get('Monitor_Config', {}).get('Event_Manager_Config', {})
_PROTOCOL  = _infra.get('protocol', 'redis')
_HOST      = _infra.get('host', 'localhost')
_PORT      = _infra.get('port', 6379)

_SENSOR_TOPIC     = '/HumanStatus'
_ACTION_TOPIC     = '/NewWaypoint'
_PUBLISH_INTERVAL = 0.5
_ENV_SIZE         = 10.0
_ROBOT_SPEED      = 0.35   # m/s
_SAFETY_RADIUS    = 1.5    # visual ring around each robot waypoint

_ROBOT_COLORS = {
    'robot_1': '#3498db',
    'robot_2': '#e74c3c',
    'robot_3': '#2ecc71',
}

# Warehouse shelving units: (x0, y0, x1, y1)
MAZE_WALLS = [
    (1.0, 2.5, 3.5, 3.0),   # shelf A — lower-left
    (6.5, 2.5, 9.0, 3.0),   # shelf B — lower-right
    (3.5, 4.5, 6.5, 5.0),   # shelf C — centre
    (1.0, 7.0, 3.5, 7.5),   # shelf D — upper-left
    (6.5, 7.0, 9.0, 7.5),   # shelf E — upper-right
    (3.5, 8.5, 6.5, 9.0),   # shelf F — top-centre
    (4.5, 1.0, 5.0, 3.0),   # pillar G — centre-bottom
]

# Each robot has a fixed start and goal; it loops start→goal→start indefinitely
ROBOT_DEFS = {
    'robot_1': {'start': [0.5, 0.5], 'goal': [4.0, 9.5]},
    'robot_2': {'start': [9.5, 0.5], 'goal': [6.0, 9.5]},
    'robot_3': {'start': [0.5, 5.0], 'goal': [9.5, 5.0]},
}


class _Robot:
    def __init__(self, start, goal):
        self.start    = list(start)
        self.goal     = list(goal)
        self.pos      = list(start)
        self.waypoint = list(goal)   # immediate target; may be a MAPE-K detour

    def step(self, dt: float):
        tx, ty = self.waypoint
        dx, dy = tx - self.pos[0], ty - self.pos[1]
        dist   = math.hypot(dx, dy)
        if dist < 0.1:
            if [round(self.waypoint[0], 2), round(self.waypoint[1], 2)] != \
               [round(self.goal[0],     2), round(self.goal[1],     2)]:
                self.waypoint = list(self.goal)   # detour reached → head to original goal
            else:
                self.pos      = list(self.start)  # goal reached → loop back to start
                self.waypoint = list(self.goal)
            return
        s = min(_ROBOT_SPEED * dt, dist)
        self.pos[0] += dx / dist * s
        self.pos[1] += dy / dist * s


# ── Simulation state ───────────────────────────────────────────────────────────

class SimulationState:
    def __init__(self):
        self._lock          = threading.Lock()
        self.running        = False
        self.human_x        = 5.5
        self.human_y        = 0.5
        self.human_target_x = 5.5
        self.human_target_y = 9.5
        self.human_speed    = 0.5
        self.human_velocity = 0.0
        self.robots         = {rid: _Robot(d['start'], d['goal'])
                                for rid, d in ROBOT_DEFS.items()}
        self._last_command  = None
        self._vel_history   = []
        self._max_history   = 300

    def step(self, dt: float):
        with self._lock:
            dx   = self.human_target_x - self.human_x
            dy   = self.human_target_y - self.human_y
            dist = math.hypot(dx, dy)
            if dist < 0.05:
                self.human_velocity = 0.0
            else:
                s = min(self.human_speed * dt, dist)
                self.human_x       += dx / dist * s
                self.human_y       += dy / dist * s
                self.human_velocity = self.human_speed
                self._vel_history.append((time.time(), self.human_velocity))
                if len(self._vel_history) > self._max_history:
                    self._vel_history.pop(0)
            for robot in self.robots.values():
                robot.step(dt)

    def set_target(self, x: float, y: float):
        with self._lock:
            self.human_target_x = max(0.0, min(_ENV_SIZE, x))
            self.human_target_y = max(0.0, min(_ENV_SIZE, y))

    def set_speed(self, speed: float):
        with self._lock:
            self.human_speed = speed

    def apply_command(self, cmd: dict):
        with self._lock:
            self._last_command = cmd
            wp = cmd.get('new_waypoint', {})
            if isinstance(wp, dict) and 'robot_id' in wp:
                rid = wp['robot_id']
                if rid in self.robots:
                    self.robots[rid].waypoint = [
                        float(wp.get('x', self.robots[rid].pos[0])),
                        float(wp.get('y', self.robots[rid].pos[1])),
                    ]

    def snapshot(self):
        with self._lock:
            return {
                'human_x':        self.human_x,
                'human_y':        self.human_y,
                'human_velocity': self.human_velocity,
                'target_x':       self.human_target_x,
                'target_y':       self.human_target_y,
                'robots': {
                    rid: {
                        'pos':      list(r.pos),
                        'waypoint': list(r.waypoint),
                        'goal':     list(r.goal),
                        'start':    list(r.start),
                    }
                    for rid, r in self.robots.items()
                },
                'vel_history':  list(self._vel_history),
                'last_command': self._last_command,
            }


_sim = SimulationState()


# ── Communication ──────────────────────────────────────────────────────────────

_cm = CommunicationManager({'protocol': _PROTOCOL, 'host': _HOST, 'port': _PORT})


def _on_waypoint(msg: str):
    try:
        _sim.apply_command(json.loads(msg))
    except Exception:
        pass


_cm.subscribe(_ACTION_TOPIC, _on_waypoint)
_cm.start()


def _publisher_loop():
    last = time.time()
    while True:
        now  = time.time()
        dt   = now - last
        last = now
        if _sim.running:
            _sim.step(dt)
            snap    = _sim.snapshot()
            payload = {
                'human_position': f"{snap['human_x']:.3f},{snap['human_y']:.3f}",
                'human_velocity': snap['human_velocity'],
            }
            try:
                _cm.publish(_SENSOR_TOPIC, json.dumps(payload))
            except Exception:
                pass
        time.sleep(_PUBLISH_INTERVAL)


threading.Thread(target=_publisher_loop, daemon=True).start()


# ── Dash app ───────────────────────────────────────────────────────────────────

app       = dash.Dash(__name__)
app.title = "Multi Robot Navigation Simulator"

_BTN = {
    'border': 'none', 'borderRadius': '6px', 'cursor': 'pointer',
    'fontSize': '14px', 'padding': '10px 20px', 'color': 'white',
}

app.layout = html.Div([
    html.H1("Multi-Robot Navigation Simulator",
            style={'textAlign': 'center', 'color': '#2c3e50',
                   'fontWeight': '300', 'marginBottom': '4px'}),
    html.P("Click the map to set the human's target position.",
           style={'textAlign': 'center', 'color': '#7f8c8d', 'marginBottom': '18px'}),

    # Control bar
    html.Div([
        html.Button('▶ Start', id='btn-start', n_clicks=0,
                    style={**_BTN, 'backgroundColor': '#27ae60', 'marginRight': '8px'}),
        html.Button('■ Stop',  id='btn-stop',  n_clicks=0,
                    style={**_BTN, 'backgroundColor': '#c0392b', 'marginRight': '20px'}),
        html.Span('Human speed (m/s):', style={'fontWeight': '600', 'marginRight': '10px'}),
        html.Div(dcc.Slider(
            id='speed-slider', min=0.1, max=3.0, step=0.1, value=0.5,
            marks={0.1: '0.1', 1.0: '1.0', 2.0: '2.0', 3.0: '3.0'},
            tooltip={'placement': 'bottom', 'always_visible': False},
        ), style={'width': '220px', 'display': 'inline-block'}),
        html.Span(id='status-label',
                  style={'marginLeft': '20px', 'fontWeight': '600',
                         'fontSize': '14px', 'color': '#2c3e50'}),
    ], style={
        'display': 'flex', 'alignItems': 'center', 'flexWrap': 'wrap', 'gap': '6px',
        'backgroundColor': '#ecf0f1', 'padding': '14px 20px',
        'borderRadius': '8px', 'marginBottom': '18px',
        'boxShadow': '0 1px 4px rgba(0,0,0,0.07)',
    }),

    # Map + velocity chart
    html.Div([
        html.Div([
            dcc.Graph(id='map-graph', style={'height': '520px'},
                      config={'displayModeBar': False}),
        ], style={
            'flex': '1', 'backgroundColor': '#fff', 'borderRadius': '8px',
            'border': '1px solid #e9ecef', 'padding': '10px',
            'boxShadow': '0 1px 4px rgba(0,0,0,0.06)',
        }),
        html.Div([
            dcc.Graph(id='velocity-chart', style={'height': '520px'}),
        ], style={
            'flex': '0 0 340px', 'backgroundColor': '#fff', 'borderRadius': '8px',
            'border': '1px solid #e9ecef', 'padding': '10px',
            'boxShadow': '0 1px 4px rgba(0,0,0,0.06)',
        }),
    ], style={'display': 'flex', 'gap': '15px', 'marginBottom': '18px'}),

    # Last command panel
    html.Div([
        html.H3('Latest NewWaypoint Command (from Execute Node)',
                style={'color': '#2c3e50', 'fontWeight': '400',
                       'fontSize': '1rem', 'marginBottom': '8px'}),
        html.Pre(id='last-command',
                 style={
                     'backgroundColor': '#f8f9fa', 'padding': '12px',
                     'borderRadius': '6px', 'border': '1px solid #dee2e6',
                     'fontFamily': 'monospace', 'fontSize': '12px',
                     'minHeight': '60px', 'whiteSpace': 'pre-wrap',
                 }),
    ], style={
        'backgroundColor': '#fff', 'padding': '16px', 'borderRadius': '8px',
        'border': '1px solid #e9ecef', 'boxShadow': '0 1px 4px rgba(0,0,0,0.06)',
    }),

    dcc.Interval(id='ticker', interval=500, n_intervals=0),

], style={
    'backgroundColor': '#f5f6fa', 'minHeight': '100vh', 'padding': '24px',
    'fontFamily': '"Segoe UI", Tahoma, Geneva, Verdana, sans-serif',
})


# ── Callbacks ──────────────────────────────────────────────────────────────────

@app.callback(
    Output('status-label', 'children'),
    Input('btn-start',    'n_clicks'),
    Input('btn-stop',     'n_clicks'),
    Input('speed-slider', 'value'),
    prevent_initial_call=True,
)
def handle_controls(_start, _stop, speed):
    triggered = ctx.triggered_id
    if triggered == 'btn-start':
        _sim.running = True
    elif triggered == 'btn-stop':
        _sim.running = False
    elif triggered == 'speed-slider':
        _sim.set_speed(speed)
    state = 'Running' if _sim.running else 'Stopped'
    return f'State: {state}  |  Speed: {_sim.human_speed:.1f} m/s'


@app.callback(
    Output('map-graph', 'figure'),
    Input('ticker', 'n_intervals'),
)
def update_map(_):
    snap   = _sim.snapshot()
    fig    = go.Figure()
    angles = [math.radians(a) for a in range(0, 361, 6)]

    # Warehouse shelving walls
    for (x0, y0, x1, y1) in MAZE_WALLS:
        fig.add_shape(
            type='rect', x0=x0, y0=y0, x1=x1, y1=y1,
            fillcolor='rgba(70,70,90,0.60)',
            line=dict(color='#333', width=1.5),
            layer='below',
        )

    # Invisible click grid — lets the user click anywhere in the map
    grid = [i * 0.5 for i in range(int(_ENV_SIZE / 0.5) + 1)]
    fig.add_trace(go.Scatter(
        x=[x for x in grid for _ in grid],
        y=[y for _ in grid for y in grid],
        mode='markers',
        marker=dict(size=18, opacity=0, color='rgba(0,0,0,0)'),
        showlegend=False,
        hovertemplate='Set target: (%{x:.1f}, %{y:.1f})<extra></extra>',
        name='_click_area',
    ))

    for robot_id, rdata in snap['robots'].items():
        color = _ROBOT_COLORS.get(robot_id, '#95a5a6')
        px, py = rdata['pos']
        wx, wy = rdata['waypoint']
        gx, gy = rdata['goal']
        sx, sy = rdata['start']
        label  = robot_id.replace('_', ' ').title()
        has_detour = abs(wx - gx) > 0.15 or abs(wy - gy) > 0.15

        # Full planned path: current pos → waypoint (→ goal if on detour)
        path_xs = [px, wx]
        path_ys = [py, wy]
        if has_detour:
            path_xs.append(gx)
            path_ys.append(gy)
        fig.add_trace(go.Scatter(
            x=path_xs, y=path_ys,
            mode='lines',
            line=dict(color=color, width=2, dash='dash'),
            showlegend=False,
            hoverinfo='skip',
        ))

        # Safety ring around current waypoint
        fig.add_trace(go.Scatter(
            x=[wx + _SAFETY_RADIUS * math.cos(a) for a in angles],
            y=[wy + _SAFETY_RADIUS * math.sin(a) for a in angles],
            mode='lines',
            line=dict(color=color, width=1, dash='dot'),
            showlegend=False,
            hoverinfo='skip',
        ))

        # Goal marker (star)
        fig.add_trace(go.Scatter(
            x=[gx], y=[gy],
            mode='markers',
            marker=dict(size=14, color=color, symbol='star',
                        line=dict(color='white', width=1), opacity=0.75),
            showlegend=False,
            hovertemplate=f'<b>{label} Goal</b><br>({gx:.1f}, {gy:.1f})<extra></extra>',
        ))

        # Start marker (triangle)
        fig.add_trace(go.Scatter(
            x=[sx], y=[sy],
            mode='markers',
            marker=dict(size=10, color=color, symbol='triangle-up',
                        line=dict(color='white', width=1), opacity=0.4),
            showlegend=False,
            hovertemplate=f'<b>{label} Start</b><br>({sx:.1f}, {sy:.1f})<extra></extra>',
        ))

        # Robot body
        fig.add_trace(go.Scatter(
            x=[px], y=[py],
            mode='markers+text',
            marker=dict(size=22, color=color, symbol='square',
                        line=dict(color='white', width=2)),
            text=[label],
            textposition='top center',
            textfont=dict(size=11, color=color),
            name=label,
            hovertemplate=(
                f'<b>{label}</b><br>pos=({px:.2f}, {py:.2f})<br>'
                f'target=({wx:.2f}, {wy:.2f})<extra></extra>'
            ),
        ))

    hx, hy = snap['human_x'], snap['human_y']
    tx, ty = snap['target_x'], snap['target_y']

    # Human → target path hint
    fig.add_trace(go.Scatter(
        x=[hx, tx], y=[hy, ty],
        mode='lines',
        line=dict(color='rgba(230,126,34,0.35)', width=2, dash='dot'),
        showlegend=False,
        hoverinfo='skip',
    ))

    # Human body
    fig.add_trace(go.Scatter(
        x=[hx], y=[hy],
        mode='markers+text',
        marker=dict(size=20, color='#e67e22', symbol='circle',
                    line=dict(color='white', width=2)),
        text=['Human'],
        textposition='top center',
        textfont=dict(size=11, color='#e67e22'),
        name='Human',
        hovertemplate=(
            f'<b>Human</b><br>pos=({hx:.2f}, {hy:.2f})<br>'
            f'speed={snap["human_velocity"]:.2f} m/s<extra></extra>'
        ),
    ))

    # Target marker
    fig.add_trace(go.Scatter(
        x=[tx], y=[ty],
        mode='markers',
        marker=dict(size=12, color='#e67e22', symbol='x',
                    line=dict(color='#e67e22', width=2)),
        name='Target',
        hovertemplate=f'<b>Target</b><br>({tx:.2f}, {ty:.2f})<extra></extra>',
    ))

    fig.update_layout(
        title={'text': 'Warehouse Environment (10 × 10 m)', 'x': 0.5,
               'font': {'size': 14, 'color': '#2c3e50'}},
        xaxis=dict(range=[-0.5, _ENV_SIZE + 0.5], title='X (m)',
                   showgrid=True, gridcolor='rgba(0,0,0,0.06)',
                   zeroline=False, scaleanchor='y', scaleratio=1),
        yaxis=dict(range=[-0.5, _ENV_SIZE + 0.5], title='Y (m)',
                   showgrid=True, gridcolor='rgba(0,0,0,0.06)', zeroline=False),
        template='plotly_white',
        legend=dict(orientation='h', y=-0.15, x=0.5, xanchor='center'),
        margin=dict(l=50, r=20, t=50, b=70),
        height=500,
        clickmode='event+select',
    )
    return fig


@app.callback(
    Output('map-graph', 'figure', allow_duplicate=True),
    Input('map-graph', 'clickData'),
    prevent_initial_call=True,
)
def handle_map_click(click_data):
    if click_data and 'points' in click_data:
        pt = click_data['points'][0]
        _sim.set_target(pt['x'], pt['y'])
    return dash.no_update


@app.callback(
    Output('velocity-chart', 'figure'),
    Input('ticker', 'n_intervals'),
)
def update_velocity(_):
    snap    = _sim.snapshot()
    now     = time.time()
    history = snap['vel_history']
    xs = [h[0] - now for h in history]
    ys = [h[1]       for h in history]
    fig = go.Figure(go.Scatter(
        x=xs, y=ys, mode='lines', name='Human Speed',
        line=dict(color='#e67e22', width=2),
        fill='tozeroy', fillcolor='rgba(230,126,34,0.1)',
    ))
    fig.update_layout(
        title={'text': 'Human Speed', 'x': 0.5, 'font': {'size': 14}},
        xaxis=dict(title='Time (s ago)', range=[-60, 0]),
        yaxis=dict(title='Speed (m/s)', rangemode='nonnegative'),
        template='plotly_white',
        margin=dict(l=50, r=20, t=50, b=40),
        height=500,
    )
    return fig


@app.callback(
    Output('last-command', 'children'),
    Input('ticker', 'n_intervals'),
)
def update_command(_):
    cmd = _sim.snapshot()['last_command']
    return 'No command received yet.' if cmd is None else json.dumps(cmd, indent=2)


def main():
    app.run(host='127.0.0.1', port=8060, debug=False)


if __name__ == '__main__':
    main()
