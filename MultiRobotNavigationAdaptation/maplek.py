import json
import math
import time
import threading

import yaml
from rpclpy.node import Node
from rpclpy.utils import timeit_callback
from rpclpy.DashboardApp import run_dashboard
from messages import *


# Shared environment — mutable goals (Legitimate updates _ROBOT_WAYPOINTS on approval)
_ROBOT_STARTS = {
    'robot_1': [0.5, 0.5],
    'robot_2': [9.5, 0.5],
    'robot_3': [0.5, 5.0],
}
_ROBOT_WAYPOINTS = {
    'robot_1': [4.0, 9.5],
    'robot_2': [6.0, 9.5],
    'robot_3': [9.5, 5.0],
}
_COLLISION_RADIUS = 1.8   # human within this distance of a robot path triggers re-planning
_AVOIDANCE_OFFSET = 2.5   # metres to push the new waypoint away from the human


def _parse_pos(pos_str: str):
    """Parse 'x,y' or '(x,y)' into (float, float)."""
    try:
        cleaned = pos_str.replace('(', '').replace(')', '').strip()
        parts = cleaned.split(',')
        return float(parts[0].strip()), float(parts[1].strip())
    except Exception:
        return 0.0, 0.0


def _dist(ax, ay, bx, by):
    return math.sqrt((ax - bx) ** 2 + (ay - by) ** 2)


def _point_to_segment_dist(px, py, ax, ay, bx, by):
    """Minimum distance from point (px,py) to line segment (ax,ay)–(bx,by)."""
    dx, dy = bx - ax, by - ay
    seg_sq = dx * dx + dy * dy
    if seg_sq < 1e-9:
        return math.sqrt((px - ax) ** 2 + (py - ay) ** 2)
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / seg_sq))
    return math.sqrt((px - (ax + t * dx)) ** 2 + (py - (ay + t * dy)) ** 2)


class Monitor(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Monitor"
        self.logger.info("Monitor instantiated")

        _baseline_score = BaselineScore()
        _baseline_score.score = None
        self.write_knowledge(_baseline_score)

        _ratio_list = AnomalyRatioList()
        _ratio_list.ratio_list = []
        self.write_knowledge(_ratio_list)

        _obs_list = ObservationList()
        _obs_list.observations = []
        self.write_knowledge(_obs_list)

        _in_planning = InPlanning()
        _in_planning.in_planning = False
        self.write_knowledge(_in_planning)

        _abnormal_start_idx = AbnormalStartIdx()
        _abnormal_start_idx.abnormal_start_idx = None
        self.write_knowledge(_abnormal_start_idx)

        _replanning_counter = ReplanningCounter()
        _replanning_counter.replanning_counter = 0
        self.write_knowledge(_replanning_counter)

    @timeit_callback
    def monitor(self, msg):
        data = json.loads(msg)
        human_position = data.get('human_position', '0.0,0.0')
        human_velocity = data.get('human_velocity', 0.0)

        score = abs(human_velocity)

        baseline_obj = self.read_knowledge(BaselineScore)
        if baseline_obj.score is None:
            baseline_obj.score = score if score > 0.0 else 0.1
            self.write_knowledge(baseline_obj)

        ratio_list_obj = self.read_knowledge(AnomalyRatioList)
        ratio_list_obj.ratio_list.append(score / baseline_obj.score)
        self.write_knowledge(ratio_list_obj)

        obs_list_obj = self.read_knowledge(ObservationList)
        obs_list_obj.observations.append(data)
        self.write_knowledge(obs_list_obj)

        in_planning_obj = self.read_knowledge(InPlanning)
        if not in_planning_obj.in_planning:
            self.publish_event(event_key='/new_data')

    def register_callbacks(self):
        self.register_event_callback(event_key='/HumanStatus', callback=self.monitor)


class Analysis(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Analysis"
        self.logger.info("Analysis instantiated")

    @timeit_callback
    def analysis(self, msg):
        detect_window_size = 5

        obs_list = self.read_knowledge(ObservationList).observations
        if len(obs_list) < detect_window_size:
            return

        recent = obs_list[-detect_window_size:]

        # Detect which robot's path (start → current goal) the human is closest to
        affected_robot = None
        min_dist = float('inf')

        for obs in recent:
            hx, hy = _parse_pos(obs.get('human_position', '0.0,0.0'))
            for robot_id, wp in _ROBOT_WAYPOINTS.items():
                start = _ROBOT_STARTS.get(robot_id, [0.0, 0.0])
                d = _point_to_segment_dist(hx, hy, start[0], start[1], wp[0], wp[1])
                if d < min_dist:
                    min_dist = d
                    affected_robot = robot_id

        # Only trigger adaptation when the human is close enough to a robot path
        if affected_robot is None or min_dist > _COLLISION_RADIUS:
            return

        # Annotate the latest observation so Plan can retrieve it
        if obs_list:
            obs_list[-1]['_affected_robot'] = affected_robot
            obs_list[-1]['_collision_dist'] = min_dist
            obs_list_obj = self.read_knowledge(ObservationList)
            obs_list_obj.observations = obs_list
            self.write_knowledge(obs_list_obj)

        _in_planning = InPlanning()
        _in_planning.in_planning = True
        self.write_knowledge(_in_planning)

        _abnormal_start_idx = AbnormalStartIdx()
        _abnormal_start_idx.abnormal_start_idx = len(obs_list) - detect_window_size
        self.write_knowledge(_abnormal_start_idx)

        self.logger.info(
            f"Anomaly: human approaching {affected_robot} (dist={min_dist:.2f} m)"
        )
        self.publish_event(event_key='/anomaly')

    def register_callbacks(self):
        self.register_event_callback(event_key='/new_data', callback=self.analysis)


class Plan(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Plan"
        self.logger.info("Plan instantiated")

    @timeit_callback
    def planner(self, msg):
        def _plan_thread():
            abnormal_start_idx = self.read_knowledge(AbnormalStartIdx).abnormal_start_idx
            replanning_counter = self.read_knowledge(ReplanningCounter).replanning_counter
            data_needed = max(5, 8 * (2 ** replanning_counter))

            while True:
                obs_list = self.read_knowledge(ObservationList).observations
                if len(obs_list) >= abnormal_start_idx + data_needed:
                    training_data = obs_list[abnormal_start_idx:abnormal_start_idx + data_needed]

                    # Find which robot is affected
                    affected_robot = None
                    for obs in reversed(training_data):
                        if '_affected_robot' in obs:
                            affected_robot = obs['_affected_robot']
                            break
                    if affected_robot is None:
                        affected_robot = 'robot_1'

                    # Average human position over the training window
                    xs, ys = [], []
                    for obs in training_data:
                        hx, hy = _parse_pos(obs.get('human_position', '0.0,0.0'))
                        xs.append(hx)
                        ys.append(hy)
                    avg_hx = sum(xs) / len(xs)
                    avg_hy = sum(ys) / len(ys)

                    # Push the robot waypoint away from the human
                    wx, wy = _ROBOT_WAYPOINTS[affected_robot]
                    dx = wx - avg_hx
                    dy = wy - avg_hy
                    mag = math.sqrt(dx ** 2 + dy ** 2) or 1.0
                    new_wx = max(0.5, min(9.5, wx + (dx / mag) * _AVOIDANCE_OFFSET))
                    new_wy = max(0.5, min(9.5, wy + (dy / mag) * _AVOIDANCE_OFFSET))

                    # Store plan annotation for Legitimate to validate
                    obs_list[-1]['_new_plan'] = {
                        'robot_id': affected_robot,
                        'x': round(new_wx, 2),
                        'y': round(new_wy, 2),
                    }
                    obs_list_obj = self.read_knowledge(ObservationList)
                    obs_list_obj.observations = obs_list
                    self.write_knowledge(obs_list_obj)

                    self.logger.info(
                        f"Plan: reroute {affected_robot} to ({new_wx:.2f}, {new_wy:.2f})"
                    )
                    break

                time.sleep(1)

            self.publish_event(event_key='/new_plan')

        threading.Thread(target=_plan_thread, daemon=True).start()

    def register_callbacks(self):
        self.register_event_callback(event_key='/anomaly', callback=self.planner)


class Legitimate(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Legitimate"
        self.logger.info("Legitimate instantiated")

    @timeit_callback
    def legitimizer(self, msg):
        validate_window_size = 5
        valid_threshold = 4.0

        obs_list = self.read_knowledge(ObservationList).observations
        baseline = self.read_knowledge(BaselineScore).score
        replanning_counter = self.read_knowledge(ReplanningCounter).replanning_counter

        # Retrieve the proposed plan
        new_plan = None
        for obs in reversed(obs_list):
            if '_new_plan' in obs:
                new_plan = obs['_new_plan']
                break

        if new_plan is None:
            self.logger.warning("Legitimate: no plan found, approving by default")
            self._approve()
            return

        new_wx = new_plan['x']
        new_wy = new_plan['y']
        affected_robot = new_plan['robot_id']

        # Check the new waypoint doesn't land inside any other robot's path corridor
        for robot_id, wp in _ROBOT_WAYPOINTS.items():
            if robot_id == affected_robot:
                continue
            start = _ROBOT_STARTS.get(robot_id, [0.0, 0.0])
            d = _point_to_segment_dist(new_wx, new_wy, start[0], start[1], wp[0], wp[1])
            if d < _COLLISION_RADIUS:
                self.logger.warning(
                    f"Legitimate: new waypoint for {affected_robot} conflicts with "
                    f"{robot_id} (dist={d:.2f} m) — re-planning"
                )
                _rc = ReplanningCounter()
                _rc.replanning_counter = replanning_counter + 1
                self.write_knowledge(_rc)
                self.publish_event(event_key='/anomaly')
                return

        # Validate: check recent human velocities haven't spiked above valid_threshold
        recent = obs_list[-validate_window_size:]
        scores = [abs(obs.get('human_velocity', 0.0)) for obs in recent]
        safe_baseline = baseline if baseline else 0.1
        ratios = [s / safe_baseline for s in scores]

        if any(r > valid_threshold for r in ratios):
            _rc = ReplanningCounter()
            _rc.replanning_counter = replanning_counter + 1
            self.write_knowledge(_rc)
            self.publish_event(event_key='/anomaly')
        else:
            # Commit the new waypoint to the live registry
            _ROBOT_WAYPOINTS[affected_robot][0] = new_wx
            _ROBOT_WAYPOINTS[affected_robot][1] = new_wy
            self.logger.info(
                f"Legitimate: approved — {affected_robot} → ({new_wx:.2f}, {new_wy:.2f})"
            )
            self._approve()

    def _approve(self):
        _rc = ReplanningCounter()
        _rc.replanning_counter = 0
        self.write_knowledge(_rc)

        _asi = AbnormalStartIdx()
        _asi.abnormal_start_idx = None
        self.write_knowledge(_asi)

        self.publish_event(event_key='/isLegit')

    def register_callbacks(self):
        self.register_event_callback(event_key='/new_plan', callback=self.legitimizer)


class Execute(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Execute"
        self.logger.info("Execute instantiated")

    @timeit_callback
    def executer(self, msg):
        obs_list = self.read_knowledge(ObservationList).observations

        new_plan = None
        for obs in reversed(obs_list):
            if '_new_plan' in obs:
                new_plan = obs['_new_plan']
                break

        action = NewWaypoint()
        action.new_waypoint = new_plan if new_plan else {}

        self.publish_event(event_key='/NewWaypoint', message=action.__dict__)
        self.logger.info(f"Execute: dispatched {action.new_waypoint}")

        _in_planning = InPlanning()
        _in_planning.in_planning = False
        self.write_knowledge(_in_planning)

    def register_callbacks(self):
        self.register_event_callback(event_key='/isLegit', callback=self.executer)


def main():
    try:
        with open('config.yaml') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise Exception("config.yaml not found")

    monitor_node = Monitor(config['Monitor_Config'])
    monitor_node.register_callbacks()
    monitor_node.start()

    analysis_node = Analysis(config['Analysis_Config'])
    analysis_node.register_callbacks()
    analysis_node.start()

    plan_node = Plan(config['Plan_Config'])
    plan_node.register_callbacks()
    plan_node.start()

    legitimate_node = Legitimate(config['Legitimate_Config'])
    legitimate_node.register_callbacks()
    legitimate_node.start()

    execute_node = Execute(config['Execute_Config'])
    execute_node.register_callbacks()
    execute_node.start()

    threading.Thread(
        target=run_dashboard,
        kwargs={'host': '127.0.0.1', 'port': 8050, 'debug': False, 'start_trust': True},
        daemon=True,
    ).start()


if __name__ == '__main__':
    main()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
