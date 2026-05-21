# Generated for: MultiRobotNavigationAdaptation
# MAPE-K Knowledge and Message Classes


# ── Knowledge Classes (shared state in Knowledge Manager) ────────────────────

class BaselineScore(object):
    def __init__(self):
        self.name = "BaselineScore"
        self.uid = None
        self.timestamp = None
        self.score = None


class AnomalyRatioList(object):
    def __init__(self):
        self.name = "AnomalyRatioList"
        self.uid = None
        self.timestamp = None
        self.ratio_list = []


class ObservationList(object):
    def __init__(self):
        self.name = "ObservationList"
        self.uid = None
        self.timestamp = None
        self.observations = []


class InPlanning(object):
    def __init__(self):
        self.name = "InPlanning"
        self.uid = None
        self.timestamp = None
        self.in_planning = False


class AbnormalStartIdx(object):
    def __init__(self):
        self.name = "AbnormalStartIdx"
        self.uid = None
        self.timestamp = None
        self.abnormal_start_idx = None


class ReplanningCounter(object):
    def __init__(self):
        self.name = "ReplanningCounter"
        self.uid = None
        self.timestamp = None
        self.replanning_counter = 0


# ── Event / IO Messages ───────────────────────────────────────────────────────

class HumanStatus(object):
    def __init__(self):
        self.name = "HumanStatus"
        self.uid = None
        self.timestamp = None
        self.human_position = None  # Position as "x,y" string (meters)
        self.human_velocity = None  # Scalar speed in m/s


class NewWaypoint(object):
    def __init__(self):
        self.name = "NewWaypoint"
        self.uid = None
        self.timestamp = None
        self.new_waypoint = {}  # {"robot_id": str, "x": float, "y": float}
