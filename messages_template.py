# ============================================================
# MAPE-K Message Definitions Template
# ============================================================
# Two categories:
#   1. Knowledge Classes  – shared state stored in the Knowledge Manager
#   2. Event / IO Messages – data payloads exchanged between nodes
#
# Rename fields and classes to match your use case.
# ============================================================


# ------------------------------------------------------------
# Knowledge Classes (shared state in Knowledge Manager)
# ------------------------------------------------------------

class BaselineScore(object):
    """Baseline performance score captured when the system is first healthy."""
    def __init__(self):
        self.name = "BaselineScore"
        self.uid = None
        self.timestamp = None
        self.score = None           # TODO: set to your performance metric type


class AnomalyRatioList(object):
    """Running list of (current_score / baseline_score) ratios."""
    def __init__(self):
        self.name = "AnomalyRatioList"
        self.uid = None
        self.timestamp = None
        self.ratio_list = []


class ObservationList(object):
    """Historical list of raw sensor observations for analysis and re-planning."""
    def __init__(self):
        self.name = "ObservationList"
        self.uid = None
        self.timestamp = None
        self.observations = []      # TODO: each element is your use-case's raw observation


class InPlanning(object):
    """Flag indicating whether a re-planning cycle is currently active."""
    def __init__(self):
        self.name = "InPlanning"
        self.uid = None
        self.timestamp = None
        self.in_planning = False


class AbnormalStartIdx(object):
    """Index into ObservationList where the current anomaly was first confirmed."""
    def __init__(self):
        self.name = "AbnormalStartIdx"
        self.uid = None
        self.timestamp = None
        self.abnormal_start_idx = None


class ReplanningCounter(object):
    """Number of re-planning attempts made for the current anomaly episode."""
    def __init__(self):
        self.name = "ReplanningCounter"
        self.uid = None
        self.timestamp = None
        self.replanning_counter = 0


# ------------------------------------------------------------
# Event / IO Message Templates
# ------------------------------------------------------------

class SensorData(object):
    """
    Incoming data from the environment / sensor.
    TODO: Replace the placeholder fields with your use-case specific fields.
    """
    def __init__(self):
        self.name = "SensorData"
        self.uid = None
        self.timestamp = None
        # TODO: add your sensor fields, e.g.:
        # self.value = None
        # self.state_vector = []


class ActionCommand(object):
    """
    Output command sent by the Execute node to the actuator / system.
    TODO: Replace the placeholder fields with your use-case specific fields.
    """
    def __init__(self):
        self.name = "ActionCommand"
        self.uid = None
        self.timestamp = None
        # TODO: add your action fields, e.g.:
        # self.command = None
        # self.parameters = {}
