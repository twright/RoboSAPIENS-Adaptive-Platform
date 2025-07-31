"""
Data models for the sensor monitoring example.
These classes define the structure of data exchanged between nodes.
"""

class SensorReading:
    """Represents a sensor reading with temperature and metadata."""
    
    def __init__(self, sensor_id="", temperature=0.0, location="", status="normal"):
        self.name = "sensor_reading"  # Knowledge key
        self.topic = "sensor/temperature"  # Event topic
        self.sensor_id = sensor_id
        self.temperature = temperature
        self.location = location
        self.status = status
        self.uid = None  # Will be set by the Node
        self.timestamp = None  # Will be set by the Node

    def __str__(self):
        return f"SensorReading(id={self.sensor_id}, temp={self.temperature}°C, location={self.location}, status={self.status})"


class Alert:
    """Represents an alert when sensor readings are abnormal."""
    
    def __init__(self, alert_type="", message="", severity="low", sensor_id=""):
        self.name = "alert"  # Knowledge key
        self.topic = "alerts/temperature"  # Event topic
        self.alert_type = alert_type
        self.message = message
        self.severity = severity
        self.sensor_id = sensor_id
        self.uid = None  # Will be set by the Node
        self.timestamp = None  # Will be set by the Node

    def __str__(self):
        return f"Alert({self.alert_type}): {self.message} [Severity: {self.severity}]"


class SystemStatus:
    """Represents the overall system status."""
    
    def __init__(self, active_sensors=0, total_readings=0, alerts_count=0, avg_temperature=0.0):
        self.name = "system_status"  # Knowledge key
        self.topic = "system/status"  # Event topic
        self.active_sensors = active_sensors
        self.total_readings = total_readings
        self.alerts_count = alerts_count
        self.avg_temperature = avg_temperature
        self.uid = None  # Will be set by the Node
        self.timestamp = None  # Will be set by the Node

    def __str__(self):
        return f"SystemStatus(sensors={self.active_sensors}, readings={self.total_readings}, alerts={self.alerts_count}, avg_temp={self.avg_temperature:.1f}°C)"
