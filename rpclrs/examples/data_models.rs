use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SensorReading {
    pub sensor_id: String,
    pub temperature: f64,
    pub location: String,
    pub status: String,
    pub uid: Option<String>,
    pub timestamp: Option<String>,
}

impl SensorReading {
    pub const TOPIC: &'static str = "sensor/temperature";
    pub const KNOWLEDGE_KEY: &'static str = "sensor_reading";

    pub fn new(sensor_id: String, temperature: f64, location: String, status: String) -> Self {
        Self {
            sensor_id,
            temperature,
            location,
            status,
            uid: None,
            timestamp: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Alert {
    pub alert_type: String,
    pub message: String,
    pub severity: String,
    pub sensor_id: String,
    pub uid: Option<String>,
    pub timestamp: Option<String>,
}

impl Alert {
    pub const TOPIC: &'static str = "alerts/temperature";
    pub const KNOWLEDGE_KEY: &'static str = "alert";

    pub fn new(alert_type: String, message: String, severity: String, sensor_id: String) -> Self {
        Self {
            alert_type,
            message,
            severity,
            sensor_id,
            uid: None,
            timestamp: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SystemStatus {
    pub active_sensors: i32,
    pub total_readings: i32,
    pub alerts_count: i32,
    pub avg_temperature: f64,
    pub uid: Option<String>,
    pub timestamp: Option<String>,
}

impl SystemStatus {
    pub const TOPIC: &'static str = "system/status";
    pub const KNOWLEDGE_KEY: &'static str = "system_status";

    pub fn new(active_sensors: i32, total_readings: i32, alerts_count: i32, avg_temperature: f64) -> Self {
        Self {
            active_sensors,
            total_readings,
            alerts_count,
            avg_temperature,
            uid: None,
            timestamp: None,
        }
    }
}
