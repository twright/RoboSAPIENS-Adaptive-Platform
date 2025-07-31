use rpclrs::Node;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque, HashSet};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};

mod data_models;
use data_models::{SensorReading, Alert, SystemStatus};

#[derive(Default)]
struct Statistics {
    total_readings: i32,
    active_sensors: HashSet<String>,
    alert_count: i32,
    temperature_sum: f64,
}

struct MonitorNode {
    node: Node,
    monitoring: Arc<AtomicBool>,
    sensor_data: Arc<Mutex<HashMap<String, VecDeque<Value>>>>,
    alerts: Arc<Mutex<VecDeque<Alert>>>,
    statistics: Arc<Mutex<Statistics>>,
}

impl MonitorNode {
    fn new() -> anyhow::Result<Self> {
        let node = Node::new("MonitorNode", "localhost", 6379)?;
        
        Ok(MonitorNode {
            node,
            monitoring: Arc::new(AtomicBool::new(false)),
            sensor_data: Arc::new(Mutex::new(HashMap::new())),
            alerts: Arc::new(Mutex::new(VecDeque::new())),
            statistics: Arc::new(Mutex::new(Statistics::default())),
        })
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.node.start()?;
        log::info!("Monitor Node initialized");
        Ok(())
    }

    fn start_monitoring(&mut self) -> anyhow::Result<()> {
        if self.monitoring.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.monitoring.store(true, Ordering::Relaxed);

        // Create new node instance for callback thread
        let mut callback_node = Node::new("MonitorNodeCallbacks", "localhost", 6379)?;
        callback_node.start()?;

        // Clone Arc references for callbacks
        let sensor_data = Arc::clone(&self.sensor_data);
        let alerts = Arc::clone(&self.alerts);
        let statistics = Arc::clone(&self.statistics);

        // Register sensor reading callback
        let sensor_data_clone = Arc::clone(&sensor_data);
        let statistics_clone = Arc::clone(&statistics);
        callback_node.register_event_callback(SensorReading::TOPIC, move |topic, message| {
            Self::handle_sensor_reading(topic, message, &sensor_data_clone, &statistics_clone);
        })?;

        // Register alert callback
        let alerts_clone = Arc::clone(&alerts);
        let statistics_clone = Arc::clone(&statistics);
        callback_node.register_event_callback(Alert::TOPIC, move |topic, message| {
            Self::handle_alert(topic, message, &alerts_clone, &statistics_clone);
        })?;

        // Register direct message callback
        callback_node.register_message_callback("sensor/+/temperature", move |topic, message| {
            Self::handle_direct_message(topic, message);
        })?;

        // Start status reporting thread
        let monitoring = Arc::clone(&self.monitoring);
        let statistics_for_status = Arc::clone(&self.statistics);
        
        // Create another node instance for status thread
        let mut status_node = Node::new("MonitorNodeStatus", "localhost", 6379)?;
        status_node.start()?;
        
        thread::spawn(move || {
            Self::status_loop(status_node, monitoring, statistics_for_status);
        });

        log::info!("Started monitoring sensor data...");
        Ok(())
    }

    fn handle_sensor_reading(
        _topic: &str, 
        message: &str, 
        sensor_data: &Arc<Mutex<HashMap<String, VecDeque<Value>>>>,
        statistics: &Arc<Mutex<Statistics>>
    ) {
        match serde_json::from_str::<Value>(message) {
            Ok(data) => {
                let sensor_id = data.get("sensor_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                
                let temperature = data.get("temperature")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                
                let status = data.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                // Store reading
                {
                    let mut sensor_data_map = sensor_data.lock().unwrap();
                    let readings = sensor_data_map.entry(sensor_id.clone()).or_insert_with(VecDeque::new);
                    
                    let reading_info = json!({
                        "temperature": temperature,
                        "location": data.get("location"),
                        "status": status,
                        "timestamp": data.get("timestamp")
                    });
                    
                    readings.push_back(reading_info);
                    
                    // Keep only recent readings
                    const MAX_READINGS_PER_SENSOR: usize = 50;
                    if readings.len() > MAX_READINGS_PER_SENSOR {
                        readings.pop_front();
                    }
                }

                // Update statistics
                {
                    let mut stats = statistics.lock().unwrap();
                    stats.total_readings += 1;
                    stats.active_sensors.insert(sensor_id.clone());
                    stats.temperature_sum += temperature;
                }

                log::info!("Processed reading from {}: {}°C [{}]", sensor_id, temperature, status);

                // Analyze sensor data
                Self::analyze_sensor_data(&sensor_id, sensor_data);
            }
            Err(e) => {
                log::error!("Error processing sensor reading: {}", e);
            }
        }
    }

    fn handle_alert(
        _topic: &str, 
        message: &str, 
        alerts: &Arc<Mutex<VecDeque<Alert>>>,
        statistics: &Arc<Mutex<Statistics>>
    ) {
        match serde_json::from_str::<Alert>(message) {
            Ok(alert) => {
                {
                    let mut alerts_deque = alerts.lock().unwrap();
                    alerts_deque.push_back(alert.clone());
                    
                    // Keep only recent alerts
                    const MAX_ALERTS: usize = 100;
                    if alerts_deque.len() > MAX_ALERTS {
                        alerts_deque.pop_front();
                    }
                }

                {
                    let mut stats = statistics.lock().unwrap();
                    stats.alert_count += 1;
                }

                log::warn!("Alert received: {} [Severity: {}]", alert.message, alert.severity);
                Self::handle_alert_response(&alert);
            }
            Err(e) => {
                log::error!("Error processing alert: {}", e);
            }
        }
    }

    fn handle_direct_message(topic: &str, message: &str) {
        if let Some(sensor_id) = Self::extract_sensor_id_from_topic(topic) {
            if let Ok(temperature) = message.parse::<f64>() {
                log::debug!("Direct message from {}: {}°C", sensor_id, temperature);
            }
        }
    }

    fn extract_sensor_id_from_topic(topic: &str) -> Option<String> {
        let parts: Vec<&str> = topic.split('/').collect();
        if parts.len() >= 2 {
            Some(parts[1].to_string())
        } else {
            None
        }
    }

    fn analyze_sensor_data(sensor_id: &str, sensor_data: &Arc<Mutex<HashMap<String, VecDeque<Value>>>>) {
        let sensor_data_map = sensor_data.lock().unwrap();
        if let Some(readings) = sensor_data_map.get(sensor_id) {
            if readings.len() < 2 {
                return;
            }

            // Get recent temperatures
            let recent_temps: Vec<f64> = readings.iter()
                .rev()
                .take(5)
                .filter_map(|reading| reading.get("temperature")?.as_f64())
                .collect();

            if recent_temps.len() >= 2 {
                // Detect rapid temperature changes
                let temp_change = (recent_temps[0] - recent_temps[1]).abs();
                if temp_change > 5.0 {
                    log::warn!("Rapid temperature change detected for {}: {}°C", sensor_id, temp_change);
                }

                // Detect trends
                if recent_temps.len() >= 3 {
                    let increasing = recent_temps.windows(2).all(|w| w[0] > w[1]);
                    let decreasing = recent_temps.windows(2).all(|w| w[0] < w[1]);

                    if increasing || decreasing {
                        let trend = if increasing { "increasing" } else { "decreasing" };
                        log::info!("Temperature trend for {}: {}", sensor_id, trend);
                    }
                }
            }
        }
    }

    fn handle_alert_response(alert: &Alert) {
        match alert.severity.as_str() {
            "high" => log::error!("HIGH SEVERITY ALERT: {}", alert.message),
            "medium" => log::warn!("Medium severity alert: {}", alert.message),
            _ => log::info!("Alert: {}", alert.message),
        }
    }

    fn status_loop(
        mut node: Node, 
        monitoring: Arc<AtomicBool>, 
        statistics: Arc<Mutex<Statistics>>
    ) {
        while monitoring.load(Ordering::Relaxed) {
            match Self::status_loop_iteration(&mut node, &statistics) {
                Ok(_) => {},
                Err(e) => {
                    log::error!("Error in status loop: {}", e);
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            }
            
            thread::sleep(Duration::from_secs(10));
        }
    }

    fn status_loop_iteration(node: &mut Node, statistics: &Arc<Mutex<Statistics>>) -> anyhow::Result<()> {
        let (active_sensors, total_readings, alerts_count, avg_temp) = {
            let stats = statistics.lock().unwrap();
            let avg_temp = if stats.total_readings > 0 {
                stats.temperature_sum / stats.total_readings as f64
            } else {
                0.0
            };
            
            (
                stats.active_sensors.len() as i32,
                stats.total_readings,
                stats.alert_count,
                avg_temp
            )
        };

        let mut status = SystemStatus::new(active_sensors, total_readings, alerts_count, avg_temp);
        status.uid = Some(node.generate_uuid());
        status.timestamp = Some(node.get_timestamp());

        // Store system status in knowledge base
        node.write_knowledge_object(SystemStatus::KNOWLEDGE_KEY, &status)?;

        // Publish system status as event
        let status_json = serde_json::to_value(&status)?;
        node.publish_event(SystemStatus::TOPIC, status_json)?;

        log::info!("System Status - Sensors: {}, Readings: {}, Alerts: {}, Avg Temp: {:.1}°C", 
                  status.active_sensors, status.total_readings, status.alerts_count, status.avg_temperature);

        Ok(())
    }

    fn stop_monitoring(&self) {
        self.monitoring.store(false, Ordering::Relaxed);
        log::info!("Stopped monitoring.");
    }

    fn get_sensor_summary(&self) -> Value {
        let sensor_data_map = self.sensor_data.lock().unwrap();
        let mut summary = json!({});
        
        for (sensor_id, readings) in sensor_data_map.iter() {
            if let Some(latest) = readings.back() {
                summary[sensor_id] = json!({
                    "latest_temperature": latest.get("temperature"),
                    "status": latest.get("status"),
                    "location": latest.get("location"),
                    "reading_count": readings.len(),
                    "last_seen": latest.get("timestamp")
                });
            }
        }
        
        summary
    }

    fn get_recent_alerts(&self, count: usize) -> Vec<Alert> {
        let alerts_deque = self.alerts.lock().unwrap();
        alerts_deque.iter()
            .rev()
            .take(count)
            .cloned()
            .collect()
    }

    fn get_statistics(&self) -> Value {
        let stats = self.statistics.lock().unwrap();
        let avg_temp = if stats.total_readings > 0 {
            stats.temperature_sum / stats.total_readings as f64
        } else {
            0.0
        };

        json!({
            "active_sensors": stats.active_sensors.len(),
            "total_readings": stats.total_readings,
            "alert_count": stats.alert_count,
            "average_temperature": avg_temp
        })
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    log::info!("Starting monitor node...");

    let mut monitor = MonitorNode::new()?;
    
    // Start the node
    monitor.start()?;
    
    // Start monitoring
    monitor.start_monitoring()?;
    
    println!("Monitor node started. Press Ctrl+C to stop...");
    println!("Waiting for sensor data...");
    println!("{}", "-".repeat(50));
    
    // Periodically display status
    loop {
        thread::sleep(Duration::from_secs(15));
        
        println!("\n{}", "=".repeat(60));
        println!("SENSOR MONITORING STATUS");
        println!("{}", "=".repeat(60));
        
        // Show statistics
        let stats = monitor.get_statistics();
        println!("Active Sensors: {}", stats["active_sensors"]);
        println!("Total Readings: {}", stats["total_readings"]);
        println!("Alert Count: {}", stats["alert_count"]);
        println!("Average Temperature: {:.1}°C", stats["average_temperature"]);
        
        // Show sensor summary
        let sensors = monitor.get_sensor_summary();
        if !sensors.as_object().unwrap().is_empty() {
            println!("\nSensor Details:");
            for (sensor_id, info) in sensors.as_object().unwrap() {
                println!("  {}: {}°C [{}] at {}", 
                        sensor_id, 
                        info.get("latest_temperature").unwrap_or(&json!(0)),
                        info.get("status").unwrap_or(&json!("unknown")),
                        info.get("location").unwrap_or(&json!("unknown")));
            }
        }
        
        // Show recent alerts
        let alerts = monitor.get_recent_alerts(3);
        if !alerts.is_empty() {
            println!("\nRecent Alerts ({}):", alerts.len());
            for alert in alerts.iter().take(3) {
                println!("  [{}] {}", alert.severity.to_uppercase(), alert.message);
            }
        }
        
        println!("{}", "-".repeat(60));
    }
}
