use rpclrs::Node;
use serde_json::json;
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use rand::Rng;

mod data_models;
use data_models::{SensorReading, Alert};

struct SensorNode {
    node: Node,
    sensor_id: String,
    location: String,
    sensing: Arc<AtomicBool>,
    reading_count: Arc<std::sync::Mutex<i32>>,
    base_temperature: Arc<std::sync::Mutex<f64>>,
    temperature_drift: Arc<std::sync::Mutex<f64>>,
}

impl SensorNode {
    fn new(sensor_id: String, location: String) -> anyhow::Result<Self> {
        let node = Node::new("SensorNode", "localhost", 6379)?;
        
        Ok(SensorNode {
            node,
            sensor_id,
            location,
            sensing: Arc::new(AtomicBool::new(false)),
            reading_count: Arc::new(std::sync::Mutex::new(0)),
            base_temperature: Arc::new(std::sync::Mutex::new(22.0)),
            temperature_drift: Arc::new(std::sync::Mutex::new(0.0)),
        })
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.node.start()?;
        log::info!("Sensor Node initialized: {} at {}", self.sensor_id, self.location);
        Ok(())
    }

    fn start_sensing(&mut self) -> anyhow::Result<()> {
        if self.sensing.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.sensing.store(true, Ordering::Relaxed);
        log::info!("Starting sensor readings...");

        let sensing = Arc::clone(&self.sensing);
        let reading_count = Arc::clone(&self.reading_count);
        let base_temperature = Arc::clone(&self.base_temperature);
        let temperature_drift = Arc::clone(&self.temperature_drift);
        let sensor_id = self.sensor_id.clone();
        let location = self.location.clone();
        
        // Move node ownership to thread
        let mut node = Node::new("SensorNode", "localhost", 6379)?;
        node.start()?;

        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            
            while sensing.load(Ordering::Relaxed) {
                match Self::sensor_loop_iteration(&mut node, &sensor_id, &location, &reading_count, 
                                                &base_temperature, &temperature_drift, &mut rng) {
                    Ok(_) => {},
                    Err(e) => {
                        log::error!("Error in sensor loop: {}", e);
                        thread::sleep(Duration::from_secs(1));
                    }
                }
                
                thread::sleep(Duration::from_secs(2));
            }
        });

        Ok(())
    }

    fn sensor_loop_iteration(
        node: &mut Node,
        sensor_id: &str,
        location: &str,
        reading_count: &Arc<std::sync::Mutex<i32>>,
        base_temperature: &Arc<std::sync::Mutex<f64>>,
        temperature_drift: &Arc<std::sync::Mutex<f64>>,
        rng: &mut rand::rngs::ThreadRng
    ) -> anyhow::Result<()> {
        // Generate temperature
        let temperature = Self::generate_temperature(base_temperature, temperature_drift, rng);
        
        // Create sensor reading
        let mut reading = SensorReading::new(
            sensor_id.to_string(),
            temperature,
            location.to_string(),
            Self::determine_status(temperature),
        );
        
        reading.uid = Some(node.generate_uuid());
        reading.timestamp = Some(node.get_timestamp());
        
        {
            let mut count = reading_count.lock().unwrap();
            *count += 1;
        }
        
        // Store in knowledge base
        node.write_knowledge_object(SensorReading::KNOWLEDGE_KEY, &reading)?;
        
        // Publish as event
        let reading_json = serde_json::to_value(&reading)?;
        node.publish_event(SensorReading::TOPIC, reading_json)?;
        
        // Send direct message
        let temp_topic = format!("sensor/{}/temperature", sensor_id);
        node.publish_message(&temp_topic, &temperature.to_string())?;
        
        log::info!("Published reading: {} = {}°C [{}]", sensor_id, temperature, reading.status);
        
        // Send alert if needed
        if reading.status != "normal" {
            Self::send_alert(node, &reading)?;
        }
        
        Ok(())
    }

    fn generate_temperature(
        base_temperature: &Arc<std::sync::Mutex<f64>>,
        temperature_drift: &Arc<std::sync::Mutex<f64>>,
        rng: &mut rand::rngs::ThreadRng
    ) -> f64 {
        let noise = rng.gen_range(-1.5..=1.5);
        
        // Update drift
        {
            let mut drift = temperature_drift.lock().unwrap();
            *drift += rng.gen_range(-0.1..=0.1);
            *drift = drift.max(-5.0).min(5.0);
        }
        
        let drift = *temperature_drift.lock().unwrap();
        let base = *base_temperature.lock().unwrap();
        
        // Occasional spike
        let spike = if rng.gen::<f64>() < 0.05 {
            rng.gen_range(-10.0..=15.0)
        } else {
            0.0
        };
        
        let temperature = base + drift + noise + spike;
        (temperature * 10.0).round() / 10.0
    }

    fn determine_status(temperature: f64) -> String {
        if temperature < 15.0 {
            "too_cold".to_string()
        } else if temperature > 30.0 {
            "too_hot".to_string()
        } else if temperature < 18.0 || temperature > 26.0 {
            "warning".to_string()
        } else {
            "normal".to_string()
        }
    }

    fn send_alert(node: &mut Node, reading: &SensorReading) -> anyhow::Result<()> {
        let (alert_type, message, severity) = match reading.status.as_str() {
            "too_cold" => (
                "low_temperature".to_string(),
                format!("Temperature too low: {}°C at {}", reading.temperature, reading.location),
                "high".to_string(),
            ),
            "too_hot" => (
                "high_temperature".to_string(),
                format!("Temperature too high: {}°C at {}", reading.temperature, reading.location),
                "high".to_string(),
            ),
            _ => (
                "temperature_warning".to_string(),
                format!("Temperature outside normal range: {}°C at {}", reading.temperature, reading.location),
                "medium".to_string(),
            ),
        };

        let mut alert = Alert::new(alert_type, message, severity, reading.sensor_id.clone());
        alert.uid = Some(node.generate_uuid());
        alert.timestamp = Some(node.get_timestamp());

        // Store alert in knowledge base
        node.write_knowledge_object(Alert::KNOWLEDGE_KEY, &alert)?;

        // Publish alert event
        let alert_json = serde_json::to_value(&alert)?;
        node.publish_event(Alert::TOPIC, alert_json)?;

        log::warn!("Alert sent: {} [Severity: {}]", alert.message, alert.severity);
        Ok(())
    }

    fn stop_sensing(&self) {
        self.sensing.store(false, Ordering::Relaxed);
        log::info!("Stopping sensor readings...");
    }

    fn get_stats(&self) -> serde_json::Value {
        let count = *self.reading_count.lock().unwrap();
        let sensing = self.sensing.load(Ordering::Relaxed);
        
        json!({
            "sensor_id": self.sensor_id,
            "location": self.location,
            "readings_count": count,
            "status": if sensing { "active" } else { "inactive" }
        })
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    log::info!("Starting sensor node...");

    let mut sensor = SensorNode::new("TEMP_001".to_string(), "Lab Room A".to_string())?;
    
    // Start the node
    sensor.start()?;
    
    // Start sensing
    sensor.start_sensing()?;
    
    println!("Sensor node started. Press Ctrl+C to stop...");
    println!("Sensor ID: TEMP_001");
    println!("Location: Lab Room A");
    println!("{}", "-".repeat(50));
    
    // Keep running and show stats
    loop {
        thread::sleep(Duration::from_secs(5));
        let stats = sensor.get_stats();
        println!("Stats: {} readings sent", stats["readings_count"]);
    }
}
