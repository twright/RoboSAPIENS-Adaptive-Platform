"""
Sensor Node Example

This node simulates a temperature sensor that:
1. Generates random temperature readings
2. Publishes readings as events
3. Stores readings in knowledge base
4. Sends alerts when temperature is abnormal
"""

import sys
import os
import time
import random
import yaml
from threading import Thread

# Add the parent directory to the path to import rpclpy
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from rpclpy.node import Node
from data_models import SensorReading, Alert


class SensorNode(Node):
    """A sensor node that generates and publishes temperature readings."""
    
    def __init__(self, config, sensor_id="TEMP_001", location="Lab Room A"):
        super().__init__(config)
        self.sensor_id = sensor_id
        self.location = location
        self.running = False
        self.reading_count = 0
        
        # Temperature simulation parameters
        self.base_temperature = 22.0  # Base temperature in Celsius
        self.temperature_drift = 0.0  # Gradual drift
        
        self.logger.info(f"Sensor Node initialized: {sensor_id} at {location}")

    def start_sensing(self):
        """Start the sensor reading loop."""
        self.running = True
        self.logger.info("Starting sensor readings...")
        
        # Start sensor thread
        sensor_thread = Thread(target=self._sensor_loop, daemon=True)
        sensor_thread.start()

    def stop_sensing(self):
        """Stop the sensor reading loop."""
        self.running = False
        self.logger.info("Stopping sensor readings...")

    def _sensor_loop(self):
        """Main sensor loop that generates and publishes readings."""
        while self.running:
            try:
                # Generate a realistic temperature reading
                temperature = self._generate_temperature()
                
                # Create sensor reading object
                reading = SensorReading(
                    sensor_id=self.sensor_id,
                    temperature=temperature,
                    location=self.location,
                    status=self._determine_status(temperature)
                )
                
                self.reading_count += 1
                
                # Store in knowledge base
                self.write_knowledge_as_object(reading)
                
                # Publish as event
                self.publish_event(SensorReading, reading.__dict__)
                
                # Send message with just the temperature value
                self.publish_message(f"sensor/{self.sensor_id}/temperature", str(temperature))
                
                self.logger.info(f"Published reading: {reading}")
                
                # Check if alert is needed
                if reading.status != "normal":
                    self._send_alert(reading)
                
                # Wait before next reading
                time.sleep(2)
                
            except Exception as e:
                self.logger.error(f"Error in sensor loop: {e}")
                time.sleep(1)

    def _generate_temperature(self):
        """Generate a realistic temperature reading with some randomness."""
        # Add some random noise
        noise = random.uniform(-1.5, 1.5)
        
        # Add gradual drift over time
        self.temperature_drift += random.uniform(-0.1, 0.1)
        self.temperature_drift = max(-5.0, min(5.0, self.temperature_drift))  # Limit drift
        
        # Occasionally simulate a spike or drop
        if random.random() < 0.05:  # 5% chance
            spike = random.uniform(-10, 15)
            temperature = self.base_temperature + self.temperature_drift + noise + spike
        else:
            temperature = self.base_temperature + self.temperature_drift + noise
        
        return round(temperature, 1)

    def _determine_status(self, temperature):
        """Determine the status based on temperature."""
        if temperature < 15:
            return "too_cold"
        elif temperature > 30:
            return "too_hot"
        elif temperature < 18 or temperature > 26:
            return "warning"
        else:
            return "normal"

    def _send_alert(self, reading):
        """Send an alert for abnormal readings."""
        if reading.status == "too_cold":
            alert = Alert(
                alert_type="low_temperature",
                message=f"Temperature too low: {reading.temperature}°C at {reading.location}",
                severity="high",
                sensor_id=reading.sensor_id
            )
        elif reading.status == "too_hot":
            alert = Alert(
                alert_type="high_temperature", 
                message=f"Temperature too high: {reading.temperature}°C at {reading.location}",
                severity="high",
                sensor_id=reading.sensor_id
            )
        else:  # warning
            alert = Alert(
                alert_type="temperature_warning",
                message=f"Temperature outside normal range: {reading.temperature}°C at {reading.location}",
                severity="medium",
                sensor_id=reading.sensor_id
            )
        
        # Store alert in knowledge base
        self.write_knowledge_as_object(alert)
        
        # Publish alert as event
        self.publish_event(Alert, alert.__dict__)
        
        self.logger.warning(f"Alert sent: {alert}")

    def get_stats(self):
        """Get sensor statistics."""
        return {
            "sensor_id": self.sensor_id,
            "location": self.location,
            "readings_count": self.reading_count,
            "status": "active" if self.running else "inactive"
        }


def main():
    """Main function to run the sensor node."""
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml') 
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Create and start sensor node
    sensor = SensorNode(config, sensor_id="TEMP_001", location="Lab Room A")
    
    try:
        # Start the node communication
        sensor.start()
        
        # Start sensing
        sensor.start_sensing()
        
        print("Sensor node started. Press Ctrl+C to stop...")
        print(f"Sensor ID: {sensor.sensor_id}")
        print(f"Location: {sensor.location}")
        print("-" * 50)
        
        # Keep the main thread alive
        while True:
            time.sleep(5)
            stats = sensor.get_stats()
            print(f"Stats: {stats['readings_count']} readings sent")
            
    except KeyboardInterrupt:
        print("\nShutting down sensor node...")
        sensor.stop_sensing()
        sensor.shutdown()
        print("Sensor node stopped.")


if __name__ == "__main__":
    main()
