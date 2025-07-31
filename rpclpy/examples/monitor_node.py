"""
Monitor Node Example

This node monitors sensor data by:
1. Subscribing to sensor events
2. Processing and analyzing readings
3. Maintaining system statistics
4. Responding to alerts
"""

import sys
import os
import time
import yaml
import json
from collections import defaultdict, deque
from threading import Thread, Lock

# Add the parent directory to the path to import rpclpy
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from rpclpy.node import Node
from data_models import SensorReading, Alert, SystemStatus


class MonitorNode(Node):
    """A monitor node that processes sensor data and maintains system status."""
    
    def __init__(self, config):
        super().__init__(config)
        self.running = False
        
        # Data storage
        self.sensor_data = defaultdict(deque)  # sensor_id -> recent readings
        self.alerts = deque(maxlen=100)  # Recent alerts
        self.statistics = {
            "total_readings": 0,
            "active_sensors": set(),
            "alert_count": 0,
            "temperature_sum": 0.0
        }
        
        # Thread safety
        self.data_lock = Lock()
        
        # Configuration
        self.max_readings_per_sensor = 50
        
        self.logger.info("Monitor Node initialized")

    def start_monitoring(self):
        """Start monitoring sensor data."""
        self.running = True
        
        # Subscribe to sensor events
        self.register_event_callback("sensor/temperature", self._handle_sensor_reading)
        self.register_event_callback("alerts/temperature", self._handle_alert)
        
        # Subscribe to direct messages  
        self.register_message_callback("sensor/+/temperature", self._handle_direct_message)
        
        # Start status reporting thread
        status_thread = Thread(target=self._status_loop, daemon=True)
        status_thread.start()
        
        self.logger.info("Started monitoring sensor data...")

    def stop_monitoring(self):
        """Stop monitoring."""
        self.running = False
        self.logger.info("Stopped monitoring.")

    def _handle_sensor_reading(self, topic, message):
        """Handle incoming sensor reading events."""
        try:
            # Parse the message
            data = json.loads(message)
            
            sensor_id = data.get('sensor_id', 'unknown')
            temperature = data.get('temperature', 0.0)
            location = data.get('location', 'unknown')
            status = data.get('status', 'unknown')
            timestamp = data.get('timestamp', 'unknown')
            
            with self.data_lock:
                # Store the reading
                reading_info = {
                    'temperature': temperature,
                    'location': location, 
                    'status': status,
                    'timestamp': timestamp
                }
                
                self.sensor_data[sensor_id].append(reading_info)
                
                # Keep only recent readings
                if len(self.sensor_data[sensor_id]) > self.max_readings_per_sensor:
                    self.sensor_data[sensor_id].popleft()
                
                # Update statistics
                self.statistics["total_readings"] += 1
                self.statistics["active_sensors"].add(sensor_id)
                self.statistics["temperature_sum"] += temperature
            
            self.logger.info(f"Processed reading from {sensor_id}: {temperature}°C [{status}]")
            
            # Analyze the reading
            self._analyze_sensor_data(sensor_id, reading_info)
            
        except Exception as e:
            self.logger.error(f"Error processing sensor reading: {e}")

    def _handle_alert(self, topic, message):
        """Handle incoming alert events."""
        try:
            data = json.loads(message)
            
            alert_info = {
                'type': data.get('alert_type', 'unknown'),
                'message': data.get('message', ''),
                'severity': data.get('severity', 'low'),
                'sensor_id': data.get('sensor_id', 'unknown'),
                'timestamp': data.get('timestamp', 'unknown')
            }
            
            with self.data_lock:
                self.alerts.append(alert_info)
                self.statistics["alert_count"] += 1
            
            self.logger.warning(f"Alert received: {alert_info['message']} [Severity: {alert_info['severity']}]")
            
            # Take action based on severity
            self._handle_alert_response(alert_info)
            
        except Exception as e:
            self.logger.error(f"Error processing alert: {e}")

    def _handle_direct_message(self, topic, message):
        """Handle direct temperature messages."""
        try:
            # Extract sensor ID from topic (e.g., "sensor/TEMP_001/temperature")
            parts = topic.split('/')
            if len(parts) >= 2:
                sensor_id = parts[1]
                temperature = float(message)
                
                self.logger.debug(f"Direct message from {sensor_id}: {temperature}°C")
                
        except Exception as e:
            self.logger.error(f"Error processing direct message: {e}")

    def _analyze_sensor_data(self, sensor_id, reading):
        """Analyze sensor data for patterns or issues."""
        try:
            with self.data_lock:
                recent_readings = list(self.sensor_data[sensor_id])
            
            if len(recent_readings) < 3:
                return  # Need more data
            
            # Check for trends
            temps = [r['temperature'] for r in recent_readings[-5:]]  # Last 5 readings
            
            # Detect rapid temperature changes
            if len(temps) >= 2:
                temp_change = abs(temps[-1] - temps[-2])
                if temp_change > 5.0:
                    self.logger.warning(f"Rapid temperature change detected for {sensor_id}: {temp_change}°C")
            
            # Detect consistent trends
            if len(temps) >= 3:
                increasing = all(temps[i] < temps[i+1] for i in range(len(temps)-1))
                decreasing = all(temps[i] > temps[i+1] for i in range(len(temps)-1))
                
                if increasing or decreasing:
                    trend = "increasing" if increasing else "decreasing"
                    self.logger.info(f"Temperature trend for {sensor_id}: {trend}")
            
        except Exception as e:
            self.logger.error(f"Error analyzing sensor data: {e}")

    def _handle_alert_response(self, alert_info):
        """Take appropriate action based on alert."""
        severity = alert_info['severity']
        
        if severity == "high":
            # High severity - could trigger immediate action
            self.logger.critical(f"HIGH SEVERITY ALERT: {alert_info['message']}")
            # In a real system, this might trigger automated responses
            
        elif severity == "medium":
            self.logger.warning(f"Medium severity alert: {alert_info['message']}")
            
        # Store alert details in knowledge base for future reference
        alert_key = f"alert_{alert_info.get('timestamp', int(time.time()))}"
        self.write_knowledge(alert_key, json.dumps(alert_info))

    def _status_loop(self):
        """Periodically update and publish system status."""
        while self.running:
            try:
                with self.data_lock:
                    # Calculate average temperature
                    avg_temp = 0.0
                    if self.statistics["total_readings"] > 0:
                        avg_temp = self.statistics["temperature_sum"] / self.statistics["total_readings"]
                    
                    # Create system status
                    status = SystemStatus(
                        active_sensors=len(self.statistics["active_sensors"]),
                        total_readings=self.statistics["total_readings"],
                        alerts_count=self.statistics["alert_count"],
                        avg_temperature=avg_temp
                    )
                
                # Store system status in knowledge base
                self.write_knowledge_as_object(status)
                
                # Publish system status as event
                self.publish_event(SystemStatus, status.__dict__)
                
                self.logger.info(f"System Status: {status}")
                
                # Sleep for 10 seconds
                time.sleep(10)
                
            except Exception as e:
                self.logger.error(f"Error in status loop: {e}")
                time.sleep(5)

    def get_sensor_summary(self):
        """Get a summary of all sensors."""
        with self.data_lock:
            summary = {}
            for sensor_id, readings in self.sensor_data.items():
                if readings:
                    latest = readings[-1]
                    summary[sensor_id] = {
                        'latest_temperature': latest['temperature'],
                        'status': latest['status'],
                        'location': latest['location'],
                        'reading_count': len(readings),
                        'last_seen': latest['timestamp']
                    }
            return summary

    def get_recent_alerts(self, count=10):
        """Get recent alerts."""
        with self.data_lock:
            return list(self.alerts)[-count:]

    def get_statistics(self):
        """Get system statistics."""
        with self.data_lock:
            stats = dict(self.statistics)
            stats["active_sensors"] = len(stats["active_sensors"])
            if stats["total_readings"] > 0:
                stats["average_temperature"] = stats["temperature_sum"] / stats["total_readings"]
            else:
                stats["average_temperature"] = 0.0
            del stats["temperature_sum"]  # Remove internal field
            return stats


def main():
    """Main function to run the monitor node."""
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Create and start monitor node
    monitor = MonitorNode(config)
    
    try:
        # Start the node communication
        monitor.start()
        
        # Start monitoring
        monitor.start_monitoring()
        
        print("Monitor node started. Press Ctrl+C to stop...")
        print("Waiting for sensor data...")
        print("-" * 50)
        
        # Periodically display status
        while True:
            time.sleep(15)
            
            # Display summary
            print("\n" + "="*60)
            print("SENSOR MONITORING STATUS")
            print("="*60)
            
            # Show statistics
            stats = monitor.get_statistics()
            print(f"Active Sensors: {stats['active_sensors']}")
            print(f"Total Readings: {stats['total_readings']}")
            print(f"Alert Count: {stats['alert_count']}")
            print(f"Average Temperature: {stats['average_temperature']:.1f}°C")
            
            # Show sensor summary
            sensors = monitor.get_sensor_summary()
            if sensors:
                print("\nSensor Details:")
                for sensor_id, info in sensors.items():
                    print(f"  {sensor_id}: {info['latest_temperature']}°C [{info['status']}] at {info['location']}")
            
            # Show recent alerts
            alerts = monitor.get_recent_alerts(3)
            if alerts:
                print(f"\nRecent Alerts ({len(alerts)}):")
                for alert in alerts[-3:]:
                    print(f"  [{alert['severity'].upper()}] {alert['message']}")
            
            print("-" * 60)
            
    except KeyboardInterrupt:
        print("\nShutting down monitor node...")
        monitor.stop_monitoring()
        monitor.shutdown()
        print("Monitor node stopped.")


if __name__ == "__main__":
    main()
