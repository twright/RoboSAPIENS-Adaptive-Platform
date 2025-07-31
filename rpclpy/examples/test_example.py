"""
Test Script for RPC Library Example

This script demonstrates different aspects of the rpclpy library by running
both nodes and testing their interactions.
"""

import sys
import os
import time
import yaml
import threading
from threading import Thread

# Add the parent directory to the path to import rpclpy
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from rpclpy.node import Node
from sensor_node import SensorNode
from monitor_node import MonitorNode


class TestController:
    """Controller to run and test both nodes."""
    
    def __init__(self):
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.sensor_node = None
        self.monitor_node = None
        
    def setup_nodes(self):
        """Create and setup both nodes."""
        print("Setting up nodes...")
        
        # Create monitor node first (so it's ready to receive events)
        self.monitor_node = MonitorNode(self.config)
        self.monitor_node.start()
        self.monitor_node.start_monitoring()
        
        # Give monitor time to setup subscriptions
        time.sleep(2)
        
        # Create sensor node
        self.sensor_node = SensorNode(self.config, sensor_id="TEST_SENSOR", location="Test Lab")
        self.sensor_node.start()
        
        print("✓ Nodes setup complete")
    
    def run_communication_test(self):
        """Test basic communication between nodes."""
        print("\n🧪 Testing Node Communication...")
        
        # Start sensor readings
        self.sensor_node.start_sensing()
        
        # Let them communicate for a bit
        print("- Starting sensor readings...")
        time.sleep(5)
        
        # Check if monitor received data
        sensor_summary = self.monitor_node.get_sensor_summary()
        if sensor_summary:
            print("✓ Communication successful! Monitor received sensor data:")
            for sensor_id, info in sensor_summary.items():
                print(f"  Sensor {sensor_id}: {info['reading_count']} readings received")
        else:
            print("❌ Communication failed - no sensor data received")
        
        return len(sensor_summary) > 0
    
    def run_knowledge_test(self):
        """Test knowledge management functionality."""
        print("\n🧠 Testing Knowledge Management...")
        
        # Test writing and reading simple data
        test_key = "test_data"
        test_value = {"message": "Hello from test!", "timestamp": time.time()}
        
        # Write to knowledge base
        self.sensor_node.write_knowledge(test_key, str(test_value))
        
        # Read from knowledge base using the other node
        retrieved_value = self.monitor_node.read_knowledge(test_key)
        
        if retrieved_value:
            print(f"✓ Knowledge storage successful!")
            print(f"  Stored: {test_value}")
            print(f"  Retrieved: {retrieved_value}")
            return True
        else:
            print("❌ Knowledge storage failed")
            return False
    
    def run_event_test(self):
        """Test event publishing and subscription."""
        print("\n📡 Testing Event System...")
        
        # Let the sensor run and generate events
        time.sleep(8)
        
        # Check monitor's statistics to see if events were received
        stats = self.monitor_node.get_statistics()
        
        if stats['total_readings'] > 0:
            print("✓ Event system working!")
            print(f"  Events received: {stats['total_readings']}")
            print(f"  Active sensors: {stats['active_sensors']}")
            return True
        else:
            print("❌ Event system failed - no events received")
            return False
    
    def run_alert_test(self):
        """Test alert generation and handling."""
        print("\n🚨 Testing Alert System...")
        
        # Force sensor to generate some extreme readings by manipulating temperature
        original_base = self.sensor_node.base_temperature
        self.sensor_node.base_temperature = 35.0  # Force high temperature
        
        # Wait for alerts
        time.sleep(6)
        
        # Check for alerts
        alerts = self.monitor_node.get_recent_alerts(5)
        
        # Restore original temperature
        self.sensor_node.base_temperature = original_base
        
        if alerts:
            print("✓ Alert system working!")
            print(f"  Alerts generated: {len(alerts)}")
            for alert in alerts[-2:]:  # Show last 2 alerts
                print(f"  - {alert['type']}: {alert['message']}")
            return True
        else:
            print("❌ Alert system failed - no alerts generated")
            return False
    
    def run_all_tests(self):
        """Run all tests."""
        print("🚀 Starting RPC Library Tests")
        print("=" * 50)
        
        try:
            # Setup
            self.setup_nodes()
            
            # Run tests
            test_results = []
            test_results.append(("Communication", self.run_communication_test()))
            test_results.append(("Knowledge Management", self.run_knowledge_test()))
            test_results.append(("Event System", self.run_event_test()))
            test_results.append(("Alert System", self.run_alert_test()))
            
            # Let everything settle
            time.sleep(3)
            
            # Show final summary
            print("\n📊 Final System Status:")
            stats = self.monitor_node.get_statistics()
            sensor_stats = self.sensor_node.get_stats()
            
            print(f"  Monitor - Total readings: {stats['total_readings']}, Alerts: {stats['alert_count']}")
            print(f"  Sensor - Readings sent: {sensor_stats['readings_count']}")
            
            # Show test results
            print("\n🎯 Test Results Summary:")
            print("=" * 30)
            passed = 0
            for test_name, result in test_results:
                status = "PASS" if result else "FAIL"
                print(f"  {test_name}: {status}")
                if result:
                    passed += 1
            
            print(f"\nOverall: {passed}/{len(test_results)} tests passed")
            
            if passed == len(test_results):
                print("🎉 All tests passed! Your library is working correctly.")
            else:
                print("⚠️  Some tests failed. Check your configuration and dependencies.")
            
        except Exception as e:
            print(f"❌ Test failed with error: {e}")
        
        finally:
            # Cleanup
            print("\n🧹 Cleaning up...")
            if self.sensor_node:
                self.sensor_node.stop_sensing()
                self.sensor_node.shutdown()
            if self.monitor_node:
                self.monitor_node.stop_monitoring()
                self.monitor_node.shutdown()
            print("✓ Cleanup complete")


def main():
    """Main function to run the tests."""
    print("RPC Library Test Suite")
    print("This will test communication, knowledge management, events, and alerts")
    print("Make sure Redis is running on localhost:6379")
    print("\nPress Enter to start tests, or Ctrl+C to exit...")
    
    try:
        input()
        controller = TestController()
        controller.run_all_tests()
    except KeyboardInterrupt:
        print("\nTest cancelled by user.")
    except Exception as e:
        print(f"Test failed: {e}")


if __name__ == "__main__":
    main()
