# RPC Library Examples

This folder contains examples demonstrating how to use the rpclpy library for node communication.

## Sensor Monitor Example

This example demonstrates:

- Node-to-node communication using events and messages
- Knowledge management for storing and retrieving data
- Real-time sensor data publishing and monitoring

### Components:

- **SensorNode**: Simulates a temperature sensor that publishes readings
- **MonitorNode**: Subscribes to sensor data and analyzes it
- **Data Models**: Defines the structure for sensor data and alerts

### How to Run:

1. Make sure you have Redis running locally (or configure for another backend)
2. Run the monitor node first: `python monitor_node.py`
3. Run the sensor node: `python sensor_node.py`
4. Watch the nodes communicate in real-time!

### Configuration:

Both nodes use the same configuration file (`config.yaml`) which defines:

- Communication protocols (MQTT for events, Redis for messages)
- Knowledge storage backend (Redis)
- Logging settings
