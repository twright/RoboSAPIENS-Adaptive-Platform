# rpclpy — RoboSapiens Python Client Library

A high-performance Python library for building distributed node-based systems with communication, events, and knowledge management capabilities, backed by Redis.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Redis](https://img.shields.io/badge/Redis-6.0+-red.svg)](https://redis.io)
[![PyPI](https://img.shields.io/pypi/v/rpclpy.svg)](https://pypi.org/project/rpclpy/)

## Features

- **Node Communication**: Publish/subscribe messaging and events with automatic metadata
- **Knowledge Management**: Distributed key-value storage with object serialization
- **Redis Integration**: Fast, reliable backend for all operations across multiple databases
- **Real-time Communication**: Event-driven architecture with thread-safe operations
- **Flexible Protocols**: Redis, MQTT, RabbitMQ, Kafka, WebSocket, TCP, Zenoh
- **Multiple Knowledge Backends**: Redis, Memcached, Ignite, Hazelcast, Tarantool, Aerospike, SQLite
- **Logging & Monitoring**: Built-in structured logging with configurable levels (terminal, file, MQTT, Redis)
- **UUID & Timestamps**: Automatic message identification and timestamping
- **Dashboard**: Built-in web dashboard via Dash for real-time monitoring

## Installation

```bash
pip install rpclpy
```

Install optional extras based on what you need:

```bash
pip install rpclpy[mqtt]       # MQTT protocol / logging
pip install rpclpy[kafka]      # Kafka protocol / backend
pip install rpclpy[rabbitmq]   # RabbitMQ protocol
pip install rpclpy[websocket]  # WebSocket protocol
pip install rpclpy[dashboard]  # Web dashboard (Dash + Plotly)
pip install rpclpy[all]        # Everything
```

## Repository Structure

```
rpclpy/
├── __init__.py               # Library entry point
├── node.py                   # Main Node class
├── CommunicationManager.py   # Multi-protocol pub/sub
├── KnowledgeManager.py       # Knowledge storage backends
├── LoggingAndTracking.py     # Logging infrastructure
├── DashboardApp.py           # Web monitoring dashboard
└── utils.py                  # Utility decorators
```

## Quick Start

```python
from rpclpy.node import Node
import yaml

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

node = Node(config)
node.start()

# Publish an event
node.publish_event("sensor/data", {"temperature": 22.5})

# Subscribe to events
node.register_event_callback("alerts/high", handle_alert)

# Read / write knowledge
node.write_knowledge("last_reading", {"value": 22.5})
data = node.read_knowledge("last_reading")
```

## Prerequisites

- Python 3.8+
- Redis 6.0+ running on localhost:6379

**Install Redis:**

```bash
# Ubuntu/Debian
sudo apt update && sudo apt install redis-server
sudo systemctl start redis-server

# macOS
brew install redis && brew services start redis

# Docker
docker run --name redis-server -p 6379:6379 -d redis
```

## Configuration

rpclpy is fully configuration-driven via a YAML file. Redis databases are separated by concern:

- **DB 0**: Events (pub/sub)
- **DB 1**: Messages (pub/sub)
- **DB 2**: Knowledge (key-value)

```yaml
Logger_Config:
  log_level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  logger_type: "terminal"   # terminal | file | mqtt | redis

Event_Manager_Config:
  protocol: "redis"
  host: "localhost"
  port: 6379
  db: 0

Message_Manager_Config:
  protocol: "redis"
  host: "localhost"
  port: 6379
  db: 1

Knowledge_Config:
  knowledge_type: "redis"
  host: "localhost"
  port: 6379
  db: 2
```

## API Reference

| Method | Description |
|--------|-------------|
| `Node(config)` | Create a node from a config dict |
| `node.start()` | Start event and message managers |
| `node.shutdown()` | Stop all managers |
| `node.publish_event(topic, data)` | Publish an event |
| `node.register_event_callback(topic, fn)` | Subscribe to an event topic |
| `node.publish_message(topic, data)` | Publish a direct message |
| `node.register_message_callback(topic, fn)` | Subscribe to a message topic |
| `node.write_knowledge(key, value)` | Store a value |
| `node.read_knowledge(key)` | Retrieve a value |

## Architecture

```
┌─────────────┐    Events/Messages    ┌─────────────┐
│  Your Node  │◄─────────────────────►│  Other Node │
└─────────────┘       Real-time       └─────────────┘
       │              Communication           │
       ▼                                     ▼
┌─────────────────────────────────────────────────────┐
│                    Redis Backend                    │
│  ┌─────────┐  ┌─────────┐  ┌─────────────────────┐  │
│  │ Events  │  │Messages │  │   Knowledge Base    │  │
│  │ (DB 0)  │  │ (DB 1)  │  │      (DB 2)         │  │
│  │ Pub/Sub │  │ Pub/Sub │  │    Key-Value        │  │
│  └─────────┘  └─────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

## Examples

### Sensor Monitoring System

The repository includes a complete real-time sensor monitoring example:

```bash
cd rpclpy/examples

# Terminal 1 — start the monitor
python monitor_node.py

# Terminal 2 — start the sensor
python sensor_node.py
```

Output:

```
SENSOR MONITORING STATUS
============================================================
Active Sensors: 1
Total Readings: 47
Alert Count: 3
Average Temperature: 23.2°C

Sensor Details:
  TEMP_001: 24.1°C [normal] at Lab Room A

Recent Alerts (2):
  [HIGH] Temperature too high: 31.2°C at Lab Room A
  [MEDIUM] Temperature outside normal range: 27.8°C at Lab Room A
------------------------------------------------------------
```

## Troubleshooting

**Redis connection failed:**
```bash
redis-cli ping   # should return PONG
sudo systemctl start redis-server   # Linux
brew services start redis           # macOS
```

**Import errors after install:**
```bash
# Verify the install
python -c "import rpclpy; print(rpclpy.__version__)"
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Set up the dev environment:
   ```bash
   docker run --name redis-dev -p 6379:6379 -d redis
   pip install -e ".[all]"
   ```
4. Follow PEP 8 style
5. Submit a pull request with a clear description

## License

MIT License

## Version

**Current version: 1.0.0**

### Changelog

- **v1.0.0** (2025-07-31): Initial release
  - Core node communication (events, messages, knowledge)
  - Multi-protocol support: Redis, MQTT, RabbitMQ, Kafka, WebSocket, TCP, Zenoh
  - Multiple knowledge backends
  - Thread-safe operations
  - Structured logging and web dashboard

---

**Built with ❤️ by the RoboSapiens Team**

For questions, support, or contributions, visit the [GitHub repository](https://github.com/saharnn96/robosapiens-clientlibraries).
