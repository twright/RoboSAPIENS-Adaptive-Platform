# RPCLCPP - RoboSapiens Client Library for C++

A C++ library for creating nodes that can communicate with each other using messages, events, and knowledge management via Redis.

## Dependencies

- CMake 3.12+
- C++17 compiler
- hiredis (Redis C client)
- nlohmann/json
- Redis server

## Installation

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install cmake build-essential libhiredis-dev nlohmann-json3-dev redis-server
```

### Build

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
sudo make install
```

## Examples

### Sensor Monitor Example

This example demonstrates:

- Node-to-node communication using events and messages
- Knowledge management for storing and retrieving data
- Real-time sensor data publishing and monitoring

#### Build Examples

```bash
cd build
make sensor_node monitor_node
```

#### Run

1. Start Redis: `sudo systemctl start redis-server`
2. Run monitor: `./sensor_node`
3. Run sensor: `./monitor_node` (in another terminal)

## Features

- **Communication**: Redis pub/sub for events and messages
- **Knowledge Management**: Redis key-value storage
- **Thread-safe**: Concurrent operations supported
- **Type-safe**: Modern C++17 with JSON serialization
- **Logging**: Built-in structured logging

## API Overview

```cpp
#include "node.hpp"

class MyNode : public rpclcpp::Node {
public:
    MyNode() : Node("MyNode") {}

    void example() {
        // Publish event
        json data = {{"temperature", 22.5}};
        publish_event("sensor/data", data);

        // Subscribe to events
        register_event_callback("alerts/high", [](const std::string& topic, const std::string& msg) {
            // Handle alert
        });

        // Knowledge management
        write_knowledge("config", "value");
        std::string value = read_knowledge("config");
    }
};
```
