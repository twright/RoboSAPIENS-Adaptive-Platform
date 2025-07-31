cmake_minimum_required(VERSION 3.12)

# Find required packages
find_package(nlohmann_json REQUIRED)

# Include parent directory for rpclcpp headers
include_directories(../include)

# Sensor node executable
add_executable(sensor_node sensor_node.cpp)
target_link_libraries(sensor_node rpclcpp nlohmann_json::nlohmann_json)

# Monitor node executable
add_executable(monitor_node monitor_node.cpp)
target_link_libraries(monitor_node rpclcpp nlohmann_json::nlohmann_json)

# Install executables
install(TARGETS sensor_node monitor_node
    RUNTIME DESTINATION bin
)
