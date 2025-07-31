#pragma once

#include <nlohmann/json.hpp>
#include <string>

namespace rpclcpp_examples
{

    using json = nlohmann::json;

    struct SensorReading
    {
        std::string sensor_id;
        double temperature;
        std::string location;
        std::string status;
        std::string uid;
        std::string timestamp;

        // JSON serialization
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(SensorReading, sensor_id, temperature, location, status, uid, timestamp)

        static constexpr const char *TOPIC = "sensor/temperature";
        static constexpr const char *KNOWLEDGE_KEY = "sensor_reading";
    };

    struct Alert
    {
        std::string alert_type;
        std::string message;
        std::string severity;
        std::string sensor_id;
        std::string uid;
        std::string timestamp;

        // JSON serialization
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(Alert, alert_type, message, severity, sensor_id, uid, timestamp)

        static constexpr const char *TOPIC = "alerts/temperature";
        static constexpr const char *KNOWLEDGE_KEY = "alert";
    };

    struct SystemStatus
    {
        int active_sensors;
        int total_readings;
        int alerts_count;
        double avg_temperature;
        std::string uid;
        std::string timestamp;

        // JSON serialization
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(SystemStatus, active_sensors, total_readings, alerts_count, avg_temperature, uid, timestamp)

        static constexpr const char *TOPIC = "system/status";
        static constexpr const char *KNOWLEDGE_KEY = "system_status";
    };

} // namespace rpclcpp_examples
