#include "../include/node.hpp"
#include "data_models.hpp"
#include <map>
#include <deque>
#include <mutex>
#include <thread>
#include <atomic>
#include <iostream>

using namespace rpclcpp;
using namespace rpclcpp_examples;

class MonitorNode : public Node
{
private:
    std::atomic<bool> monitoring_;
    std::thread status_thread_;

    // Data storage
    std::map<std::string, std::deque<json>> sensor_data_;
    std::deque<Alert> alerts_;

    // Statistics
    struct Statistics
    {
        int total_readings = 0;
        std::set<std::string> active_sensors;
        int alert_count = 0;
        double temperature_sum = 0.0;
    } statistics_;

    std::mutex data_mutex_;
    static constexpr size_t MAX_READINGS_PER_SENSOR = 50;
    static constexpr size_t MAX_ALERTS = 100;

public:
    MonitorNode() : Node("MonitorNode"), monitoring_(false)
    {
        get_logger().info("Monitor Node initialized");
    }

    ~MonitorNode()
    {
        stop_monitoring();
    }

    void start_monitoring()
    {
        if (monitoring_)
            return;

        monitoring_ = true;

        // Register event callbacks
        register_event_callback(SensorReading::TOPIC,
                                [this](const std::string &topic, const std::string &message)
                                {
                                    handle_sensor_reading(topic, message);
                                });

        register_event_callback(Alert::TOPIC,
                                [this](const std::string &topic, const std::string &message)
                                {
                                    handle_alert(topic, message);
                                });

        // Register message callback for direct temperature messages
        register_message_callback("sensor/+/temperature",
                                  [this](const std::string &topic, const std::string &message)
                                  {
                                      handle_direct_message(topic, message);
                                  });

        // Start status reporting thread
        status_thread_ = std::thread([this]()
                                     { status_loop(); });

        get_logger().info("Started monitoring sensor data...");
    }

    void stop_monitoring()
    {
        if (!monitoring_)
            return;

        monitoring_ = false;
        get_logger().info("Stopped monitoring.");

        if (status_thread_.joinable())
        {
            status_thread_.join();
        }
    }

private:
    void handle_sensor_reading(const std::string &topic, const std::string &message)
    {
        try
        {
            json data = json::parse(message);

            std::string sensor_id = data.value("sensor_id", "unknown");
            double temperature = data.value("temperature", 0.0);
            std::string location = data.value("location", "unknown");
            std::string status = data.value("status", "unknown");
            std::string timestamp = data.value("timestamp", "unknown");

            std::lock_guard<std::mutex> lock(data_mutex_);

            // Store reading
            json reading_info = {
                {"temperature", temperature},
                {"location", location},
                {"status", status},
                {"timestamp", timestamp}};

            sensor_data_[sensor_id].push_back(reading_info);

            // Keep only recent readings
            if (sensor_data_[sensor_id].size() > MAX_READINGS_PER_SENSOR)
            {
                sensor_data_[sensor_id].pop_front();
            }

            // Update statistics
            statistics_.total_readings++;
            statistics_.active_sensors.insert(sensor_id);
            statistics_.temperature_sum += temperature;

            get_logger().info("Processed reading from " + sensor_id + ": " +
                              std::to_string(temperature) + "°C [" + status + "]");

            // Analyze sensor data
            analyze_sensor_data(sensor_id, reading_info);
        }
        catch (const std::exception &e)
        {
            get_logger().error("Error processing sensor reading: " + std::string(e.what()));
        }
    }

    void handle_alert(const std::string &topic, const std::string &message)
    {
        try
        {
            json data = json::parse(message);

            Alert alert;
            alert.alert_type = data.value("alert_type", "unknown");
            alert.message = data.value("message", "");
            alert.severity = data.value("severity", "low");
            alert.sensor_id = data.value("sensor_id", "unknown");
            alert.timestamp = data.value("timestamp", "unknown");

            std::lock_guard<std::mutex> lock(data_mutex_);
            alerts_.push_back(alert);

            // Keep only recent alerts
            if (alerts_.size() > MAX_ALERTS)
            {
                alerts_.pop_front();
            }

            statistics_.alert_count++;

            get_logger().warning("Alert received: " + alert.message +
                                 " [Severity: " + alert.severity + "]");

            handle_alert_response(alert);
        }
        catch (const std::exception &e)
        {
            get_logger().error("Error processing alert: " + std::string(e.what()));
        }
    }

    void handle_direct_message(const std::string &topic, const std::string &message)
    {
        try
        {
            // Extract sensor ID from topic (e.g., "sensor/TEMP_001/temperature")
            size_t first_slash = topic.find('/');
            size_t second_slash = topic.find('/', first_slash + 1);

            if (first_slash != std::string::npos && second_slash != std::string::npos)
            {
                std::string sensor_id = topic.substr(first_slash + 1,
                                                     second_slash - first_slash - 1);
                double temperature = std::stod(message);

                get_logger().debug("Direct message from " + sensor_id + ": " +
                                   std::to_string(temperature) + "°C");
            }
        }
        catch (const std::exception &e)
        {
            get_logger().error("Error processing direct message: " + std::string(e.what()));
        }
    }

    void analyze_sensor_data(const std::string &sensor_id, const json &reading)
    {
        try
        {
            std::vector<double> recent_temps;

            // Get recent temperatures (last 5 readings)
            auto &readings = sensor_data_[sensor_id];
            size_t start = readings.size() >= 5 ? readings.size() - 5 : 0;

            for (size_t i = start; i < readings.size(); ++i)
            {
                recent_temps.push_back(readings[i]["temperature"]);
            }

            if (recent_temps.size() < 2)
                return;

            // Detect rapid temperature changes
            double temp_change = std::abs(recent_temps.back() - recent_temps[recent_temps.size() - 2]);
            if (temp_change > 5.0)
            {
                get_logger().warning("Rapid temperature change detected for " + sensor_id +
                                     ": " + std::to_string(temp_change) + "°C");
            }

            // Detect trends
            if (recent_temps.size() >= 3)
            {
                bool increasing = true, decreasing = true;

                for (size_t i = 1; i < recent_temps.size(); ++i)
                {
                    if (recent_temps[i] <= recent_temps[i - 1])
                        increasing = false;
                    if (recent_temps[i] >= recent_temps[i - 1])
                        decreasing = false;
                }

                if (increasing || decreasing)
                {
                    std::string trend = increasing ? "increasing" : "decreasing";
                    get_logger().info("Temperature trend for " + sensor_id + ": " + trend);
                }
            }
        }
        catch (const std::exception &e)
        {
            get_logger().error("Error analyzing sensor data: " + std::string(e.what()));
        }
    }

    void handle_alert_response(const Alert &alert)
    {
        if (alert.severity == "high")
        {
            get_logger().critical("HIGH SEVERITY ALERT: " + alert.message);
        }
        else if (alert.severity == "medium")
        {
            get_logger().warning("Medium severity alert: " + alert.message);
        }

        // Store alert in knowledge base
        std::string alert_key = "alert_" + alert.timestamp;
        json alert_json = alert;
        write_knowledge(alert_key, alert_json.dump());
    }

    void status_loop()
    {
        while (monitoring_)
        {
            try
            {
                SystemStatus status;

                {
                    std::lock_guard<std::mutex> lock(data_mutex_);

                    // Calculate average temperature
                    double avg_temp = 0.0;
                    if (statistics_.total_readings > 0)
                    {
                        avg_temp = statistics_.temperature_sum / statistics_.total_readings;
                    }

                    status.active_sensors = static_cast<int>(statistics_.active_sensors.size());
                    status.total_readings = statistics_.total_readings;
                    status.alerts_count = statistics_.alert_count;
                    status.avg_temperature = avg_temp;
                    status.uid = generate_uuid();
                    status.timestamp = get_timestamp();
                }

                // Store system status in knowledge base
                json status_json = status;
                write_knowledge(SystemStatus::KNOWLEDGE_KEY, status_json.dump());

                // Publish system status as event
                publish_event(SystemStatus::TOPIC, status_json);

                get_logger().info("System Status - Sensors: " + std::to_string(status.active_sensors) +
                                  ", Readings: " + std::to_string(status.total_readings) +
                                  ", Alerts: " + std::to_string(status.alerts_count) +
                                  ", Avg Temp: " + std::to_string(status.avg_temperature) + "°C");

                std::this_thread::sleep_for(std::chrono::seconds(10));
            }
            catch (const std::exception &e)
            {
                get_logger().error("Error in status loop: " + std::string(e.what()));
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }
    }

public:
    json get_sensor_summary()
    {
        std::lock_guard<std::mutex> lock(data_mutex_);
        json summary = json::object();

        for (const auto &[sensor_id, readings] : sensor_data_)
        {
            if (!readings.empty())
            {
                const auto &latest = readings.back();
                summary[sensor_id] = {
                    {"latest_temperature", latest["temperature"]},
                    {"status", latest["status"]},
                    {"location", latest["location"]},
                    {"reading_count", readings.size()},
                    {"last_seen", latest["timestamp"]}};
            }
        }

        return summary;
    }

    json get_recent_alerts(size_t count = 10)
    {
        std::lock_guard<std::mutex> lock(data_mutex_);
        json alert_list = json::array();

        size_t start = alerts_.size() > count ? alerts_.size() - count : 0;
        for (size_t i = start; i < alerts_.size(); ++i)
        {
            alert_list.push_back(json(alerts_[i]));
        }

        return alert_list;
    }

    json get_statistics()
    {
        std::lock_guard<std::mutex> lock(data_mutex_);

        double avg_temp = 0.0;
        if (statistics_.total_readings > 0)
        {
            avg_temp = statistics_.temperature_sum / statistics_.total_readings;
        }

        return json{
            {"active_sensors", statistics_.active_sensors.size()},
            {"total_readings", statistics_.total_readings},
            {"alert_count", statistics_.alert_count},
            {"average_temperature", avg_temp}};
    }
};

int main()
{
    try
    {
        // Create monitor node
        MonitorNode monitor;

        // Start the node
        if (!monitor.start())
        {
            std::cerr << "Failed to start monitor node" << std::endl;
            return -1;
        }

        // Start monitoring
        monitor.start_monitoring();

        std::cout << "Monitor node started. Press Ctrl+C to stop..." << std::endl;
        std::cout << "Waiting for sensor data..." << std::endl;
        std::cout << std::string(50, '-') << std::endl;

        // Periodically display status
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(15));

            std::cout << "\n"
                      << std::string(60, '=') << std::endl;
            std::cout << "SENSOR MONITORING STATUS" << std::endl;
            std::cout << std::string(60, '=') << std::endl;

            // Show statistics
            json stats = monitor.get_statistics();
            std::cout << "Active Sensors: " << stats["active_sensors"] << std::endl;
            std::cout << "Total Readings: " << stats["total_readings"] << std::endl;
            std::cout << "Alert Count: " << stats["alert_count"] << std::endl;
            std::cout << "Average Temperature: " << std::fixed << std::setprecision(1)
                      << stats["average_temperature"].get<double>() << "°C" << std::endl;

            // Show sensor summary
            json sensors = monitor.get_sensor_summary();
            if (!sensors.empty())
            {
                std::cout << "\nSensor Details:" << std::endl;
                for (const auto &[sensor_id, info] : sensors.items())
                {
                    std::cout << "  " << sensor_id << ": "
                              << info["latest_temperature"] << "°C ["
                              << info["status"] << "] at "
                              << info["location"] << std::endl;
                }
            }

            // Show recent alerts
            json alerts = monitor.get_recent_alerts(3);
            if (!alerts.empty())
            {
                std::cout << "\nRecent Alerts (" << alerts.size() << "):" << std::endl;
                for (const auto &alert : alerts)
                {
                    std::cout << "  [" << alert["severity"] << "] "
                              << alert["message"] << std::endl;
                }
            }

            std::cout << std::string(60, '-') << std::endl;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Monitor node error: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}
