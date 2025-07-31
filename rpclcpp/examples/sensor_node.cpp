#include "../include/node.hpp"
#include "data_models.hpp"
#include <random>
#include <thread>
#include <chrono>
#include <atomic>

using namespace rpclcpp;
using namespace rpclcpp_examples;

class SensorNode : public Node
{
private:
    std::string sensor_id_;
    std::string location_;
    std::atomic<bool> sensing_;
    std::thread sensor_thread_;
    int reading_count_;

    // Temperature simulation
    double base_temperature_;
    double temperature_drift_;
    std::mt19937 rng_;
    std::uniform_real_distribution<double> noise_dist_;
    std::uniform_real_distribution<double> drift_dist_;
    std::uniform_real_distribution<double> spike_dist_;
    std::uniform_real_distribution<double> prob_dist_;

public:
    SensorNode(const std::string &sensor_id, const std::string &location)
        : Node("SensorNode"), sensor_id_(sensor_id), location_(location),
          sensing_(false), reading_count_(0),
          base_temperature_(22.0), temperature_drift_(0.0),
          rng_(std::random_device{}()),
          noise_dist_(-1.5, 1.5),
          drift_dist_(-0.1, 0.1),
          spike_dist_(-10.0, 15.0),
          prob_dist_(0.0, 1.0)
    {

        get_logger().info("Sensor Node initialized: " + sensor_id + " at " + location);
    }

    ~SensorNode()
    {
        stop_sensing();
    }

    void start_sensing()
    {
        if (sensing_)
            return;

        sensing_ = true;
        get_logger().info("Starting sensor readings...");

        sensor_thread_ = std::thread([this]()
                                     { sensor_loop(); });
    }

    void stop_sensing()
    {
        if (!sensing_)
            return;

        sensing_ = false;
        get_logger().info("Stopping sensor readings...");

        if (sensor_thread_.joinable())
        {
            sensor_thread_.join();
        }
    }

private:
    void sensor_loop()
    {
        while (sensing_)
        {
            try
            {
                // Generate temperature reading
                double temperature = generate_temperature();

                // Create sensor reading
                SensorReading reading;
                reading.sensor_id = sensor_id_;
                reading.temperature = temperature;
                reading.location = location_;
                reading.status = determine_status(temperature);
                reading.uid = generate_uuid();
                reading.timestamp = get_timestamp();

                reading_count_++;

                // Store in knowledge base
                json reading_json = reading;
                write_knowledge(SensorReading::KNOWLEDGE_KEY, reading_json.dump());

                // Publish as event
                publish_event(SensorReading::TOPIC, reading_json);

                // Send direct message with temperature
                std::string temp_topic = "sensor/" + sensor_id_ + "/temperature";
                publish_message(temp_topic, std::to_string(temperature));

                get_logger().info("Published reading: " + sensor_id_ + " = " +
                                  std::to_string(temperature) + "°C [" + reading.status + "]");

                // Send alert if needed
                if (reading.status != "normal")
                {
                    send_alert(reading);
                }

                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
            catch (const std::exception &e)
            {
                get_logger().error("Error in sensor loop: " + std::string(e.what()));
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    }

    double generate_temperature()
    {
        // Add noise
        double noise = noise_dist_(rng_);

        // Add gradual drift
        temperature_drift_ += drift_dist_(rng_);
        temperature_drift_ = std::max(-5.0, std::min(5.0, temperature_drift_));

        // Occasionally add spike
        double spike = 0.0;
        if (prob_dist_(rng_) < 0.05)
        { // 5% chance
            spike = spike_dist_(rng_);
        }

        double temperature = base_temperature_ + temperature_drift_ + noise + spike;
        return std::round(temperature * 10.0) / 10.0; // Round to 1 decimal
    }

    std::string determine_status(double temperature)
    {
        if (temperature < 15)
            return "too_cold";
        else if (temperature > 30)
            return "too_hot";
        else if (temperature < 18 || temperature > 26)
            return "warning";
        else
            return "normal";
    }

    void send_alert(const SensorReading &reading)
    {
        Alert alert;
        alert.sensor_id = reading.sensor_id;
        alert.uid = generate_uuid();
        alert.timestamp = get_timestamp();

        if (reading.status == "too_cold")
        {
            alert.alert_type = "low_temperature";
            alert.message = "Temperature too low: " + std::to_string(reading.temperature) +
                            "°C at " + reading.location;
            alert.severity = "high";
        }
        else if (reading.status == "too_hot")
        {
            alert.alert_type = "high_temperature";
            alert.message = "Temperature too high: " + std::to_string(reading.temperature) +
                            "°C at " + reading.location;
            alert.severity = "high";
        }
        else
        {
            alert.alert_type = "temperature_warning";
            alert.message = "Temperature outside normal range: " + std::to_string(reading.temperature) +
                            "°C at " + reading.location;
            alert.severity = "medium";
        }

        // Store alert in knowledge base
        json alert_json = alert;
        write_knowledge(Alert::KNOWLEDGE_KEY, alert_json.dump());

        // Publish alert event
        publish_event(Alert::TOPIC, alert_json);

        get_logger().warning("Alert sent: " + alert.message + " [Severity: " + alert.severity + "]");
    }

public:
    json get_stats()
    {
        return json{
            {"sensor_id", sensor_id_},
            {"location", location_},
            {"readings_count", reading_count_},
            {"status", sensing_ ? "active" : "inactive"}};
    }
};

int main()
{
    try
    {
        // Create sensor node
        SensorNode sensor("TEMP_001", "Lab Room A");

        // Start the node
        if (!sensor.start())
        {
            std::cerr << "Failed to start sensor node" << std::endl;
            return -1;
        }

        // Start sensing
        sensor.start_sensing();

        std::cout << "Sensor node started. Press Ctrl+C to stop..." << std::endl;
        std::cout << "Sensor ID: TEMP_001" << std::endl;
        std::cout << "Location: Lab Room A" << std::endl;
        std::cout << std::string(50, '-') << std::endl;

        // Keep running and show stats
        while (true)
        {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            json stats = sensor.get_stats();
            std::cout << "Stats: " << stats["readings_count"] << " readings sent" << std::endl;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Sensor node error: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}
