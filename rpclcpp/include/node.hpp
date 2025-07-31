#pragma once

#include "communication_manager.hpp"
#include "knowledge_manager.hpp"
#include "logger.hpp"
#include <string>
#include <memory>
#include <functional>
#include <map>
#include <nlohmann/json.hpp>

namespace rpclcpp
{

    using json = nlohmann::json;

    class Node
    {
    public:
        using EventCallback = std::function<void(const std::string &topic, const std::string &message)>;
        using MessageCallback = std::function<void(const std::string &topic, const std::string &message)>;

        Node(const std::string &name,
             const std::string &redis_host = "localhost",
             int redis_port = 6379);

        virtual ~Node();

        // Lifecycle methods
        bool start();
        void shutdown();
        bool is_running() const;

        // Communication methods
        bool publish_event(const std::string &topic, const json &message);
        bool publish_message(const std::string &topic, const std::string &message);

        bool register_event_callback(const std::string &topic, EventCallback callback);
        bool register_message_callback(const std::string &topic, MessageCallback callback);

        // Knowledge management methods
        bool write_knowledge(const std::string &key, const std::string &value);
        std::string read_knowledge(const std::string &key);
        bool knowledge_exists(const std::string &key);

        // Template methods for object knowledge
        template <typename T>
        bool write_knowledge_object(const std::string &key, const T &obj);

        template <typename T>
        T read_knowledge_object(const std::string &key);

        // Utility methods
        std::string generate_uuid();
        std::string get_timestamp();

    protected:
        Logger &get_logger();

    private:
        class Impl;
        std::unique_ptr<Impl> pImpl;
    };

    // Template implementations
    template <typename T>
    bool Node::write_knowledge_object(const std::string &key, const T &obj)
    {
        json j = obj;
        return write_knowledge(key, j.dump());
    }

    template <typename T>
    T Node::read_knowledge_object(const std::string &key)
    {
        std::string data = read_knowledge(key);
        if (data.empty())
        {
            return T{};
        }
        json j = json::parse(data);
        return j.get<T>();
    }

} // namespace rpclcpp
