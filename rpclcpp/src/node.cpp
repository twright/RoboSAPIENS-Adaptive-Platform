#include "node.hpp"
#include <random>
#include <sstream>
#include <iomanip>
#include <chrono>

namespace rpclcpp
{

    class Node::Impl
    {
    public:
        std::string name;
        std::unique_ptr<CommunicationManager> event_manager;
        std::unique_ptr<CommunicationManager> message_manager;
        std::unique_ptr<KnowledgeManager> knowledge_manager;
        std::unique_ptr<Logger> logger;
        std::atomic<bool> running;

        Impl(const std::string &n, const std::string &host, int port)
            : name(n), running(false)
        {

            // Create managers with different Redis databases
            event_manager = std::make_unique<CommunicationManager>(host, port, 0);   // DB 0 for events
            message_manager = std::make_unique<CommunicationManager>(host, port, 1); // DB 1 for messages
            knowledge_manager = std::make_unique<KnowledgeManager>(host, port, 2);   // DB 2 for knowledge
            logger = std::make_unique<Logger>(name, LogLevel::INFO);
        }
    };

    Node::Node(const std::string &name, const std::string &redis_host, int redis_port)
        : pImpl(std::make_unique<Impl>(name, redis_host, redis_port))
    {
        pImpl->logger->info("Node " + name + " initialized");
    }

    Node::~Node()
    {
        if (is_running())
        {
            shutdown();
        }
    }

    bool Node::start()
    {
        if (pImpl->running)
        {
            return true;
        }

        pImpl->logger->info("Starting node " + pImpl->name);

        if (!pImpl->event_manager->start())
        {
            pImpl->logger->error("Failed to start event manager");
            return false;
        }

        if (!pImpl->message_manager->start())
        {
            pImpl->logger->error("Failed to start message manager");
            pImpl->event_manager->stop();
            return false;
        }

        pImpl->running = true;
        pImpl->logger->info("Node " + pImpl->name + " started successfully");
        return true;
    }

    void Node::shutdown()
    {
        if (!pImpl->running)
        {
            return;
        }

        pImpl->logger->info("Shutting down node " + pImpl->name);

        pImpl->running = false;
        pImpl->event_manager->stop();
        pImpl->message_manager->stop();

        pImpl->logger->info("Node " + pImpl->name + " shut down");
    }

    bool Node::is_running() const
    {
        return pImpl->running;
    }

    bool Node::publish_event(const std::string &topic, const json &message)
    {
        if (!pImpl->running)
        {
            return false;
        }

        // Add metadata
        json enriched_message = message;
        enriched_message["uid"] = generate_uuid();
        enriched_message["timestamp"] = get_timestamp();

        bool result = pImpl->event_manager->publish(topic, enriched_message.dump());
        if (result)
        {
            pImpl->logger->info("Published event to topic: " + topic);
        }
        else
        {
            pImpl->logger->error("Failed to publish event to topic: " + topic);
        }

        return result;
    }

    bool Node::publish_message(const std::string &topic, const std::string &message)
    {
        if (!pImpl->running)
        {
            return false;
        }

        bool result = pImpl->message_manager->publish(topic, message);
        if (result)
        {
            pImpl->logger->info("Published message to topic: " + topic);
        }
        else
        {
            pImpl->logger->error("Failed to publish message to topic: " + topic);
        }

        return result;
    }

    bool Node::register_event_callback(const std::string &topic, EventCallback callback)
    {
        if (!pImpl->running)
        {
            return false;
        }

        bool result = pImpl->event_manager->subscribe(topic, callback);
        if (result)
        {
            pImpl->logger->info("Registered event callback for topic: " + topic);
        }
        else
        {
            pImpl->logger->error("Failed to register event callback for topic: " + topic);
        }

        return result;
    }

    bool Node::register_message_callback(const std::string &topic, MessageCallback callback)
    {
        if (!pImpl->running)
        {
            return false;
        }

        bool result = pImpl->message_manager->subscribe(topic, callback);
        if (result)
        {
            pImpl->logger->info("Registered message callback for topic: " + topic);
        }
        else
        {
            pImpl->logger->error("Failed to register message callback for topic: " + topic);
        }

        return result;
    }

    bool Node::write_knowledge(const std::string &key, const std::string &value)
    {
        bool result = pImpl->knowledge_manager->write(key, value);
        if (result)
        {
            pImpl->logger->debug("Wrote knowledge: " + key);
        }
        else
        {
            pImpl->logger->error("Failed to write knowledge: " + key);
        }

        return result;
    }

    std::string Node::read_knowledge(const std::string &key)
    {
        std::string result = pImpl->knowledge_manager->read(key);
        if (!result.empty())
        {
            pImpl->logger->debug("Read knowledge: " + key);
        }
        else
        {
            pImpl->logger->debug("Knowledge not found: " + key);
        }

        return result;
    }

    bool Node::knowledge_exists(const std::string &key)
    {
        return pImpl->knowledge_manager->exists(key);
    }

    std::string Node::generate_uuid()
    {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(0, 15);
        static std::uniform_int_distribution<> dis2(8, 11);

        std::stringstream ss;
        ss << std::hex;
        for (int i = 0; i < 8; i++)
        {
            ss << dis(gen);
        }

        return ss.str();
    }

    std::string Node::get_timestamp()
    {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);

        std::stringstream ss;
        ss << std::put_time(std::gmtime(&time_t), "%Y-%m-%dT%H:%M:%SZ");
        return ss.str();
    }

    Logger &Node::get_logger()
    {
        return *pImpl->logger;
    }

} // namespace rpclcpp
