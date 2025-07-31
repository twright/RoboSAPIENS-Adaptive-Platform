#pragma once

#include <string>
#include <memory>
#include <functional>
#include <map>
#include <thread>
#include <mutex>

namespace rpclcpp
{

    class CommunicationManager
    {
    public:
        using MessageCallback = std::function<void(const std::string &topic, const std::string &message)>;

        CommunicationManager(const std::string &host = "localhost", int port = 6379, int db = 0);
        ~CommunicationManager();

        bool start();
        void stop();

        bool publish(const std::string &topic, const std::string &message);
        bool subscribe(const std::string &topic, MessageCallback callback);

        bool is_connected() const;

    private:
        class Impl;
        std::unique_ptr<Impl> pImpl;
    };

} // namespace rpclcpp
