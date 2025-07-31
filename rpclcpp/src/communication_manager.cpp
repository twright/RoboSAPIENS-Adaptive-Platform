#include "communication_manager.hpp"
#include <hiredis/hiredis.h>
#include <iostream>
#include <thread>
#include <atomic>

namespace rpclcpp
{

    class CommunicationManager::Impl
    {
    public:
        std::string host;
        int port;
        int db;
        redisContext *context;
        redisContext *sub_context;
        std::atomic<bool> running;
        std::thread subscriber_thread;
        std::map<std::string, MessageCallback> callbacks;
        std::mutex callbacks_mutex;

        Impl(const std::string &h, int p, int d)
            : host(h), port(p), db(d), context(nullptr), sub_context(nullptr), running(false) {}

        ~Impl()
        {
            stop();
        }

        bool connect()
        {
            context = redisConnect(host.c_str(), port);
            if (context == nullptr || context->err)
            {
                if (context)
                {
                    std::cerr << "Redis connection error: " << context->errstr << std::endl;
                    redisFree(context);
                    context = nullptr;
                }
                return false;
            }

            // Select database
            redisReply *reply = (redisReply *)redisCommand(context, "SELECT %d", db);
            if (reply == nullptr || reply->type == REDIS_REPLY_ERROR)
            {
                if (reply)
                    freeReplyObject(reply);
                redisFree(context);
                context = nullptr;
                return false;
            }
            freeReplyObject(reply);

            // Create subscriber context
            sub_context = redisConnect(host.c_str(), port);
            if (sub_context == nullptr || sub_context->err)
            {
                if (sub_context)
                {
                    redisFree(sub_context);
                    sub_context = nullptr;
                }
                redisFree(context);
                context = nullptr;
                return false;
            }

            // Select database for subscriber
            reply = (redisReply *)redisCommand(sub_context, "SELECT %d", db);
            if (reply == nullptr || reply->type == REDIS_REPLY_ERROR)
            {
                if (reply)
                    freeReplyObject(reply);
                redisFree(sub_context);
                redisFree(context);
                sub_context = nullptr;
                context = nullptr;
                return false;
            }
            freeReplyObject(reply);

            return true;
        }

        void start_subscriber()
        {
            subscriber_thread = std::thread([this]()
                                            {
            while (running) {
                redisReply* reply;
                if (redisGetReply(sub_context, (void**)&reply) == REDIS_OK) {
                    if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 3) {
                        std::string topic = reply->element[1]->str;
                        std::string message = reply->element[2]->str;
                        
                        std::lock_guard<std::mutex> lock(callbacks_mutex);
                        auto it = callbacks.find(topic);
                        if (it != callbacks.end()) {
                            it->second(topic, message);
                        }
                    }
                    freeReplyObject(reply);
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            } });
        }

        void stop()
        {
            running = false;
            if (subscriber_thread.joinable())
            {
                subscriber_thread.join();
            }
            if (context)
            {
                redisFree(context);
                context = nullptr;
            }
            if (sub_context)
            {
                redisFree(sub_context);
                sub_context = nullptr;
            }
        }
    };

    CommunicationManager::CommunicationManager(const std::string &host, int port, int db)
        : pImpl(std::make_unique<Impl>(host, port, db))
    {
    }

    CommunicationManager::~CommunicationManager() = default;

    bool CommunicationManager::start()
    {
        if (!pImpl->connect())
        {
            return false;
        }

        pImpl->running = true;
        pImpl->start_subscriber();
        return true;
    }

    void CommunicationManager::stop()
    {
        pImpl->stop();
    }

    bool CommunicationManager::publish(const std::string &topic, const std::string &message)
    {
        if (!pImpl->context)
            return false;

        redisReply *reply = (redisReply *)redisCommand(pImpl->context, "PUBLISH %s %s",
                                                       topic.c_str(), message.c_str());
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR)
        {
            if (reply)
                freeReplyObject(reply);
            return false;
        }

        freeReplyObject(reply);
        return true;
    }

    bool CommunicationManager::subscribe(const std::string &topic, MessageCallback callback)
    {
        if (!pImpl->sub_context)
            return false;

        {
            std::lock_guard<std::mutex> lock(pImpl->callbacks_mutex);
            pImpl->callbacks[topic] = callback;
        }

        redisReply *reply = (redisReply *)redisCommand(pImpl->sub_context, "SUBSCRIBE %s", topic.c_str());
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR)
        {
            if (reply)
                freeReplyObject(reply);
            return false;
        }

        freeReplyObject(reply);
        return true;
    }

    bool CommunicationManager::is_connected() const
    {
        return pImpl->context != nullptr && pImpl->sub_context != nullptr;
    }

} // namespace rpclcpp
