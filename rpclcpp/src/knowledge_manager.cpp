#include "knowledge_manager.hpp"
#include <hiredis/hiredis.h>
#include <iostream>

namespace rpclcpp
{

    class KnowledgeManager::Impl
    {
    public:
        std::string host;
        int port;
        int db;
        redisContext *context;

        Impl(const std::string &h, int p, int d)
            : host(h), port(p), db(d), context(nullptr) {}

        ~Impl()
        {
            if (context)
            {
                redisFree(context);
            }
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
            return true;
        }
    };

    KnowledgeManager::KnowledgeManager(const std::string &host, int port, int db)
        : pImpl(std::make_unique<Impl>(host, port, db))
    {
        pImpl->connect();
    }

    KnowledgeManager::~KnowledgeManager() = default;

    bool KnowledgeManager::write(const std::string &key, const std::string &value)
    {
        if (!pImpl->context)
            return false;

        redisReply *reply = (redisReply *)redisCommand(pImpl->context, "SET %s %s",
                                                       key.c_str(), value.c_str());
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR)
        {
            if (reply)
                freeReplyObject(reply);
            return false;
        }

        freeReplyObject(reply);
        return true;
    }

    std::string KnowledgeManager::read(const std::string &key)
    {
        if (!pImpl->context)
            return "";

        redisReply *reply = (redisReply *)redisCommand(pImpl->context, "GET %s", key.c_str());
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_NIL)
        {
            if (reply)
                freeReplyObject(reply);
            return "";
        }

        std::string result = reply->str;
        freeReplyObject(reply);
        return result;
    }

    bool KnowledgeManager::exists(const std::string &key)
    {
        if (!pImpl->context)
            return false;

        redisReply *reply = (redisReply *)redisCommand(pImpl->context, "EXISTS %s", key.c_str());
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR)
        {
            if (reply)
                freeReplyObject(reply);
            return false;
        }

        bool result = reply->integer > 0;
        freeReplyObject(reply);
        return result;
    }

    bool KnowledgeManager::remove(const std::string &key)
    {
        if (!pImpl->context)
            return false;

        redisReply *reply = (redisReply *)redisCommand(pImpl->context, "DEL %s", key.c_str());
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR)
        {
            if (reply)
                freeReplyObject(reply);
            return false;
        }

        bool result = reply->integer > 0;
        freeReplyObject(reply);
        return result;
    }

    bool KnowledgeManager::is_connected() const
    {
        return pImpl->context != nullptr;
    }

} // namespace rpclcpp
