#pragma once

#include <string>
#include <memory>
#include <vector>
#include <map>
#include <any>

namespace rpclcpp
{

    class KnowledgeManager
    {
    public:
        KnowledgeManager(const std::string &host = "localhost", int port = 6379, int db = 2);
        ~KnowledgeManager();

        bool write(const std::string &key, const std::string &value);
        std::string read(const std::string &key);
        bool exists(const std::string &key);
        bool remove(const std::string &key);

        // Template methods for object serialization
        template <typename T>
        bool write_object(const std::string &key, const T &obj);

        template <typename T>
        T read_object(const std::string &key);

        bool is_connected() const;

    private:
        class Impl;
        std::unique_ptr<Impl> pImpl;
    };

} // namespace rpclcpp
