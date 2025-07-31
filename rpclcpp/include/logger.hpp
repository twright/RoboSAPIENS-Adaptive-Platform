#pragma once

#include <string>
#include <memory>
#include <sstream>
#include <chrono>
#include <iomanip>

namespace rpclcpp
{

    enum class LogLevel
    {
        DEBUG = 0,
        INFO = 1,
        WARNING = 2,
        ERROR = 3,
        CRITICAL = 4
    };

    class Logger
    {
    public:
        Logger(const std::string &name, LogLevel level = LogLevel::INFO);
        ~Logger();

        void debug(const std::string &message);
        void info(const std::string &message);
        void warning(const std::string &message);
        void error(const std::string &message);
        void critical(const std::string &message);

        void set_level(LogLevel level);
        LogLevel get_level() const;

    private:
        void log(LogLevel level, const std::string &message);
        std::string get_timestamp();
        std::string level_to_string(LogLevel level);

        std::string name_;
        LogLevel level_;
        mutable std::mutex mutex_;
    };

} // namespace rpclcpp
