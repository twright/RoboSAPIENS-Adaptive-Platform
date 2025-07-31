#include "logger.hpp"
#include <iostream>
#include <mutex>

namespace rpclcpp
{

    Logger::Logger(const std::string &name, LogLevel level)
        : name_(name), level_(level)
    {
    }

    Logger::~Logger() = default;

    void Logger::debug(const std::string &message)
    {
        log(LogLevel::DEBUG, message);
    }

    void Logger::info(const std::string &message)
    {
        log(LogLevel::INFO, message);
    }

    void Logger::warning(const std::string &message)
    {
        log(LogLevel::WARNING, message);
    }

    void Logger::error(const std::string &message)
    {
        log(LogLevel::ERROR, message);
    }

    void Logger::critical(const std::string &message)
    {
        log(LogLevel::CRITICAL, message);
    }

    void Logger::set_level(LogLevel level)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        level_ = level;
    }

    LogLevel Logger::get_level() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return level_;
    }

    void Logger::log(LogLevel level, const std::string &message)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (level < level_)
        {
            return;
        }

        std::string timestamp = get_timestamp();
        std::string level_str = level_to_string(level);

        std::cout << timestamp << " - " << name_ << " - " << level_str << " - " << message << std::endl;
    }

    std::string Logger::get_timestamp()
    {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);

        std::stringstream ss;
        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
        return ss.str();
    }

    std::string Logger::level_to_string(LogLevel level)
    {
        switch (level)
        {
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::WARNING:
            return "WARNING";
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::CRITICAL:
            return "CRITICAL";
        default:
            return "UNKNOWN";
        }
    }

} // namespace rpclcpp
