#include <gtest/gtest.h>
#include <string>
#include <memory>
#include "redis_log.hpp"

namespace {
using namespace redis_asio;
struct TestLoggerRT final : Logger {
    void log(Level lvl, std::string_view msg) noexcept override { entries.emplace_back(lvl, std::string(msg)); }
    bool should_log(Level) const noexcept override { return true; }
    std::vector<std::pair<Level, std::string>> entries;
};
}

TEST(RuntimeFmt_On, MacrosBuildFormatArgsAndLog) {
    auto lg = std::make_shared<TestLoggerRT>();
    std::string fmt = std::string("hello {} {} ") + "{}"; // runtime-built format
    REDIS_INFO_RT(lg, fmt, 1, "x", 3);
    ASSERT_EQ(lg->entries.size(), 1u);
    EXPECT_EQ(lg->entries[0].second, std::string("hello 1 x 3"));
}

TEST(RuntimeFmt_On, DirectAPI_logf_rt_Works) {
    auto lg = std::make_shared<TestLoggerRT>();
    std::string fmt = "warn {}";
    // Since logf_rt takes an explicit location, it should prefix when level >= WARN
    const auto loc = std::source_location::current();
    ::redis_asio::logf_rt(lg, Logger::Level::warn, fmt, loc, 7);
    ASSERT_EQ(lg->entries.size(), 1u);
    EXPECT_EQ(lg->entries[0].first, Logger::Level::warn);
    const auto& s = lg->entries[0].second;
    // Expect file:line prefix and the message suffix
    EXPECT_NE(s.find(".cpp:"), std::string::npos) << s;
    EXPECT_NE(s.find(" warn 7"), std::string::npos) << s;
}

TEST(RuntimeFmt_On, LocationPrefixAddedForWarnAndAbove) {
    auto lg = std::make_shared<TestLoggerRT>();
    // WARN should include file:line prefix by default threshold
    REDIS_WARN_RT(lg, std::string("w{}"), 1);
    ASSERT_EQ(lg->entries.size(), 1u);
    auto& s = lg->entries[0].second;
    // Look for the test filename in the prefix (basename match)
    EXPECT_NE(s.find("redis_log_rt_tests.cpp"), std::string::npos) << s;
    EXPECT_NE(s.find(":"), std::string::npos) << s; // has :line
    EXPECT_NE(s.find(" w1"), std::string::npos) << s; // message suffix present
}

TEST(RuntimeFmt_On, LocationNotAddedBelowThreshold) {
    auto lg = std::make_shared<TestLoggerRT>();
    REDIS_INFO_RT(lg, std::string("i{}"), 2);
    ASSERT_EQ(lg->entries.size(), 1u);
    auto& s = lg->entries[0].second;
    // Below WARN (default), should not contain filename
    EXPECT_EQ(s.find("redis_log_rt_tests.cpp"), std::string::npos) << s;
    EXPECT_EQ(s, std::string("i2"));
}