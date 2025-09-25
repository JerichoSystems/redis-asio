#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "redis_log.hpp"

namespace {
using namespace redis_asio;

struct TestLogger final : Logger {
    explicit TestLogger(Level min = Level::trace) : min_(min) {}

    void log(Level lvl, std::string_view msg) noexcept override {
        entries.emplace_back(lvl, std::string(msg));
    }
    bool should_log(Level lvl) const noexcept override { return lvl >= min_; }
    void set_min(Level l) { min_ = l; }

    std::vector<std::pair<Level, std::string>> entries;
    Level min_;
};

} // namespace

TEST(LoggingBasic_On, FormatsAndWritesWhenEnabled) {
    auto lg = std::make_shared<TestLogger>(Logger::Level::trace);
    infof(lg, "hello {} {}", 42, "x");
    ASSERT_EQ(lg->entries.size(), 1u);
    EXPECT_EQ(lg->entries[0].first, Logger::Level::info);
    EXPECT_EQ(lg->entries[0].second, std::string("hello 42 x"));
}

TEST(LoggingWarn_On, WritesPlainMessage) {
    auto lg = std::make_shared<TestLogger>(Logger::Level::trace);
    warnf(lg, "warn message {}", 7);
    ASSERT_EQ(lg->entries.size(), 1u);
    EXPECT_EQ(lg->entries[0].second, std::string("warn message 7"));
}

TEST(LoggingDynamicLevel_On, UsesRuntimeLevelAndThreshold) {
    auto lg = std::make_shared<TestLogger>(Logger::Level::trace);
    logf(lg, Logger::Level::debug, "dbg {}", 1);
    logf(lg, Logger::Level::critical, "crit {}", 2);
    ASSERT_EQ(lg->entries.size(), 2u);
    EXPECT_EQ(lg->entries[0].second, std::string("dbg 1"));
    EXPECT_EQ(lg->entries[1].second, std::string("crit 2"));
}

TEST(LoggingPointerKinds_On, RawPointerAndSharedPtrWork) {
    TestLogger raw;
    TestLogger *p = &raw;
    infof(p, "raw {}", 1);
    ASSERT_EQ(raw.entries.size(), 1u);
    EXPECT_EQ(raw.entries[0].second, std::string("raw 1"));

    auto sp = std::make_shared<TestLogger>();
    infof(sp, "sp {}", 2);
    ASSERT_EQ(sp->entries.size(), 1u);
    EXPECT_EQ(sp->entries[0].second, std::string("sp 2"));
}

TEST(Macros_On, REDIS_LOGF_ForwardsDynamicLevel) {
    auto lg = std::make_shared<TestLogger>(Logger::Level::trace);
    REDIS_LOGF(lg, Logger::Level::info, "x{}", 5);
    ASSERT_EQ(lg->entries.size(), 1u);
    EXPECT_EQ(lg->entries[0].second, std::string("x5"));
}

TEST(Macros_On, DebugEvaluatedWhenEnabled) {
    auto lg = std::make_shared<TestLogger>(Logger::Level::trace);
    int side = 0;
    auto inc = [&] { ++side; return 42; };
    REDIS_DEBUG(lg, "side {}", inc());
    EXPECT_EQ(side, 1);
    ASSERT_EQ(lg->entries.size(), 1u);
    EXPECT_EQ(lg->entries[0].second, std::string("side 42"));
}

// Color check: we compile redis_logging.cpp in this target with REDIS_LOG_FORCE_COLOR
TEST(ClogLogger_On, EmitsAnsiSequencesWhenForced) {
    auto lg = make_clog_logger(Logger::Level::info, "test");
    std::ostringstream os;
    auto *old = std::clog.rdbuf(os.rdbuf());

    REDIS_INFO(lg, "color {}", 7);

    std::clog.rdbuf(old);
    std::string s = os.str();
    EXPECT_NE(s.find("["), std::string::npos) << s; // ESC[
    EXPECT_NE(s.find("[info]"), std::string::npos) << s;
}
