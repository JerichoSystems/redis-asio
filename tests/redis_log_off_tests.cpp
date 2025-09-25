#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "redis_log.hpp"

namespace {
using namespace redis_asio;

struct TestLogger2 final : Logger {
    explicit TestLogger2(Level min = Level::trace) : min_(min) {}
    void log(Level lvl, std::string_view msg) noexcept override {
        entries.emplace_back(lvl, std::string(msg));
    }
    bool should_log(Level lvl) const noexcept override { return lvl >= min_; }
    std::vector<std::pair<Level, std::string>> entries;
    Level min_;
};

} // namespace

TEST(LoggingBasic_Off, InfoStillWorks) {
    auto lg = std::make_shared<TestLogger2>(Logger::Level::trace);
    infof(lg, "hello {}", 9);
    ASSERT_EQ(lg->entries.size(), 1u);
    EXPECT_EQ(lg->entries[0].second, std::string("hello 9"));
}

TEST(Macros_Off, DebugNoArgEvaluationWhenCompiledOut) {
    auto lg = std::make_shared<TestLogger2>(Logger::Level::trace);
    int side = 0;
    auto inc = [&] { ++side; return 42; };
    REDIS_DEBUG(lg, "side {}", inc());
    EXPECT_EQ(side, 0); // arguments not evaluated
    EXPECT_TRUE(lg->entries.empty());
}
