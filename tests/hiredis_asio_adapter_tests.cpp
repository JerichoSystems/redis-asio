#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <stdexcept>
#include <unistd.h>

#include "hiredis_asio_adapter.hpp"

namespace redis_asio {
struct HiredisAsioAdapterTestAccess {
    static bool reading(const HiredisAsioAdapter &adapter) { return adapter.reading_; }
    static bool writing(const HiredisAsioAdapter &adapter) { return adapter.writing_; }
};
} // namespace redis_asio

using redis_asio::HiredisAsioAdapter;
using redis_asio::HiredisAsioAdapterTestAccess;

namespace {
struct Pipe {
    int fds[2]{-1, -1};
    Pipe() {
        if (::pipe(fds) != 0)
            throw std::runtime_error("pipe failed");
    }
    ~Pipe() {
        if (fds[0] != -1)
            ::close(fds[0]);
        if (fds[1] != -1)
            ::close(fds[1]);
    }
};
} // namespace

TEST(HiredisAsioAdapterTests, CallbacksToggleFlags) {
    boost::asio::io_context ioc;
    Pipe p;

    redisAsyncContext ctx{};
    ctx.c.fd = p.fds[0];

    auto adapter = std::make_shared<HiredisAsioAdapter>(ioc.get_executor(), &ctx);
    adapter->start();

    EXPECT_TRUE(HiredisAsioAdapterTestAccess::reading(*adapter));
    EXPECT_TRUE(HiredisAsioAdapterTestAccess::writing(*adapter));
    EXPECT_EQ(ctx.ev.data, adapter.get());
    ASSERT_NE(ctx.ev.addRead, nullptr);
    ASSERT_NE(ctx.ev.addWrite, nullptr);
    ASSERT_NE(ctx.ev.cleanup, nullptr);
    ctx.ev.addWrite(ctx.ev.data);
    ctx.ev.delRead(ctx.ev.data);
    ctx.ev.delWrite(ctx.ev.data);
    EXPECT_FALSE(HiredisAsioAdapterTestAccess::reading(*adapter));
    EXPECT_FALSE(HiredisAsioAdapterTestAccess::writing(*adapter));
}

TEST(HiredisAsioAdapterTests, ReaddAfterDisableRestartsFlags) {
    boost::asio::io_context ioc;
    Pipe p;

    redisAsyncContext ctx{};
    ctx.c.fd = p.fds[0];

    auto adapter = std::make_shared<HiredisAsioAdapter>(ioc.get_executor(), &ctx);
    adapter->start();
    ctx.ev.delRead(ctx.ev.data);
    ctx.ev.delWrite(ctx.ev.data);

    ctx.ev.addRead(ctx.ev.data);
    ctx.ev.addWrite(ctx.ev.data);

    EXPECT_TRUE(HiredisAsioAdapterTestAccess::reading(*adapter));
    EXPECT_TRUE(HiredisAsioAdapterTestAccess::writing(*adapter));
}

TEST(HiredisAsioAdapterTests, StopClearsCallbacksAndFlags) {
    boost::asio::io_context ioc;
    Pipe p;

    redisAsyncContext ctx{};
    ctx.c.fd = p.fds[0];

    auto adapter = std::make_shared<HiredisAsioAdapter>(ioc.get_executor(), &ctx);
    adapter->start();
    adapter->stop();

    EXPECT_FALSE(HiredisAsioAdapterTestAccess::reading(*adapter));
    EXPECT_FALSE(HiredisAsioAdapterTestAccess::writing(*adapter));
    EXPECT_EQ(ctx.ev.addRead, nullptr);
    EXPECT_EQ(ctx.ev.delRead, nullptr);
    EXPECT_EQ(ctx.ev.addWrite, nullptr);
    EXPECT_EQ(ctx.ev.delWrite, nullptr);
    EXPECT_EQ(ctx.ev.cleanup, nullptr);
    EXPECT_EQ(ctx.ev.data, nullptr);
}

TEST(HiredisAsioAdapterTests, CleanupStopsAdapter) {
    boost::asio::io_context ioc;
    Pipe p;

    redisAsyncContext ctx{};
    ctx.c.fd = p.fds[0];

    auto adapter = std::make_shared<HiredisAsioAdapter>(ioc.get_executor(), &ctx);
    adapter->start();

    ASSERT_NE(ctx.ev.cleanup, nullptr);
    ctx.ev.cleanup(ctx.ev.data);

    EXPECT_FALSE(HiredisAsioAdapterTestAccess::reading(*adapter));
    EXPECT_FALSE(HiredisAsioAdapterTestAccess::writing(*adapter));
    EXPECT_EQ(ctx.ev.addRead, nullptr);
    EXPECT_EQ(ctx.ev.delRead, nullptr);
    EXPECT_EQ(ctx.ev.addWrite, nullptr);
    EXPECT_EQ(ctx.ev.delWrite, nullptr);
    EXPECT_EQ(ctx.ev.cleanup, nullptr);
    EXPECT_EQ(ctx.ev.data, nullptr);
}
