#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

#include "redis_async.hpp"
#include "redis_log.hpp"
#include "redis_value.hpp"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <tuple>
#include <variant>

using namespace std::chrono_literals;
namespace asio = boost::asio;

// --- Helpers ---------------------------------------------------------------

static redis_asio::ConnectOptions opts_from_env() {
    redis_asio::ConnectOptions o;
    if (const char *h = std::getenv("REDIS_HOST"))
        o.host = h;
    if (const char *p = std::getenv("REDIS_PORT"))
        o.port = static_cast<uint16_t>(std::atoi(p));
    if (const char *u = std::getenv("REDIS_USER"))
        o.username = std::string(u);
    if (const char *pw = std::getenv("REDIS_PASS"))
        o.password = std::string(pw);
    if (const char *nm = std::getenv("REDIS_NAME"))
        o.client_name = std::string(nm);
    if (const char *t = std::getenv("REDIS_TLS"))
        o.tls.use_tls = (*t == '1');
    if (const char *vf = std::getenv("REDIS_TLS_VERIFY"))
        o.tls.verify_peer = (*vf != '0');
    if (const char *ca = std::getenv("REDIS_CAFILE"))
        o.tls.ca_file = ca;
    if (const char *cp = std::getenv("REDIS_CAPATH"))
        o.tls.ca_path = cp;
    if (const char *cf = std::getenv("REDIS_CERT"))
        o.tls.cert_file = cf;
    if (const char *kf = std::getenv("REDIS_KEY"))
        o.tls.key_file = kf;
    return o;
}

static redis_asio::ConnectOptions bogus_opts() {
    redis_asio::ConnectOptions o;
    o.host = "256.256.256.256"; // invalid
    o.port = 9999;
    return o;
}

using boost::asio::experimental::awaitable_operators::operator||;

// --- Unit tests (no server needed) ----------------------------------------
TEST(Unit, ConnectionObjectConstructs) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_tests.unit.construct");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    EXPECT_TRUE(c);
    EXPECT_FALSE(c->is_connected());
    (void)c->hello_summary();
}

TEST(Unit, OperationsFailWhenNotConnectedBeforeConnect) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_tests.unit.noconnect");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;

        // Command
        std::error_code ec; redis_asio::RedisValue rv;
        std::tie(ec, rv) = co_await c->async_command({"PING"}, as_tuple(asio::use_awaitable));
        EXPECT_TRUE(ec); // not connected

        // Subscribe
        std::error_code ecs = co_await c->async_subscribe({"x"}, asio::use_awaitable);
        EXPECT_TRUE(ecs);

        // PSubscribe
        std::error_code ecps = co_await c->async_psubscribe({"x.*"}, asio::use_awaitable);
        EXPECT_TRUE(ecps);

        // Unsubscribe
        std::error_code ecu = co_await c->async_unsubscribe({"x"}, asio::use_awaitable);
        EXPECT_TRUE(ecu);

        // PUnsubscribe
        std::error_code ecpu = co_await c->async_punsubscribe({"x.*"}, asio::use_awaitable);
        EXPECT_TRUE(ecpu);

        // Wait disconnected is immediate success when not connected
        std::error_code ewd = co_await c->async_wait_disconnected(asio::use_awaitable);
        EXPECT_FALSE(ewd);
        co_return; }, asio::detached);

    ioc.run();
}

// --- Integration tests (require running Redis) ----------------------------
TEST(Integration, ConnectDoubleConnectWaiters) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.conn");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto opts = opts_from_env();

        // Start a waiter before connecting; it should resolve once connected.
        bool waiter_done = false;
        asio::co_spawn(co_await asio::this_coro::executor, [c, &waiter_done]() -> asio::awaitable<void> {
            std::error_code ecw = co_await c->async_wait_connected(asio::use_awaitable);
            EXPECT_FALSE(ecw);
            waiter_done = true;
            co_return;
        }, asio::detached);

        auto [ec1, already1] = co_await c->async_connect(opts, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec1);
        EXPECT_FALSE(already1);

        // Force a context switch to allow the waiter to run
        asio::steady_timer gate(co_await asio::this_coro::executor);
        gate.expires_after(0ms);
        co_await gate.async_wait(asio::use_awaitable);

        EXPECT_TRUE(waiter_done); // the pre-registered waiter fired

        // connect again -> should complete immediately with already=true
        auto [ec2, already2] = co_await c->async_connect(opts, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec2);
        EXPECT_TRUE(already2);

        // waiters on connected/disconnected
        std::error_code ecw = co_await c->async_wait_connected(asio::use_awaitable);
        EXPECT_FALSE(ecw);

        c->stop();
        std::error_code ecd = co_await c->async_wait_disconnected(asio::use_awaitable);
        EXPECT_FALSE(ecd);
        co_return; }, asio::detached);

    ioc.run();
}

TEST(Integration, HelloBasicCommandsErrorReply) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.cmd");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto opts = opts_from_env();
        auto [ec, already] = co_await c->async_connect(opts, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec); (void)already;

        // Hello summary should be non-empty
        EXPECT_FALSE(c->hello_summary().empty());

        // PING
        std::error_code ecc; redis_asio::RedisValue rv;
        std::tie(ecc, rv) = co_await c->async_command({"PING"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecc);

        // SET / GET roundtrip
        std::tie(ecc, rv) = co_await c->async_command({"SET","test:key","v1"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecc);
        std::tie(ecc, rv) = co_await c->async_command({"GET","test:key"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecc);
        auto s = redis_asio::string_like(rv);
        EXPECT_TRUE(s.has_value());
        EXPECT_TRUE(*s == "v1");

        // Unknown command -> Redis error reply (ec should be OK, value type = Error)
        std::tie(ecc, rv) = co_await c->async_command({"DOESNOTEXIST"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecc);
        EXPECT_TRUE(rv.type == redis_asio::RedisValue::Type::Error);

        c->stop();
        co_return; }, asio::detached);

    ioc.run();
}

TEST(Integration, PsubscribePublishReceivePattern) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.psub");
    auto c_sub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    auto c_pub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c_sub, c_pub, log]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto opts = opts_from_env();

        auto [ec1, a1] = co_await c_sub->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec1); (void)a1;
        auto [ec2, a2] = co_await c_pub->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec2); (void)a2;

        std::error_code ecs = co_await c_sub->async_psubscribe({"news.*"}, asio::use_awaitable);
        EXPECT_FALSE(ecs);

        std::error_code ecp; redis_asio::RedisValue rv;
        std::tie(ecp, rv) = co_await c_pub->async_command({"PUBLISH","news.123","hello"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecp);

        asio::steady_timer t(co_await asio::this_coro::executor);
        t.expires_after(2s);

        auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c_sub->async_receive_publish(as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(v.index() == 1);
        auto [rxec, msg] = std::get<1>(v);
        EXPECT_FALSE(rxec);
        EXPECT_TRUE(msg.channel == "news.123");
        EXPECT_TRUE(msg.payload == "hello");
        // Now punsubscribe and ensure we don't get another
        std::error_code ecpu2 = co_await c_sub->async_punsubscribe({"newsbogus.*"}, asio::use_awaitable);
        EXPECT_FALSE(ecpu2);

        // Now punsubscribe and ensure we don't get another
        std::error_code ecpu = co_await c_sub->async_punsubscribe({"news.*"}, asio::use_awaitable);
        EXPECT_FALSE(ecpu);

        std::tie(ecp, rv) = co_await c_pub->async_command({"PUBLISH","news.456","after"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecp);
        asio::steady_timer t2(co_await asio::this_coro::executor); t2.expires_after(300ms);
        auto v2 = co_await (t2.async_wait(as_tuple(asio::use_awaitable)) || c_sub->async_receive_publish(as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(v2.index() == 0); // timer fired first

        c_sub->stop();
        c_pub->stop();
        co_return; }, asio::detached);

    ioc.run();
}

TEST(Integration, SubscribeRefcountAndUnsubscribeBehavior) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.subref");
    auto c_sub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    auto c_pub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c_sub, c_pub]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto opts = opts_from_env();
        auto [ec1, a1] = co_await c_sub->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec1); (void)a1;
        auto [ec2, a2] = co_await c_pub->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec2); (void)a2;

        // Subscribe twice
        EXPECT_FALSE(co_await c_sub->async_subscribe({"chan.R"}, asio::use_awaitable));
        EXPECT_FALSE(co_await c_sub->async_subscribe({"chan.R"}, asio::use_awaitable));

        // Publish and expect a message
        std::error_code ecp; redis_asio::RedisValue rv;
        std::tie(ecp, rv) = co_await c_pub->async_command({"PUBLISH","chan.R","one"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecp);
        {
          asio::steady_timer t(co_await asio::this_coro::executor); t.expires_after(2s);
          auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c_sub->async_receive_publish(as_tuple(asio::use_awaitable)));
          EXPECT_TRUE(v.index() == 1);
        }

        // Unsubscribe once -> still subscribed
        EXPECT_FALSE(co_await c_sub->async_unsubscribe({"chan.R"}, asio::use_awaitable));
        std::tie(ecp, rv) = co_await c_pub->async_command({"PUBLISH","chan.R","two"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecp);
        {
          asio::steady_timer t(co_await asio::this_coro::executor); t.expires_after(2s);
          auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c_sub->async_receive_publish(as_tuple(asio::use_awaitable)));
          EXPECT_TRUE(v.index() == 1);
        }

        // Unsubscribe second time -> no more messages
        EXPECT_FALSE(co_await c_sub->async_unsubscribe({"chan.R"}, asio::use_awaitable));
        std::tie(ecp, rv) = co_await c_pub->async_command({"PUBLISH","chan.R","three"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ecp);
        {
          asio::steady_timer t(co_await asio::this_coro::executor); t.expires_after(300ms);
          auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c_sub->async_receive_publish(as_tuple(asio::use_awaitable)));
          EXPECT_TRUE(v.index() == 0);
        }

        c_sub->stop();
        c_pub->stop();
        co_return; }, asio::detached);

    ioc.run();
}

TEST(Integration, ReceiveCompletesWithErrorAfterStop) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.stopclose");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto opts = opts_from_env();
        auto [ec, a] = co_await c->async_connect(opts, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec); (void)a;

        // Close then await a receive -> should return an error immediately.
        c->stop();
        auto [rxec, msg] = co_await c->async_receive_publish(as_tuple(asio::use_awaitable));
        EXPECT_TRUE(rxec); (void)msg;
        co_return; }, asio::detached);

    ioc.run();
}

TEST(Integration, ReconnectRestoresPsubscriptionsViaClientKill) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.resub");
    auto c_sub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    auto c_ctl = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log); // controller/publisher

    asio::co_spawn(ioc, [c_sub, c_ctl]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto opts = opts_from_env();

        auto [ec1, a1] = co_await c_sub->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec1); (void)a1;
        auto [ec2, a2] = co_await c_ctl->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec2); (void)a2;

        // Subscribe
        EXPECT_FALSE(co_await c_sub->async_psubscribe({"auto.*"}, asio::use_awaitable));

        // Get subscriber connection id
        std::error_code ec; redis_asio::RedisValue rv;
        std::tie(ec, rv) = co_await c_sub->async_command({"CLIENT","ID"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        auto idp = std::get_if<long long>(&rv.payload);
        EXPECT_TRUE(idp != nullptr);
        auto idstr = std::to_string(*idp);

        // Kill the subscriber connection from the controller connection
        {
            // Work around GCC coroutine ICE by avoiding mixed init-list with a std::string variable
            std::vector<std::string> kill = {"CLIENT", "KILL", "ID", idstr};
            std::tie(ec, rv) = co_await c_ctl->async_command(kill, as_tuple(asio::use_awaitable));
            EXPECT_FALSE(ec);
        }

        // Wait for disconnect then reconnect
        EXPECT_FALSE(co_await c_sub->async_wait_disconnected(asio::use_awaitable));
        // Give the client time to reconnect; if your impl exposes async_wait_connected, use it:
        EXPECT_FALSE(co_await c_sub->async_wait_connected(asio::use_awaitable));

        // Small delay to ensure the server has processed the re-subscribe after reconnect
        asio::steady_timer delay(co_await asio::this_coro::executor);
        delay.expires_after(100ms);
        co_await delay.async_wait(as_tuple(asio::use_awaitable));

        // After reconnect, publish and expect reception due to restored psubscription
        std::tie(ec, rv) = co_await c_ctl->async_command({"PUBLISH","auto.42","after-reconnect"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);

        asio::steady_timer t(co_await asio::this_coro::executor);
        t.expires_after(5s);
        auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c_sub->async_receive_publish(as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(v.index() == 1);
        auto [rxec, msg] = std::get<1>(v);
        EXPECT_FALSE(rxec);
        EXPECT_TRUE(msg.channel == "auto.42");
        EXPECT_TRUE(msg.payload == "after-reconnect");

        c_sub->stop();
        c_ctl->stop();
        co_return; }, asio::detached);

    ioc.run();
}

// --- Completion token tests (no server needed) ----------------------------------------
TEST(CompletionTokens, AsyncCommand_UseFuture) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "ct.future");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    using boost::asio::as_tuple;
    std::future<std::tuple<std::error_code, redis_asio::RedisValue>> fut =
        c->async_command({"PING"}, as_tuple(asio::use_future));
    std::thread t1([&] { ioc.poll(); });
    // Not connected -> should error
    auto [ec, v] = fut.get();
    EXPECT_TRUE(ec);
    t1.join();
}
static asio::awaitable<void>
echo_once(std::shared_ptr<redis_asio::RedisAsyncConnection> c,
          std::shared_ptr<std::vector<int>> seen,
          std::shared_ptr<std::atomic<bool>> in_handler,
          int i) {
    using boost::asio::as_tuple;

    std::vector<std::string> cmd;
    cmd.reserve(2);
    cmd.emplace_back("ECHO");
    cmd.emplace_back(std::to_string(i)); // avoid init-list + to_string gcc quirks

    auto [ec, v] = co_await c->async_command(cmd, as_tuple(asio::use_awaitable));
    EXPECT_FALSE(ec);

    bool expected = false;
    EXPECT_TRUE(in_handler->compare_exchange_strong(expected, true)); // proves no overlap
    seen->push_back(i);
    in_handler->store(false);
    co_return;
}

TEST(Concurrency, StrandSerializesHandlersEvenWithTwoThreads) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc(1);
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "strand.serial");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    std::shared_ptr<std::atomic<bool>> in_handler(new std::atomic<bool>(false));
    std::shared_ptr<std::vector<int>> seen(new std::vector<int>);
    seen->reserve(200);

    asio::co_spawn(ioc, [c, seen, in_handler]() -> asio::awaitable<void> {
    using boost::asio::as_tuple;
    auto [ec, already] = co_await c->async_connect(opts_from_env(), as_tuple(asio::use_awaitable));
    EXPECT_FALSE(ec);

    std::vector<asio::awaitable<void>> ops;
    for (int i = 0; i < 200; ++i) {
        ops.emplace_back(echo_once(c, seen, in_handler, i));
    }
    for (auto &op : ops) co_await std::move(op);
    c->stop();
    co_return; }, asio::detached);

    std::thread t1([&] { ioc.run(); });
    std::thread t2([&] { ioc.run(); });
    t1.join();
    t2.join();

    ASSERT_EQ(seen->size(), 200u);
    for (int i = 0; i < 200; ++i)
        EXPECT_EQ((*seen)[i], i);
}

TEST(Cancel, WaitConnectedCanceledByStop) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "cancel.waitconn");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;
        asio::post(ex, [c]{ c->stop(); });
        auto [ec] = co_await c->async_wait_connected(as_tuple(asio::use_awaitable));
        EXPECT_EQ(ec, make_error(redis_asio::error_category::errc::stopped));
    co_return; }, asio::detached);
    ioc.run();
}

TEST(Cancel, WaitConnectedCanceledByCancellation) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "cancel.waitconcancel");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;
        asio::steady_timer t(ex);
        t.expires_after(10ms);
        auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c->async_wait_connected(as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(v.index() == 0);
    co_return; }, asio::detached);
    ioc.run();
}

TEST(Cancel, WaitConnectCanceledByTimeout) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "cancel.waitconcanceltime");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;

        asio::steady_timer t(ex);
        t.expires_after(10ms);
        auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c->async_connect(bogus_opts(), as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(v.index() == 0);
        c->stop();
    co_return; }, asio::detached);
    ioc.run();
}

TEST(Cancel, WaitDisconnectedCanceledByCancellation) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "cancel.waitdisconcancel");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;

        auto [ec, already] = co_await c->async_connect(opts_from_env(), as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        EXPECT_FALSE(already);
        asio::steady_timer t(ex);
        t.expires_after(10ms);
        auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c->async_wait_disconnected(as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(v.index() == 0);
        c->stop();
    co_return; }, asio::detached);
    ioc.run();
}

TEST(Cancel, WaitPublishResponseCanceledByCancellation) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "cancel.waitreceivecancel");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;

        auto [ec, already] = co_await c->async_connect(opts_from_env(), as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        EXPECT_FALSE(already);
        asio::steady_timer t(ex);
        t.expires_after(10ms);
        auto v = co_await (t.async_wait(as_tuple(asio::use_awaitable)) || c->async_receive_publish(as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(v.index() == 0);
        c->stop();
    co_return; }, asio::detached);
    ioc.run();
}

TEST(Command, CommandWithArgs) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "command.args");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        EXPECT_FALSE(std::get<0>(co_await c->async_connect(opts_from_env(), as_tuple(asio::use_awaitable))));

        std::error_code ec; redis_asio::RedisValue rv;
        std::tie(ec, rv) = co_await c->async_command({"ECHO", "Hello, World!"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        auto s = redis_asio::string_like(rv);
        EXPECT_TRUE(s.has_value());
        EXPECT_EQ(*s, "Hello, World!");

        c->stop();
        co_return; }, asio::detached);
    ioc.run();
}

static asio::awaitable<void>
pipeline_echo_once(int i, std::shared_ptr<std::vector<int>> out, std::shared_ptr<redis_asio::RedisAsyncConnection> c) {
    using boost::asio::as_tuple;
    std::vector<std::string> cmd;
    cmd.reserve(2);
    cmd.emplace_back("ECHO");
    cmd.emplace_back(std::to_string(i));
    auto [ec, v] = co_await c->async_command(std::move(cmd), as_tuple(asio::use_awaitable));
    EXPECT_FALSE(ec);
    auto s = redis_asio::string_like(v);
    EXPECT_TRUE(s);
    out->emplace_back(std::stoi(*s));
    co_return;
}

TEST(Command, PipeliningOrder) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "pipeline.order");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        EXPECT_FALSE(std::get<0>(co_await c->async_connect(opts_from_env(), as_tuple(asio::use_awaitable))));

        constexpr int N = 128;
        std::vector<asio::awaitable<void>> ops;
        std::shared_ptr<std::vector<int>> out = std::make_shared<std::vector<int>>(); out->reserve(N);
        for (int i = 0; i < N; ++i)
            ops.emplace_back(pipeline_echo_once(i, out, c));
        for (auto& a: ops) co_await std::move(a);
        for (int i = 0; i < N; ++i) EXPECT_EQ((*out)[i], i);
        c->stop();
    co_return; }, asio::detached);
    ioc.run();
}

TEST(PubSub, BackpressureDropsWhenQueueFull) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "ps.backpressure");
    // Subscriber with tiny channel capacity (8)
    auto c_sub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log, 8);
    // Separate publisher
    auto c_pub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c_sub = std::move(c_sub), c_pub = std::move(c_pub)]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        using boost::asio::experimental::awaitable_operators::operator||;

        auto opts = opts_from_env();

        // Connect both ends
        EXPECT_FALSE(std::get<0>(co_await c_sub->async_connect(opts, as_tuple(asio::use_awaitable))));
        EXPECT_FALSE(std::get<0>(co_await c_pub->async_connect(opts, as_tuple(asio::use_awaitable))));

        // Subscribe but intentionally DO NOT receive yet (to build backlog).
       EXPECT_FALSE(co_await c_sub->async_subscribe({"q"}, asio::use_awaitable));

       // Blast 64 publishes quickly; each returns the subscriber count.
        for (int i = 0; i < 64; ++i) {
            std::error_code ec; redis_asio::RedisValue rv;
            auto payload = std::to_string(i);                    // avoid braced-init in co_await
            std::vector<std::string> cmd = {"PUBLISH", "q", payload};
            std::tie(ec, rv) = co_await c_pub->async_command(cmd, as_tuple(asio::use_awaitable));
            EXPECT_FALSE(ec);
        }
        // Give the push handler time to enqueue and overflow the 8-slot channel.
        asio::steady_timer settle(co_await asio::this_coro::executor);
        settle.expires_after(std::chrono::milliseconds(150));
        co_await settle.async_wait(as_tuple(asio::use_awaitable));

        // Now drain with short timeouts until idle; count how many got through.
        int received = 0;
        for (;;) {
            asio::steady_timer t(co_await asio::this_coro::executor);
            t.expires_after(std::chrono::milliseconds(60));
           auto alt = co_await (t.async_wait(as_tuple(asio::use_awaitable))
                                 || c_sub->async_receive_publish(as_tuple(asio::use_awaitable)));
            if (alt.index() == 0) break; // idle timeout
            auto [rxec, msg] = std::get<1>(alt);
            EXPECT_FALSE(rxec);
            if (!rxec && msg.channel == "q") ++received;
        }

       // With capacity=8, we expect to have dropped most of the 64; allow <= 8 and > 0.
        EXPECT_LE(received, 8);
        EXPECT_GT(received, 0);

        c_sub->stop();
        c_pub->stop();
       co_return; }, asio::detached);
    ioc.run();
}