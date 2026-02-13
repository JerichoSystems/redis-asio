#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

#include "redis_async.hpp"
#include "redis_log.hpp"
#include "redis_value.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
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

namespace redis_asio {
struct RedisAsyncConnectionTestAccess {
    using Connection = RedisAsyncConnection;

    static Connection::UnsubBaton *emplace_unsub(Connection &conn,
                                                 std::string subject,
                                                 bool pattern,
                                                 std::weak_ptr<Connection> owner = {}) {
        auto baton = std::make_unique<Connection::UnsubBaton>();
        baton->w = std::move(owner);
        baton->subject = std::move(subject);
        baton->is_pattern = pattern;
        auto *ptr = baton.get();
        if (pattern)
            conn.pch_unsub_batons_[ptr->subject] = std::move(baton);
        else
            conn.ch_unsub_batons_[ptr->subject] = std::move(baton);
        return ptr;
    }

    static Connection::SubBaton *emplace_sub(Connection &conn,
                                             std::string subject,
                                             bool pattern,
                                             std::weak_ptr<Connection> owner = {}) {
        auto baton = std::make_unique<Connection::SubBaton>();
        baton->w = std::move(owner);
        baton->subject = std::move(subject);
        baton->is_pattern = pattern;
        auto *ptr = baton.get();
        if (pattern)
            conn.pch_batons_[ptr->subject] = std::move(baton);
        else
            conn.ch_batons_[ptr->subject] = std::move(baton);
        return ptr;
    }

    static std::size_t unsub_count(const Connection &conn, bool pattern) {
        return pattern ? conn.pch_unsub_batons_.size() : conn.ch_unsub_batons_.size();
    }

    static std::size_t sub_count(const Connection &conn, bool pattern) {
        return pattern ? conn.pch_batons_.size() : conn.ch_batons_.size();
    }

    static void call_unsub(redisAsyncContext *ctx, Connection::UnsubBaton *baton, redisReply *reply = nullptr) {
        Connection::handle_unsub_reply(ctx, reply, baton);
    }

    static void call_sub(redisAsyncContext *ctx, Connection::SubBaton *baton, redisReply *reply = nullptr) {
        Connection::handle_sub_reply(ctx, reply, baton);
    }

    static void set_state(Connection &conn, Connection::ConnectionState state) {
        conn.state_ = state;
        conn.connected_.store(state == Connection::ConnectionState::ready, std::memory_order_relaxed);
    }

    static Connection::ConnectionState state(const Connection &conn) {
        return conn.state_;
    }

    static void set_ctx(Connection &conn, redisAsyncContext *ctx) {
        conn.ctx_ = ctx;
    }

    static redisAsyncContext *ctx(const Connection &conn) {
        return conn.ctx_;
    }

    static std::size_t pre_ready_queue_size(const Connection &conn) {
        return conn.pre_ready_queue_.size();
    }

    static void set_opts(Connection &conn, ConnectOptions opts) {
        conn.opts_ = std::move(opts);
    }

    static void schedule_reconnect(Connection &conn) {
        conn.schedule_reconnect();
    }

    static void handshake_result(Connection &conn, std::error_code ec, std::string summary) {
        conn.handle_handshake_result(ec, std::move(summary));
    }

    static void on_disconnected(Connection &conn, int status) {
        conn.on_disconnected(status);
    }

    static void set_connect_inflight(Connection &conn, bool value) {
        conn.connect_inflight_ = value;
    }

    static void set_async_command_argv_fn(Connection &conn, Connection::RedisAsyncCommandArgvFn fn) {
        conn.async_command_argv_fn_ = fn;
    }

    static void set_async_connect_fn(Connection &conn, Connection::RedisAsyncConnectFn fn) {
        conn.async_connect_fn_ = fn;
    }

    static void call_handle_connect(redisAsyncContext *ctx, int status) {
        Connection::handle_connect(ctx, status);
    }

    static void call_handle_disconnect(redisAsyncContext *ctx, int status) {
        Connection::handle_disconnect(ctx, status);
    }
};
} // namespace redis_asio

using redis_asio::RedisAsyncConnectionTestAccess;

namespace {
struct FakeCommandEnv {
    std::vector<std::string> submitted;
    bool invoke_with_null_reply{false};
};

FakeCommandEnv *g_fake_command_env = nullptr;
std::atomic<int> g_fake_connect_calls{0};

int fake_command_argv(redisAsyncContext *ctx, redisCallbackFn *fn, void *priv, int argc, const char **argv, const size_t *argvlen) {
    if (!g_fake_command_env || argc <= 0 || !argv || !fn)
        return REDIS_ERR;

    const size_t len = argvlen ? argvlen[0] : std::strlen(argv[0]);
    g_fake_command_env->submitted.emplace_back(argv[0], len);

    if (g_fake_command_env->invoke_with_null_reply) {
        fn(ctx, nullptr, priv);
        return REDIS_OK;
    }

    redisReply reply{};
    reply.type = REDIS_REPLY_STATUS;
    const char *ok = "OK";
    reply.str = const_cast<char *>(ok);
    reply.len = std::strlen(ok);
    fn(ctx, &reply, priv);
    return REDIS_OK;
}

int fake_command_argv_submit_fail(redisAsyncContext *, redisCallbackFn *, void *, int, const char **, const size_t *) {
    return REDIS_ERR;
}

redisAsyncContext *fake_async_connect_counting(const char *, int) {
    ++g_fake_connect_calls;
    return nullptr;
}

std::optional<std::string> config_value_from_reply(const redis_asio::RedisValue &rv) {
    if (auto *arr = std::get_if<redis_asio::RedisValue::Array>(&rv.payload)) {
        if (arr->size() >= 2) {
            if (auto s = redis_asio::string_like((*arr)[1])) {
                return *s;
            }
        }
    }
    if (auto *kv = std::get_if<redis_asio::RedisValue::KVList>(&rv.payload)) {
        for (const auto &[k, v] : *kv) {
            if (k == "client-output-buffer-limit") {
                if (auto s = redis_asio::string_like(v)) {
                    return *s;
                }
            }
        }
    }
    return std::nullopt;
}
} // namespace

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

TEST(Unit, UnsubscribeCleanupWhenWeakExpired) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto *baton = RedisAsyncConnectionTestAccess::emplace_unsub(*conn, "cleanup.channel", false);
    ASSERT_EQ(RedisAsyncConnectionTestAccess::unsub_count(*conn, false), 1u);

    RedisAsyncConnectionTestAccess::call_unsub(&fake_ctx, baton, nullptr);

    EXPECT_EQ(RedisAsyncConnectionTestAccess::unsub_count(*conn, false), 0u);
}

TEST(Unit, SubscribeCleanupWhenWeakExpired) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto *baton = RedisAsyncConnectionTestAccess::emplace_sub(*conn, "cleanup.channel", false);
    ASSERT_EQ(RedisAsyncConnectionTestAccess::sub_count(*conn, false), 1u);

    RedisAsyncConnectionTestAccess::call_sub(&fake_ctx, baton, nullptr);

    EXPECT_EQ(RedisAsyncConnectionTestAccess::sub_count(*conn, false), 0u);
}

TEST(Unit, SubscribeAckCompletesOnce) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto *baton = RedisAsyncConnectionTestAccess::emplace_sub(*conn, "ack.channel", false, conn);
    bool ack_called = false;
    baton->on_ack = [&](std::error_code ec) {
        EXPECT_FALSE(ec);
        ack_called = true;
    };

    redisReply kind{};
    kind.type = REDIS_REPLY_STRING;
    const char *kind_text = "subscribe";
    kind.str = const_cast<char *>(kind_text);
    kind.len = std::strlen(kind_text);

    redisReply *elements[1] = {&kind};
    redisReply root{};
    root.type = REDIS_REPLY_ARRAY;
    root.elements = 1;
    root.element = elements;

    RedisAsyncConnectionTestAccess::call_sub(&fake_ctx, baton, &root);
    ioc.run();
    EXPECT_TRUE(ack_called);
    EXPECT_TRUE(baton->acked);

    ack_called = false;
    ioc.restart();
    RedisAsyncConnectionTestAccess::call_sub(&fake_ctx, baton, &root);
    ioc.poll();
    EXPECT_FALSE(ack_called);
}

TEST(Unit, SubscribeMessageDeliversToChannel) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto *baton = RedisAsyncConnectionTestAccess::emplace_sub(*conn, "news.chan", false, conn);
    baton->acked = true; // simulate successful subscribe

    bool got_message = false;
    std::error_code recv_ec;
    redis_asio::PublishMessage received{};

    asio::co_spawn(ioc, [conn, &got_message, &recv_ec, &received]() -> asio::awaitable<void> {
                       using boost::asio::as_tuple;
                       auto [ec, msg] = co_await conn->async_receive_publish(as_tuple(asio::use_awaitable));
                       recv_ec = ec;
                       if (!ec) {
                           received = std::move(msg);
                           got_message = true;
                       }
                       co_return; }, asio::detached);

    ioc.poll(); // allow coroutine to start and suspend on receive

    redisReply kind{};
    kind.type = REDIS_REPLY_STRING;
    const char *kind_text = "message";
    kind.str = const_cast<char *>(kind_text);
    kind.len = std::strlen(kind_text);

    redisReply channel{};
    channel.type = REDIS_REPLY_STRING;
    const char *channel_text = "news.chan";
    channel.str = const_cast<char *>(channel_text);
    channel.len = std::strlen(channel_text);

    redisReply payload{};
    payload.type = REDIS_REPLY_STRING;
    const char *payload_text = "payload";
    payload.str = const_cast<char *>(payload_text);
    payload.len = std::strlen(payload_text);

    redisReply *elements[3] = {&kind, &channel, &payload};
    redisReply root{};
    root.type = REDIS_REPLY_ARRAY;
    root.elements = 3;
    root.element = elements;

    RedisAsyncConnectionTestAccess::call_sub(&fake_ctx, baton, &root);
    ioc.restart();
    ioc.run_for(std::chrono::milliseconds(10));

    EXPECT_TRUE(got_message);
    EXPECT_FALSE(recv_ec);
    EXPECT_EQ(received.channel, "news.chan");
    EXPECT_EQ(received.payload, "payload");
}

TEST(Unit, UnsubscribeAckErasesBatonsAndNotifies) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto *sub_baton = RedisAsyncConnectionTestAccess::emplace_sub(*conn, "chan.ref", false, conn);
    auto *unsub_baton = RedisAsyncConnectionTestAccess::emplace_unsub(*conn, "chan.ref", false, conn);

    bool ack_called = false;
    unsub_baton->on_ack = [&](std::error_code ec) {
        EXPECT_FALSE(ec);
        ack_called = true;
    };

    redisReply kind{};
    kind.type = REDIS_REPLY_STRING;
    const char *kind_text = "unsubscribe";
    kind.str = const_cast<char *>(kind_text);
    kind.len = std::strlen(kind_text);

    redisReply *elements[1] = {&kind};
    redisReply root{};
    root.type = REDIS_REPLY_ARRAY;
    root.elements = 1;
    root.element = elements;

    RedisAsyncConnectionTestAccess::call_sub(&fake_ctx, sub_baton, &root);
    ioc.run();

    EXPECT_TRUE(ack_called);
    EXPECT_EQ(RedisAsyncConnectionTestAccess::sub_count(*conn, false), 0u);
    EXPECT_EQ(RedisAsyncConnectionTestAccess::unsub_count(*conn, false), 0u);
}

TEST(Unit, UnsubscribeAckFiresOnlyOnce) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto *sub_baton = RedisAsyncConnectionTestAccess::emplace_sub(*conn, "dup.chan", false, conn);
    sub_baton->acked = true;
    auto *unsub_baton = RedisAsyncConnectionTestAccess::emplace_unsub(*conn, "dup.chan", false, conn);

    int ack_count = 0;
    unsub_baton->on_ack = [&](std::error_code ec) {
        EXPECT_FALSE(ec);
        ++ack_count;
    };

    redisReply kind{};
    kind.type = REDIS_REPLY_STRING;
    const char *kind_text = "unsubscribe";
    kind.str = const_cast<char *>(kind_text);
    kind.len = std::strlen(kind_text);

    redisReply *elements[1] = {&kind};
    redisReply root{};
    root.type = REDIS_REPLY_ARRAY;
    root.elements = 1;
    root.element = elements;

    RedisAsyncConnectionTestAccess::call_sub(&fake_ctx, sub_baton, &root);
    ioc.run();
    EXPECT_EQ(ack_count, 1);
    EXPECT_EQ(RedisAsyncConnectionTestAccess::sub_count(*conn, false), 0u);
    EXPECT_EQ(RedisAsyncConnectionTestAccess::unsub_count(*conn, false), 0u);

    ioc.restart();
    RedisAsyncConnectionTestAccess::call_sub(&fake_ctx, nullptr, &root);
    ioc.run();
    EXPECT_EQ(ack_count, 1);
}

TEST(Unit, PatternUnsubscribeAckErasesBatonsAndNotifies) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto *sub_baton = RedisAsyncConnectionTestAccess::emplace_sub(*conn, "pattern.*", true, conn);
    auto *unsub_baton = RedisAsyncConnectionTestAccess::emplace_unsub(*conn, "pattern.*", true, conn);

    bool ack_called = false;
    unsub_baton->on_ack = [&](std::error_code ec) {
        EXPECT_FALSE(ec);
        ack_called = true;
    };

    redisReply kind{};
    kind.type = REDIS_REPLY_STRING;
    const char *kind_text = "punsubscribe";
    kind.str = const_cast<char *>(kind_text);
    kind.len = std::strlen(kind_text);

    redisReply *elements[1] = {&kind};
    redisReply root{};
    root.type = REDIS_REPLY_ARRAY;
    root.elements = 1;
    root.element = elements;

    RedisAsyncConnectionTestAccess::call_sub(&fake_ctx, sub_baton, &root);
    ioc.run();

    EXPECT_TRUE(ack_called);
    EXPECT_EQ(RedisAsyncConnectionTestAccess::sub_count(*conn, true), 0u);
    EXPECT_EQ(RedisAsyncConnectionTestAccess::unsub_count(*conn, true), 0u);
}

TEST(Unit, UnsubscribeHandlerCleansUpWhenAlive) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto *unsub_baton = RedisAsyncConnectionTestAccess::emplace_unsub(*conn, "direct.cleanup", false, conn);

    redisReply kind{};
    kind.type = REDIS_REPLY_STRING;
    const char *kind_text = "unsubscribe";
    kind.str = const_cast<char *>(kind_text);
    kind.len = std::strlen(kind_text);

    redisReply *elements[1] = {&kind};
    redisReply root{};
    root.type = REDIS_REPLY_ARRAY;
    root.elements = 1;
    root.element = elements;

    RedisAsyncConnectionTestAccess::call_unsub(&fake_ctx, unsub_baton, &root);
    ioc.run();

    EXPECT_EQ(RedisAsyncConnectionTestAccess::unsub_count(*conn, false), 0u);
}

TEST(Unit, ConnectCompletesOnlyAfterHello) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    RedisAsyncConnectionTestAccess::set_state(*conn, redis_asio::RedisAsyncConnection::ConnectionState::handshaking_resp3);

    bool done = false;
    std::error_code ec;
    conn->async_wait_connected([&](std::error_code e) {
        ec = e;
        done = true;
    });

    ioc.run_for(20ms);
    ioc.restart();
    EXPECT_FALSE(done);
    EXPECT_EQ(conn->waiter_count().connect, 1u);

    RedisAsyncConnectionTestAccess::handshake_result(*conn, {}, "hello-ok");
    ioc.run_for(20ms);
    ioc.restart();
    EXPECT_TRUE(done);
    EXPECT_FALSE(ec);
    EXPECT_TRUE(conn->is_connected());
    EXPECT_EQ(RedisAsyncConnectionTestAccess::state(*conn), redis_asio::RedisAsyncConnection::ConnectionState::ready);

    conn->stop();
    ioc.run_for(20ms);
    ioc.restart();
}

TEST(Unit, CommandBeforeReadyFailFastReturnsNotConnected) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    auto opts = opts_from_env();
    opts.command_policy = redis_asio::ConnectOptions::CommandPolicy::fail_fast;
    RedisAsyncConnectionTestAccess::set_opts(*conn, opts);
    RedisAsyncConnectionTestAccess::set_state(*conn, redis_asio::RedisAsyncConnection::ConnectionState::connecting_tcp);

    bool done = false;
    std::error_code ec;
    conn->async_command({"PING"}, [&](std::error_code e, redis_asio::RedisValue) {
        ec = e;
        done = true;
    });
    ioc.poll();

    EXPECT_TRUE(done);
    EXPECT_EQ(ec, make_error(redis_asio::error_category::errc::not_connected));
}

TEST(Unit, QueueUntilReadyDrainsAfterHelloInOrder) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    auto opts = opts_from_env();
    opts.command_policy = redis_asio::ConnectOptions::CommandPolicy::queue_until_ready;
    opts.pre_ready_queue_limit = 8;
    RedisAsyncConnectionTestAccess::set_opts(*conn, opts);
    RedisAsyncConnectionTestAccess::set_ctx(*conn, &fake_ctx);
    RedisAsyncConnectionTestAccess::set_state(*conn, redis_asio::RedisAsyncConnection::ConnectionState::handshaking_resp3);

    FakeCommandEnv env{};
    g_fake_command_env = &env;
    RedisAsyncConnectionTestAccess::set_async_command_argv_fn(*conn, &fake_command_argv);

    std::vector<int> completion_order;
    std::error_code ec1;
    std::error_code ec2;
    conn->async_command({"SET", "k", "1"}, [&](std::error_code ec, redis_asio::RedisValue) {
        ec1 = ec;
        completion_order.push_back(1);
    });
    conn->async_command({"INCR", "k"}, [&](std::error_code ec, redis_asio::RedisValue) {
        ec2 = ec;
        completion_order.push_back(2);
    });

    ioc.run_for(20ms);
    ioc.restart();
    EXPECT_TRUE(env.submitted.empty());
    EXPECT_EQ(RedisAsyncConnectionTestAccess::pre_ready_queue_size(*conn), 2u);
    EXPECT_TRUE(completion_order.empty());

    RedisAsyncConnectionTestAccess::handshake_result(*conn, {}, "hello-ok");
    ioc.run_for(20ms);
    ioc.restart();

    ASSERT_EQ(env.submitted.size(), 2u);
    EXPECT_EQ(env.submitted[0], "SET");
    EXPECT_EQ(env.submitted[1], "INCR");
    EXPECT_EQ(completion_order, std::vector<int>({1, 2}));
    EXPECT_FALSE(ec1);
    EXPECT_FALSE(ec2);

    g_fake_command_env = nullptr;
    RedisAsyncConnectionTestAccess::set_ctx(*conn, nullptr);
    conn->stop();
    ioc.run_for(20ms);
    ioc.restart();
}

TEST(Unit, DisconnectDuringInFlightCommandMapsTransportClosed) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    RedisAsyncConnectionTestAccess::set_ctx(*conn, &fake_ctx);
    RedisAsyncConnectionTestAccess::set_state(*conn, redis_asio::RedisAsyncConnection::ConnectionState::ready);

    FakeCommandEnv env{};
    env.invoke_with_null_reply = true;
    g_fake_command_env = &env;
    RedisAsyncConnectionTestAccess::set_async_command_argv_fn(*conn, &fake_command_argv);

    bool done = false;
    std::error_code ec;
    conn->async_command({"PING"}, [&](std::error_code e, redis_asio::RedisValue) {
        ec = e;
        done = true;
    });
    ioc.run_for(20ms);
    ioc.restart();

    EXPECT_TRUE(done);
    EXPECT_EQ(ec, make_error(redis_asio::error_category::errc::transport_closed));

    g_fake_command_env = nullptr;
    RedisAsyncConnectionTestAccess::set_ctx(*conn, nullptr);
}

TEST(Unit, SubmitFailureMapsSubmissionFailed) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();

    RedisAsyncConnectionTestAccess::set_ctx(*conn, &fake_ctx);
    RedisAsyncConnectionTestAccess::set_state(*conn, redis_asio::RedisAsyncConnection::ConnectionState::ready);
    RedisAsyncConnectionTestAccess::set_async_command_argv_fn(*conn, &fake_command_argv_submit_fail);

    bool done = false;
    std::error_code ec;
    conn->async_command({"PING"}, [&](std::error_code e, redis_asio::RedisValue) {
        ec = e;
        done = true;
    });
    ioc.run_for(20ms);
    ioc.restart();

    EXPECT_TRUE(done);
    EXPECT_EQ(ec, make_error(redis_asio::error_category::errc::submission_failed));
    RedisAsyncConnectionTestAccess::set_ctx(*conn, nullptr);
}

TEST(Unit, StopWhileReconnectTimerPendingDoesNotReconnect) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    auto opts = opts_from_env();
    opts.reconnect_initial = 20ms;
    opts.reconnect_max = 20ms;
    RedisAsyncConnectionTestAccess::set_opts(*conn, opts);
    RedisAsyncConnectionTestAccess::set_async_connect_fn(*conn, &fake_async_connect_counting);

    g_fake_connect_calls.store(0);
    RedisAsyncConnectionTestAccess::schedule_reconnect(*conn);
    conn->stop();
    ioc.run_for(100ms);

    EXPECT_EQ(g_fake_connect_calls.load(), 0);
}

TEST(Unit, StaleContextCallbacksIgnored) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    redisAsyncContext old_ctx{};
    old_ctx.data = conn.get();
    redisAsyncContext new_ctx{};
    new_ctx.data = conn.get();

    RedisAsyncConnectionTestAccess::set_ctx(*conn, &new_ctx);
    RedisAsyncConnectionTestAccess::set_state(*conn, redis_asio::RedisAsyncConnection::ConnectionState::ready);

    RedisAsyncConnectionTestAccess::call_handle_disconnect(&old_ctx, REDIS_ERR);
    RedisAsyncConnectionTestAccess::call_handle_connect(&old_ctx, REDIS_OK);
    ioc.run_for(20ms);
    ioc.restart();

    EXPECT_EQ(RedisAsyncConnectionTestAccess::ctx(*conn), &new_ctx);
    EXPECT_EQ(RedisAsyncConnectionTestAccess::state(*conn), redis_asio::RedisAsyncConnection::ConnectionState::ready);
    RedisAsyncConnectionTestAccess::set_ctx(*conn, nullptr);
}

TEST(Unit, QueueOverflowDeterministic) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto conn = redis_asio::RedisAsyncConnection::create(ioc.get_executor());

    auto opts = opts_from_env();
    opts.command_policy = redis_asio::ConnectOptions::CommandPolicy::queue_until_ready;
    opts.pre_ready_queue_limit = 2;
    RedisAsyncConnectionTestAccess::set_opts(*conn, opts);
    RedisAsyncConnectionTestAccess::set_state(*conn, redis_asio::RedisAsyncConnection::ConnectionState::connecting_tcp);

    std::vector<int> completion_order;
    std::error_code ec1;
    std::error_code ec2;
    std::error_code ec3;
    conn->async_command({"ECHO", "1"}, [&](std::error_code ec, redis_asio::RedisValue) {
        ec1 = ec;
        completion_order.push_back(1);
    });
    conn->async_command({"ECHO", "2"}, [&](std::error_code ec, redis_asio::RedisValue) {
        ec2 = ec;
        completion_order.push_back(2);
    });
    conn->async_command({"ECHO", "3"}, [&](std::error_code ec, redis_asio::RedisValue) {
        ec3 = ec;
        completion_order.push_back(3);
    });
    ioc.run_for(20ms);
    ioc.restart();

    EXPECT_EQ(ec3, make_error(redis_asio::error_category::errc::queue_overflow));
    EXPECT_FALSE(ec1);
    EXPECT_FALSE(ec2);
    EXPECT_EQ(RedisAsyncConnectionTestAccess::pre_ready_queue_size(*conn), 2u);
    EXPECT_EQ(completion_order, std::vector<int>({3}));

    redisAsyncContext fake_ctx{};
    fake_ctx.data = conn.get();
    RedisAsyncConnectionTestAccess::set_ctx(*conn, &fake_ctx);
    FakeCommandEnv env{};
    g_fake_command_env = &env;
    RedisAsyncConnectionTestAccess::set_async_command_argv_fn(*conn, &fake_command_argv);

    RedisAsyncConnectionTestAccess::handshake_result(*conn, {}, "hello-ok");
    ioc.run_for(20ms);
    ioc.restart();

    ASSERT_EQ(env.submitted.size(), 2u);
    EXPECT_EQ(env.submitted[0], "ECHO");
    EXPECT_EQ(env.submitted[1], "ECHO");
    EXPECT_EQ(completion_order, std::vector<int>({3, 1, 2}));
    EXPECT_FALSE(ec1);
    EXPECT_FALSE(ec2);

    g_fake_command_env = nullptr;
    RedisAsyncConnectionTestAccess::set_ctx(*conn, nullptr);
    conn->stop();
    ioc.run_for(20ms);
    ioc.restart();
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

    asio::co_spawn(ioc, [c_sub, c_ctl, log]() -> asio::awaitable<void> {
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
            co_spawn(co_await asio::this_coro::executor, [log, c_ctl, idstr]() -> asio::awaitable<void> {
            std::vector<std::string> kill = {"CLIENT", "KILL", "ID", idstr};
                log->log(redis_asio::Logger::Level::info, "Issuing CLIENT KILL to force reconnect");
                std::error_code ec; redis_asio::RedisValue rv;
                std::tie(ec, rv) = co_await c_ctl->async_command(kill, as_tuple(asio::use_awaitable));
            EXPECT_FALSE(ec);
                co_return;
            }, asio::detached);
        }

        // Wait for disconnect then reconnect
        EXPECT_TRUE(co_await c_sub->async_wait_disconnected(asio::use_awaitable));

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

TEST(Integration, PubSubOutputBufferLimitForcedDisconnectReconnectsNoCrash) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.pubsub_obuf");
    auto c_sub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    auto c_pub = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    auto c_ctl = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c_sub, c_pub, c_ctl]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        using boost::asio::experimental::awaitable_operators::operator||;
        auto ex = co_await asio::this_coro::executor;
        auto opts = opts_from_env();

        auto [ec1, a1] = co_await c_sub->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec1); (void)a1;
        auto [ec2, a2] = co_await c_pub->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec2); (void)a2;
        auto [ec3, a3] = co_await c_ctl->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec3); (void)a3;

        std::optional<std::string> previous_limit;
        std::error_code ec;
        redis_asio::RedisValue rv;
        std::tie(ec, rv) = co_await c_ctl->async_command({"CONFIG", "GET", "client-output-buffer-limit"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        if (!ec) {
            if (auto parsed = config_value_from_reply(rv)) {
                previous_limit = *parsed;
            }
        }
        EXPECT_TRUE(previous_limit.has_value());
        if (!previous_limit) {
            c_sub->stop();
            c_pub->stop();
            c_ctl->stop();
            co_return;
        }

        bool limit_modified = false;
        auto restore_limit = [&]() -> asio::awaitable<void> {
            if (!limit_modified || !previous_limit)
                co_return;
            std::vector<std::string> restore = {"CONFIG", "SET", "client-output-buffer-limit", *previous_limit};
            std::error_code restore_ec;
            redis_asio::RedisValue restore_rv;
            std::tie(restore_ec, restore_rv) = co_await c_ctl->async_command(restore, as_tuple(asio::use_awaitable));
            EXPECT_FALSE(restore_ec);
        };

        std::tie(ec, rv) = co_await c_ctl->async_command(
            {"CONFIG", "SET", "client-output-buffer-limit", "normal 0 0 0 slave 0 0 0 pubsub 1024 1024 1"},
            as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        limit_modified = !ec;
        if (ec) {
            co_await restore_limit();
            c_sub->stop();
            c_pub->stop();
            c_ctl->stop();
            co_return;
        }

        EXPECT_FALSE(co_await c_sub->async_psubscribe({"obuf.*"}, asio::use_awaitable));
        std::string payload(64 * 1024, 'x');
        std::vector<std::string> publish_big = {"PUBLISH", "obuf.big", payload};
        std::tie(ec, rv) = co_await c_pub->async_command(publish_big, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);

        asio::steady_timer wait_disconnect(ex);
        wait_disconnect.expires_after(10s);
        auto disc = co_await (wait_disconnect.async_wait(as_tuple(asio::use_awaitable))
                              || c_sub->async_wait_disconnected(as_tuple(asio::use_awaitable)));
        EXPECT_EQ(disc.index(), 1u);

        asio::steady_timer wait_reconnect(ex);
        wait_reconnect.expires_after(10s);
        auto reconn = co_await (wait_reconnect.async_wait(as_tuple(asio::use_awaitable))
                                || c_sub->async_wait_connected(as_tuple(asio::use_awaitable)));
        EXPECT_EQ(reconn.index(), 1u);
        if (reconn.index() == 1u) {
            auto [reconn_ec] = std::get<1>(reconn);
            EXPECT_FALSE(reconn_ec);
        }

        std::tie(ec, rv) = co_await c_pub->async_command({"PUBLISH", "obuf.small", "ok"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        asio::steady_timer receive_timeout(ex);
        receive_timeout.expires_after(5s);
        auto rx = co_await (receive_timeout.async_wait(as_tuple(asio::use_awaitable))
                            || c_sub->async_receive_publish(as_tuple(asio::use_awaitable)));
        EXPECT_EQ(rx.index(), 1u);

        co_await restore_limit();

        c_sub->stop();
        c_pub->stop();
        c_ctl->stop();
        co_return; }, asio::detached);

    ioc.run();
}

TEST(Integration, QueueUntilReadyDoesNotDrainBeforeHelloCompletion) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.queue_before_hello");
    auto c_ctl = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    auto c_q = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c_ctl, c_q]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;
        auto opts = opts_from_env();
        auto [ec1, a1] = co_await c_ctl->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec1); (void)a1;

        std::string key = "queue_before_hello:key";
        std::error_code ec;
        redis_asio::RedisValue rv;
        std::vector<std::string> del_cmd = {"DEL", key};
        std::tie(ec, rv) = co_await c_ctl->async_command(del_cmd, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        std::tie(ec, rv) = co_await c_ctl->async_command({"CLIENT", "PAUSE", "750", "ALL"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);

        auto qopts = opts;
        qopts.command_policy = redis_asio::ConnectOptions::CommandPolicy::queue_until_ready;
        bool connect_done = false;
        std::error_code connect_ec;
        asio::co_spawn(ex, [c_q, qopts, &connect_done, &connect_ec]() -> asio::awaitable<void> {
            using boost::asio::as_tuple;
            auto [ec, already] = co_await c_q->async_connect(qopts, as_tuple(asio::use_awaitable));
            (void)already;
            connect_ec = ec;
            connect_done = true;
            co_return;
        }, asio::detached);

        bool cmd_done = false;
        std::error_code cmd_ec;
        asio::co_spawn(ex, [c_q, key, &cmd_done, &cmd_ec]() -> asio::awaitable<void> {
            using boost::asio::as_tuple;
            std::error_code ec;
            redis_asio::RedisValue rv;
            std::vector<std::string> incr_cmd = {"INCR", key};
            std::tie(ec, rv) = co_await c_q->async_command(incr_cmd, as_tuple(asio::use_awaitable));
            cmd_ec = ec;
            cmd_done = true;
            co_return;
        }, asio::detached);

        asio::steady_timer observe(ex);
        observe.expires_after(100ms);
        co_await observe.async_wait(as_tuple(asio::use_awaitable));
        EXPECT_FALSE(cmd_done);
        EXPECT_EQ(RedisAsyncConnectionTestAccess::pre_ready_queue_size(*c_q), 1u);

        asio::steady_timer wait_connect(ex);
        wait_connect.expires_after(5s);
        for (;;) {
            if (connect_done || wait_connect.expiry() <= std::chrono::steady_clock::now())
                break;
            asio::steady_timer tick(ex);
            tick.expires_after(20ms);
            co_await tick.async_wait(as_tuple(asio::use_awaitable));
        }
        EXPECT_TRUE(connect_done);
        EXPECT_FALSE(connect_ec);

        asio::steady_timer wait_cmd(ex);
        wait_cmd.expires_after(5s);
        for (;;) {
            if (cmd_done || wait_cmd.expiry() <= std::chrono::steady_clock::now())
                break;
            asio::steady_timer tick(ex);
            tick.expires_after(20ms);
            co_await tick.async_wait(as_tuple(asio::use_awaitable));
        }
        EXPECT_TRUE(cmd_done);
        EXPECT_FALSE(cmd_ec);

        std::vector<std::string> get_cmd = {"GET", key};
        std::tie(ec, rv) = co_await c_ctl->async_command(get_cmd, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        auto s = redis_asio::string_like(rv);
        EXPECT_TRUE(s.has_value());
        if (s) {
            EXPECT_EQ(*s, "1");
        }

        c_q->stop();
        c_ctl->stop();
        co_return; }, asio::detached);
    ioc.run();
}

TEST(Integration, TransientDisconnectDoesNotPoisonSubsequentReadyState) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "redis_asio_integration.transient_disconnect");
    auto c_main = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);
    auto c_ctl = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c_main, c_ctl]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        using boost::asio::experimental::awaitable_operators::operator||;
        auto ex = co_await asio::this_coro::executor;
        auto opts = opts_from_env();

        auto [ec1, a1] = co_await c_main->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec1); (void)a1;
        auto [ec2, a2] = co_await c_ctl->async_connect(opts, as_tuple(asio::use_awaitable)); EXPECT_FALSE(ec2); (void)a2;

        std::error_code ec;
        redis_asio::RedisValue rv;
        std::tie(ec, rv) = co_await c_main->async_command({"PING"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);

        std::tie(ec, rv) = co_await c_main->async_command({"CLIENT", "ID"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        auto idp = std::get_if<long long>(&rv.payload);
        EXPECT_TRUE(idp != nullptr);
        if (!idp) {
            c_main->stop();
            c_ctl->stop();
            co_return;
        }
        std::string id = std::to_string(*idp);

        std::vector<std::string> kill = {"CLIENT", "KILL", "ID", id};
        std::tie(ec, rv) = co_await c_ctl->async_command(kill, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);

        asio::steady_timer wait_disconnect(ex);
        wait_disconnect.expires_after(5s);
        auto disc = co_await (wait_disconnect.async_wait(as_tuple(asio::use_awaitable))
                              || c_main->async_wait_disconnected(as_tuple(asio::use_awaitable)));
        EXPECT_EQ(disc.index(), 1u);

        asio::steady_timer wait_connect(ex);
        wait_connect.expires_after(10s);
        auto reconn = co_await (wait_connect.async_wait(as_tuple(asio::use_awaitable))
                                || c_main->async_wait_connected(as_tuple(asio::use_awaitable)));
        EXPECT_EQ(reconn.index(), 1u);
        if (reconn.index() == 1u) {
            auto [reconn_ec] = std::get<1>(reconn);
            EXPECT_FALSE(reconn_ec);
        }

        std::tie(ec, rv) = co_await c_main->async_command({"PING"}, as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        EXPECT_EQ(RedisAsyncConnectionTestAccess::state(*c_main), redis_asio::RedisAsyncConnection::ConnectionState::ready);

        c_main->stop();
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

TEST(Cancel, WaitConnectedCanceledByCancelAfter) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "cancel.waitconcancelafter");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;
        auto [ec] = co_await c->async_wait_connected(asio::cancel_after(10ms,as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(ec.value() == (int)redis_asio::error_category::errc::stopped);
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

TEST(Cancel, WaitConnectCanceledByCancelAfter) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "cancel.waitconcancelafter");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c, log]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;

        auto [ec, already] = co_await c->async_connect(bogus_opts(), asio::cancel_after(10ms, as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(ec.value() == (int)redis_asio::error_category::errc::stopped);
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

TEST(Cancel, WaitDisconnectedCanceledByCancelAfter) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    asio::io_context ioc;
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::critical, "cancel.waitdisconcancelafter");
    auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

    asio::co_spawn(ioc, [c]() -> asio::awaitable<void> {
        using boost::asio::as_tuple;
        auto ex = co_await asio::this_coro::executor;

        auto [ec, already] = co_await c->async_connect(opts_from_env(), as_tuple(asio::use_awaitable));
        EXPECT_FALSE(ec);
        EXPECT_FALSE(already);
        auto [ec2] = co_await c->async_wait_disconnected(asio::cancel_after(10ms, as_tuple(asio::use_awaitable)));
        EXPECT_TRUE(ec2.value() == (int)redis_asio::error_category::errc::stopped);
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
