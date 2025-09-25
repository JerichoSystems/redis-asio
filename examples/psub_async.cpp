#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

#include "redis_async.hpp"
#include "redis_log.hpp"

using namespace std::chrono_literals;
namespace asio = boost::asio;

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

static asio::awaitable<void>
subscriber(std::shared_ptr<redis_asio::RedisAsyncConnection> c) {
    using boost::asio::as_tuple;
    auto opts = opts_from_env();

    auto log = c->get_logger();
    auto [ec, already] =
        co_await c->async_connect(opts, as_tuple(asio::use_awaitable));
    if (ec) {
        REDIS_ERROR_RT(log, "connect failed: {}", ec.message());
        co_return;
    }
    (void)already;
    REDIS_INFO_RT(log, "connected: {}", c->hello_summary());

    std::error_code ecps =
        co_await c->async_psubscribe({"news.*"}, asio::use_awaitable);
    if (ecps) {
        REDIS_ERROR_RT(log, "psubscribe failed: {}", ecps.message());
        co_return;
    }
    REDIS_INFO_RT(log, "subscribed to pattern: news.*");

    for (int i = 4; i >= 0; --i) {
        auto [rxec, msg] =
            co_await c->async_receive_publish(as_tuple(asio::use_awaitable));
        if (rxec) {
            REDIS_WARN_RT(log, "receive error: {}", rxec.message());
            if (!c->is_connected())
                break; // likely reconnecting; exit demo loop
            continue;
        }
        if (msg.pattern) {
            REDIS_INFO_RT(log, "pmessage pattern={} channel={} payload={} left={}", *msg.pattern,
                          msg.channel, msg.payload, i);
        } else {
            REDIS_INFO_RT(log, "message channel={} payload={} left={}", msg.channel, msg.payload, i);
        }
    }
    co_return;
}

// Optional: simple publisher that sends one test message after a short delay
static asio::awaitable<void> one_shot_publisher(asio::any_io_executor ex) {
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::trace, "psub_async");
    auto c = redis_asio::RedisAsyncConnection::create(ex, log);
    using boost::asio::as_tuple;
    auto [ec, already] =
        co_await c->async_connect(opts_from_env(), as_tuple(asio::use_awaitable));
    if (ec)
        co_return;
    (void)already;
    asio::steady_timer t(co_await asio::this_coro::executor);
    t.expires_after(500ms);
    co_await t.async_wait(asio::use_awaitable);
    std::error_code ecpub;
    redis_asio::RedisValue rv;
    std::tie(ecpub, rv) = co_await c->async_command(
        {"PUBLISH", "news.123", "hello"}, as_tuple(asio::use_awaitable));
    if (!ecpub)
        REDIS_INFO_RT(log, "published test message to news.123 with {} subscribers", rv.toString());
    co_return;
}

int main() {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    auto log = redis_asio::make_clog_logger(redis_asio::Logger::Level::trace, "psub_async");
    try {
        asio::io_context ioc;
        auto c = redis_asio::RedisAsyncConnection::create(ioc.get_executor(), log);

        asio::co_spawn(ioc, subscriber(std::move(c)), asio::detached);
        // Also fire a one-shot publisher so you see a message quickly (optional)
        asio::co_spawn(ioc, one_shot_publisher(ioc.get_executor()), asio::detached);

        ioc.run();
    } catch (const std::exception &e) {
        REDIS_ERROR_RT(log, "exception: {}", e.what());
        return 1;
    }
    return 0;
}
