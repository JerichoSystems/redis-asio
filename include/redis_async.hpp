#pragma once
/**
 * @file redis_async.hpp
 * @brief A modern C++23 Boost.Asio–integrated hiredis connection supporting RESP3, TLS, coroutines, and auto-resubscribe.
 */

#include <boost/asio.hpp>
#include <boost/asio/execution.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <hiredis/async.h>
#include <hiredis/hiredis.h>
#include <hiredis/hiredis_ssl.h>

#include <atomic>
#include <chrono>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <type_traits> // for detail::complete (if not already present)
#include <unordered_map>
#include <variant>
#include <vector>
#include <iostream>

#include "redis_log.hpp"
#include "redis_value.hpp"

namespace redis_asio {
namespace asio = boost::asio;
using tcp = asio::ip::tcp;

// Forward decls
// struct RedisValue; // defined in redis_value.hpp
// std::expected<std::string, std::error_code> string_like(const RedisValue&);
class HiredisAsioAdapter;

// ---- Error category ----
class error_category : public std::error_category {
  public:
    enum class errc { ok = 0,
                      not_connected = 1,
                      connect_failed = 2,
                      ssl_error = 3,
                      protocol_error = 4,
                      stopped = 125 };
    const char *name() const noexcept override { return "redis_asio"; }
    std::string message(int ev) const override {
        switch (static_cast<errc>(ev)) {
        case errc::ok:
            return "ok";
        case errc::not_connected:
            return "not connected";
        case errc::connect_failed:
            return "connect failed";
        case errc::ssl_error:
            return "TLS/SSL error";
        case errc::protocol_error:
            return "protocol error";
        case errc::stopped:
            return "operation aborted";
        }
        return "unknown";
    }
};

inline const std::error_category &category() {
    static error_category cat;
    return cat;
}
inline std::error_code make_error(error_category::errc e) { return {static_cast<int>(e), category()}; }

// ---- Options ----
/**
 * TLSOptions
 *
 * Configuration options for TLS connections. All file-path entries are optional.
 */
struct TLSOptions {
    bool use_tls{false};
    std::string ca_file;   // PEM bundle filename (optional)
    std::string ca_path;   // CA directory (optional)
    std::string cert_file; // client cert (optional)
    std::string key_file;  // client key (optional)
    bool verify_peer{true};
};

/**
 * ConnectOptions
 *
 * Options controlling how the client connects and reconnects to the Redis server.
 */
struct ConnectOptions {
    std::string host{"127.0.0.1"};
    uint16_t port{6379};
    std::chrono::milliseconds connect_timeout{std::chrono::seconds(5)};
    std::chrono::milliseconds reconnect_initial{std::chrono::milliseconds(200)};
    std::chrono::milliseconds reconnect_max{std::chrono::seconds(10)};
    // keepalive
    std::chrono::milliseconds keepalive_period{std::chrono::seconds(60)};
    std::chrono::milliseconds keepalive_jitter{std::chrono::seconds(15)};
    TLSOptions tls{};
    std::optional<std::string> username;    // optional; if only password is present -> AUTH default <pwd>
    std::optional<std::string> password;    // optional
    std::optional<std::string> client_name; // optional: sent via HELLO SETNAME
};

/**
 * PublishMessage
 *
 * Represents a PUB/SUB message delivered from the server. For pattern-based
 * deliveries `pattern` is present.
 */
struct PublishMessage {
    std::string channel;
    std::string payload;
    std::optional<std::string> pattern; // only present for pmessage
};

namespace detail {
// Adapt completion handlers to either (args...) or single std::tuple<args...>
template <class H, class... Args>
inline void complete(H &h, Args &&...args) {
    if constexpr (std::is_invocable_v<H &, Args...>) {
        h(std::forward<Args>(args)...);
    } else {
        h(std::make_tuple(std::forward<Args>(args)...));
    }
}

// // Route completion to the handler's associated executor, but DECAY-COPY
// // the arguments so no dangling refs cross the post/dispatch boundary.
// template <class H, class... Args>
// inline void complete_on_associated(H &&h, const asio::any_io_executor &fallback, Args &&...args) {
//     using Handler = std::decay_t<H>;
//     Handler h2 = std::forward<H>(h);
//     auto ex = asio::get_associated_executor(h2, fallback);
//     auto alloc = asio::get_associated_allocator(h2);
//     auto tup = std::make_tuple(std::decay_t<Args>(std::forward<Args>(args))...);
//     auto fn = [h3 = std::move(h2), tup = std::move(tup)]() mutable {
//         std::apply([&](auto &&...as) { detail::complete(h3, std::forward<decltype(as)>(as)...); }, tup);
//     };
//     asio::post(ex, asio::bind_allocator(alloc, std::move(fn)));
// }

// Route completion to the handler's associated executor.
// DECAY-COPY the arguments so no dangling refs cross the boundary.
template <class H, class FallbackExecutor, class... Args>
inline void complete_on_associated(H&& h, const FallbackExecutor& fallback, Args&&... args) {
    using Handler = std::decay_t<H>;
    Handler h2 = std::forward<H>(h);

    // Compute traits from the concrete handler instance
    auto ex    = boost::asio::get_associated_executor(h2, fallback);
    auto alloc = boost::asio::get_associated_allocator(h2);

    auto tup = std::make_tuple(std::decay_t<Args>(std::forward<Args>(args))...);
    auto thunk  = [h3 = std::move(h2), tup = std::move(tup)]() mutable {
        std::apply([&](auto&&... as) { complete(h3, std::forward<decltype(as)>(as)...); }, tup);
    };

    using Ex = std::decay_t<decltype(ex)>;

        //boost::asio::dispatch(ex, boost::asio::bind_allocator(alloc, std::move(fn)));
           auto token = asio::bind_executor(ex, asio::bind_allocator(alloc, std::move(thunk)));

    // Inline-ok semantics:
    asio::dispatch(std::move(token));
    // if constexpr (std::is_same_v<Ex, boost::asio::any_completion_executor>) {
    //     // any_completion_executor doesn't satisfy post(ex, token) constraints.
    //     // Execute directly (no allocator support on this path).
    //     //fn.execute(ex, std::move(fn));
    //     //boost::asio::execute(ex, std::move(fn));
    //     //boost::asio::post(ex, std::move(fn));
    //     std::cerr<<"Warning: using any_completion_executor, executing directly\n";
    //     //fn();
    //     boost::asio::dispatch(ex, boost::asio::bind_allocator(alloc, std::move(fn)));
    //     //boost::asio::post(ex, std::move(fn));
    //     // boost::asio::post(ex, complete(std::move(h));
    //     // boost::asio::post(ex, complete(std::move(h)boost::asio::bind_allocator(alloc, std::move(fn)));
    // } else {
    //     // Normal path: honor associated allocator.
    //     std::cerr<<"Posting to non-any_completion_executor\n";
    //     boost::asio::post(ex, boost::asio::bind_allocator(alloc, std::move(fn)));
    // }
}

} // namespace detail

/**
 * RedisAsyncConnection
 *
 * Asynchronous, coroutine-friendly wrapper around a hiredis async context.
 * Thread-affine: all internal operations run on the executor/strand provided
 * to `create()`; public API methods are safe to call from any thread and use
 * completion handlers or awaitable completion tokens (Boost.Asio).
 */
class RedisAsyncConnection : public std::enable_shared_from_this<RedisAsyncConnection> {
  public:
    using executor_type = asio::any_io_executor;

    /**
     * Create a connection instance bound to `exec`.
     *
     * Parameters:
     * - exec: executor or io_context where the connection's strand will run.
     * - logger: optional logger; defaults to a null logger.
     * - max_backlog: size of the internal pub/sub backlog channel.
     */
    static std::shared_ptr<RedisAsyncConnection> create(executor_type exec, std::shared_ptr<Logger> logger = make_null_logger(), size_t max_backlog = 1024);

    /**
     * Destructor; performs best-effort synchronous shutdown.
     */
    ~RedisAsyncConnection() noexcept { shutdown_from_dtor_(); }

    /**
     * Return the executor bound to this connection (the inner executor of
     * the strand used for serialized execution of internal state).
     */
    executor_type get_executor() const noexcept { return strand_.get_inner_executor(); }

    /**
     * Initiate a connection (or return immediately if already connected).
     * Completion signature: void(std::error_code, bool already_connected).
     *
     * The CompletionToken can be any Asio completion token (co_await, use_future,
     * completion handler, etc.).
     */
    template <typename CompletionToken>
    auto async_connect(ConnectOptions opts, CompletionToken &&token);

    /**
     * Await until the connection is established. Completion signature:
     * void(std::error_code) where a default-constructed error_code means success.
     */
    template <typename CompletionToken>
    auto async_wait_connected(CompletionToken &&token);

    /**
     * Await until the connection has become disconnected. Completion signature:
     * void(std::error_code) where default-constructed error_code means success.
     */
    template <typename CompletionToken>
    auto async_wait_disconnected(CompletionToken &&token);

    /**
     * Subscribe to one or more channels. Completion signature: void(std::error_code).
     * The handler will be invoked once the SUBSCRIBE has been acknowledged by the server.
     */
    template <typename CompletionToken>
    auto async_subscribe(std::vector<std::string> channels, CompletionToken &&token);

    /**
     * Pattern-subscribe to one or more patterns (PSUBSCRIBE). Completion
     * signature: void(std::error_code).
     */
    template <typename CompletionToken>
    auto async_psubscribe(std::vector<std::string> patterns, CompletionToken &&token);

    /**
     * Unsubscribe from channels. Completion signature: void(std::error_code).
     * If a channel has multiple subscribe references, use_count semantics apply.
     */
    template <typename CompletionToken>
    auto async_unsubscribe(std::vector<std::string> channels, CompletionToken &&token);

    /**
     * Unsubscribe from patterns (PUNSUBSCRIBE). Completion signature:
     * void(std::error_code).
     */
    template <typename CompletionToken>
    auto async_punsubscribe(std::vector<std::string> patterns, CompletionToken &&token);

    /**
     * Await delivery of the next publish message from the server.
     * Completion signature: void(boost::system::error_code, PublishMessage)
     * (the channel uses Boost.System compatibility for the concurrent_channel API).
     */
    template <typename CompletionToken>
    auto async_receive_publish(CompletionToken &&token);

    /**
     * Execute a Redis command. The `argv` vector contains the command name and
     * its arguments. Completion signature: void(std::error_code, RedisValue).
     *
     * The call is safe from any thread; the actual command is submitted on the
     * connection's strand. If the connection is not established, the error
     * `error_category::errc::not_connected` is returned immediately.
     */
    template <typename CompletionToken>
    auto async_command(const std::vector<std::string> &argv, CompletionToken &&token);

    // Human-readable HELLO summary (e.g., "redis 7.2 proto=3 role=master")
    /**
     * Human-readable HELLO summary returned from the server after connection
     * setup (e.g. "redis 7.2 proto=3 role=master"). Empty before handshake.
     */
    std::string hello_summary() const { return hello_summary_; }

    enum class Health { healthy,
                        suspect,
                        unhealthy };
    /**
     * Returns the last-known connection health state.
     */
    Health health() const noexcept { return health_; }

    /**
     * Stop the connection and cancel outstanding waiters. Safe to call from
     * any thread; completion handlers waiting on connected/disconnected will
     * be invoked with `error_category::errc::stopped`.
     */
    void stop();

    /**
     * Returns true if the connection is currently established.
     */
    bool is_connected() const noexcept { return connected_.load(std::memory_order_relaxed); }
    /**
     * Returns whether RESP3 is used by the connection (currently always true).
     */
    bool using_resp3() const noexcept { return true; }

    struct WaiterCounts {
        std::size_t connect;
        std::size_t disconnect;
    };
    /**
     * Return number of currently-registered waiters for connect/disconnect.
     */
    WaiterCounts waiter_count() const noexcept { return {connect_waiters_.size(), disconnect_waiters_.size()}; }

    /**
     * Return the logger used by this connection (never null).
     */
    inline std::shared_ptr<Logger> get_logger() const {
        return log_ ? log_ : make_null_logger();
    }

    /**
     * Initialize OpenSSL for hiredis (safe to call multiple times).
     */
    static void initOpenSSL() {
        static std::once_flag once;
        std::call_once(once, [] { redisInitOpenSSL(); }); // safe no-op on modern OpenSSL
    }

  private:
    void shutdown_from_dtor_() noexcept;

    struct SubSet {
        std::unordered_map<std::string, int> refc;
    };

    explicit RedisAsyncConnection(executor_type exec, std::shared_ptr<Logger> log, size_t max_backlog);
    void do_connect(ConnectOptions);
    void schedule_reconnect();
    void on_connected();
    void on_disconnected(int status);
    // Long-lived baton for SUBSCRIBE/PSUBSCRIBE: receives ack + all publishes + unsubscribe/punsubscribe
    struct SubBaton {
        std::weak_ptr<RedisAsyncConnection> w;
        asio::any_completion_handler<void(std::error_code)> on_ack; // fired once on subscribe/psubscribe ack
        std::string subject;                         // channel or pattern
        bool is_pattern{false};
        bool acked{false};
    };

    // Long-lived baton for UNSUBSCRIBE/PUNSUBSCRIBE: receives ack + all errors
    struct UnsubBaton {
        std::weak_ptr<RedisAsyncConnection> w;
        asio::any_completion_handler<void(std::error_code)> on_ack; // fired once on unsubscribe/punsubscribe ack (from the *sub counterpart)
        std::string subject;                         // channel or pattern
        bool is_pattern{false};
        bool acked{false};
    };

    static void handle_connect(const redisAsyncContext *c, int status);
    static void handle_disconnect(const redisAsyncContext *c, int status);
    // static void handle_push(redisAsyncContext *c, void *r);

    // One-roundtrip: HELLO 3 [AUTH user pass|AUTH default pass] [SETNAME name]
    void send_handshake_hello();
    void restore_subscriptions();

    // SUB/PSUB helper
    void issue_sub(const char *verb, std::string_view subject, asio::any_completion_handler<void(std::error_code)> cb);
    void issue_unsub(const char *verb, std::string_view subject, asio::any_completion_handler<void(std::error_code)> cb);

    uint64_t add_connect_waiter(asio::any_completion_handler<void(std::error_code)> h) {
        const auto id = next_waiter_id_++;
        connect_waiters_.emplace(id, std::move(h));
        return id;
    }
    uint64_t add_disconnect_waiter(asio::any_completion_handler<void(std::error_code)> h) {
        const auto id = next_waiter_id_++;
        disconnect_waiters_.emplace(id, std::move(h));
        return id;
    }

    // Returns the completion function if removed, else empty.
    asio::any_completion_handler<void(std::error_code)> erase_connect_waiter(uint64_t id) {
        auto it = connect_waiters_.find(id);
        if (it == connect_waiters_.end())
            return {};
        auto fn = std::move(it->second);
        connect_waiters_.erase(it);
        return std::move(fn);
    }

    // Returns the completion function if removed, else empty.
    asio::any_completion_handler<void(std::error_code)> erase_disconnect_waiter(uint64_t id) {
        auto it = disconnect_waiters_.find(id);
        if (it == disconnect_waiters_.end())
            return {};
        auto fn = std::move(it->second);
        disconnect_waiters_.erase(it);
        return std::move(fn);
    }

    // keepalive
    void start_keepalive();
    void schedule_next_ping();

  private:
    asio::strand<executor_type> strand_;
    asio::steady_timer reconnect_timer_;
    asio::steady_timer ping_timer_;
    // Note: with Boost 1.82+ channel + use_awaitable, the message signature usually carries an error_code first.
    asio::experimental::concurrent_channel<void(boost::system::error_code, PublishMessage)> pub_channel_;

    ConnectOptions opts_{};
    std::shared_ptr<Logger> log_;
    std::shared_ptr<HiredisAsioAdapter> adapter_;
    redisAsyncContext *ctx_{nullptr};
    redisSSLContext *sslctx_{nullptr};
    std::atomic<bool> connected_{false};
    // one baton per active subject (kept until unsubscribe/punsubscribe)
    std::unordered_map<std::string, std::unique_ptr<SubBaton>> ch_batons_;
    std::unordered_map<std::string, std::unique_ptr<SubBaton>> pch_batons_;

    // one baton per active unsub operation (kept until unsubscribe/punsubscribe occurs)
    std::unordered_map<std::string, std::unique_ptr<UnsubBaton>> ch_unsub_batons_;
    std::unordered_map<std::string, std::unique_ptr<UnsubBaton>> pch_unsub_batons_;
    std::chrono::milliseconds backoff_{};
    bool stopping_{false};
    bool connect_inflight_{false};

    SubSet ch_;
    SubSet pch_;

    std::unordered_map<uint64_t, asio::any_completion_handler<void(std::error_code)>> connect_waiters_;
    std::unordered_map<uint64_t, asio::any_completion_handler<void(std::error_code)>> disconnect_waiters_;
    std::atomic_uint64_t next_waiter_id_{0};

    // Handshake & health state
    std::string hello_summary_;
    Health health_{Health::healthy};
    int ping_failures_{0};
};

// ===== Inline template implementations =====

#include "redis_async_impl.ipp"

} // namespace redis_asio
