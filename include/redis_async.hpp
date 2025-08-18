#pragma once
/**
 * @file redis_async.hpp
 * @brief A modern C++23 Boost.Asio–integrated hiredis connection supporting RESP3, TLS, coroutines, and auto-resubscribe.
 */

#include <boost/asio.hpp>
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
struct TLSOptions {
    bool use_tls{false};
    std::string ca_file;   // PEM bundle filename (optional)
    std::string ca_path;   // CA directory (optional)
    std::string cert_file; // client cert (optional)
    std::string key_file;  // client key (optional)
    bool verify_peer{true};
};

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

// // Run completion on the handler's associated executor & allocator.
// template <class Handler, class... Args>
// inline void complete_on_associated(Handler &&h,
//                                    const boost::asio::any_io_executor &fallback_ex,
//                                    Args &&...args) {
//     namespace asio = boost::asio;
//     auto ex = asio::get_associated_executor(h, fallback_ex);
//     auto al = asio::get_associated_allocator(h);
//     asio::post(ex, asio::bind_allocator(
//                        al, [h = std::forward<Handler>(h), tup = std::make_tuple(std::forward<Args>(args)...)]() mutable {
//                            std::apply([&](auto &&...a) { detail::complete(h, std::forward<decltype(a)>(a)...); }, tup);
//                        }));
// }
// Route completion to the handler's associated executor, but DECAY-COPY
// the arguments so no dangling refs cross the post/dispatch boundary.
template <class H, class... Args>
inline void complete_on_associated(H &&h, const asio::any_io_executor &fallback, Args &&...args) {
    using Handler = std::decay_t<H>;
    Handler h2 = std::forward<H>(h);
    auto ex = asio::get_associated_executor(h2, fallback);
    auto alloc = asio::get_associated_allocator(h2);
    auto tup = std::make_tuple(std::decay_t<Args>(std::forward<Args>(args))...);
    auto fn = [h3 = std::move(h2), tup = std::move(tup)]() mutable {
        std::apply([&](auto &&...as) { detail::complete(h3, std::forward<decltype(as)>(as)...); }, tup);
    };
    asio::post(ex, asio::bind_allocator(alloc, std::move(fn)));
}

} // namespace detail

class RedisAsyncConnection : public std::enable_shared_from_this<RedisAsyncConnection> {
  public:
    using executor_type = asio::any_io_executor;

    static std::shared_ptr<RedisAsyncConnection> create(executor_type exec, std::shared_ptr<Logger> logger = make_null_logger(), size_t max_backlog = 1024);
    ~RedisAsyncConnection() noexcept { shutdown_from_dtor_(); }

    executor_type get_executor() const noexcept { return strand_.get_inner_executor(); }

    // completion: void(std::error_code, bool already_connected)
    template <typename CompletionToken>
    auto async_connect(ConnectOptions opts, CompletionToken &&token);

    template <typename CompletionToken>
    auto async_wait_connected(CompletionToken &&token);

    template <typename CompletionToken>
    auto async_wait_disconnected(CompletionToken &&token);

    template <typename CompletionToken>
    auto async_subscribe(std::vector<std::string> channels, CompletionToken &&token);

    template <typename CompletionToken>
    auto async_psubscribe(std::vector<std::string> patterns, CompletionToken &&token);

    template <typename CompletionToken>
    auto async_unsubscribe(std::vector<std::string> channels, CompletionToken &&token);

    template <typename CompletionToken>
    auto async_punsubscribe(std::vector<std::string> patterns, CompletionToken &&token);

    template <typename CompletionToken>
    auto async_receive_publish(CompletionToken &&token);

    template <typename CompletionToken>
    auto async_command(const std::vector<std::string> &argv, CompletionToken &&token);

    // Human-readable HELLO summary (e.g., "redis 7.2 proto=3 role=master")
    std::string hello_summary() const { return hello_summary_; }

    enum class Health { healthy,
                        suspect,
                        unhealthy };
    Health health() const noexcept { return health_; }

    void stop();

    bool is_connected() const noexcept { return connected_.load(std::memory_order_relaxed); }
    bool using_resp3() const noexcept { return true; }

    struct WaiterCounts {
        std::size_t connect;
        std::size_t disconnect;
    };
    WaiterCounts waiter_count() const noexcept { return {connect_waiters_.size(), disconnect_waiters_.size()}; }

    inline std::shared_ptr<Logger> get_logger() const {
        return log_ ? log_ : make_null_logger();
    }

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
        std::function<void(std::error_code)> on_ack; // fired once on subscribe/psubscribe ack
        std::string subject;                         // channel or pattern
        bool is_pattern{false};
        bool acked{false};
    };

    // Long-lived baton for UNSUBSCRIBE/PUNSUBSCRIBE: receives ack + all errors
    struct UnsubBaton {
        std::weak_ptr<RedisAsyncConnection> w;
        std::function<void(std::error_code)> on_ack; // fired once on unsubscribe/punsubscribe ack (from the *sub counterpart)
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
    void issue_sub(const char *verb, std::string_view subject, std::function<void(std::error_code)> cb);
    void issue_unsub(const char *verb, std::string_view subject, std::function<void(std::error_code)> cb);

    void add_connect_waiter(std::move_only_function<void(std::error_code)> h) { connect_waiters_.push_back(std::move(h)); }
    void add_disconnect_waiter(std::move_only_function<void(std::error_code)> h) { disconnect_waiters_.push_back(std::move(h)); }
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

    std::vector<std::move_only_function<void(std::error_code)>> connect_waiters_;
    std::vector<std::move_only_function<void(std::error_code)>> disconnect_waiters_;
    // Handshake & health state
    std::string hello_summary_;
    Health health_{Health::healthy};
    int ping_failures_{0};
};

// ===== Inline template implementations =====

template <typename CompletionToken>
auto RedisAsyncConnection::async_connect(ConnectOptions opts, CompletionToken &&token) {
    using Sig = void(std::error_code, bool);
    return asio::async_initiate<CompletionToken, Sig>(
        [w = weak_from_this(), opts = std::move(opts)](auto handler) mutable {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, opts = std::move(opts), handler = std::move(handler)]() mutable {
                    if (self->is_connected()) {
                        auto h = std::move(handler);
                        // complete on handler's associated executor, not on our strand
                        detail::complete_on_associated(std::move(h), self->strand_.get_inner_executor(), std::error_code{}, true);
                        return;
                    }
                    // store a wrapper that will later complete on the handler's executor
                    auto ex = self->strand_.get_inner_executor();
                    self->add_connect_waiter([h = std::move(handler), ex](std::error_code ec) mutable {
                        detail::complete_on_associated(std::move(h), ex, ec, false);
                    });
                    if (self->connect_inflight_)
                        return;
                    self->connect_inflight_ = true;
                    self->do_connect(std::move(opts));
                });
            }
        },
        token);
}

template <typename CompletionToken>
auto RedisAsyncConnection::async_wait_connected(CompletionToken &&token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [w = weak_from_this()](auto handler) {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, handler = std::move(handler)]() mutable {
                    if (self->is_connected()) {
                        detail::complete_on_associated(std::move(handler), self->strand_.get_inner_executor(), std::error_code{});
                        return;
                    }
                    if(self->stopping_) {
                        detail::complete_on_associated(std::move(handler), self->strand_.get_inner_executor(), make_error(error_category::errc::stopped));
                        return;
                    }
                    auto ex = self->strand_.get_inner_executor();
                    self->add_connect_waiter([h = std::move(handler), ex](std::error_code ec) mutable {
                        detail::complete_on_associated(std::move(h), ex, ec);
                    });
                });
            }
        },
        token);
}

template <typename CompletionToken>
auto RedisAsyncConnection::async_wait_disconnected(CompletionToken &&token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [w = weak_from_this()](auto handler) {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, handler = std::move(handler)]() mutable {
                    if (!self->is_connected()) {
                        auto h = std::move(handler);
                        detail::complete_on_associated(std::move(h), self->strand_.get_inner_executor(), std::error_code{});
                        return;
                    }
                    if(self->stopping_) {
                        detail::complete_on_associated(std::move(handler), self->strand_.get_inner_executor(), make_error(error_category::errc::stopped));
                        return;
                    }
                    auto ex = self->strand_.get_inner_executor();
                    self->add_disconnect_waiter([h = std::move(handler), ex](std::error_code ec) mutable {
                        detail::complete_on_associated(std::move(h), ex, ec);
                    });
                });
            }
        },
        token);
}

template <typename CompletionToken>
auto RedisAsyncConnection::async_subscribe(std::vector<std::string> channels, CompletionToken &&token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [w = weak_from_this(), channels = std::move(channels)](auto handler) mutable {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, channels = std::move(channels), handler = std::move(handler)]() mutable {
                    auto ex_fallback = self->strand_.get_inner_executor();
                    if (!self->ctx_) {
                        detail::complete_on_associated(std::move(handler), ex_fallback, make_error(error_category::errc::not_connected));
                        return;
                    }
                    size_t remaining = channels.size();
                    if (remaining == 0) {
                        detail::complete_on_associated(std::move(handler), ex_fallback, std::error_code{});
                        return;
                    }
                    auto remaining_ptr = std::make_shared<size_t>(remaining);
                    // capture handler and complete it on its associated executor exactly once
                    auto hptr = std::make_shared<std::optional<decltype(handler)>>(std::move(handler));
                    auto done = [remaining_ptr, hptr, ex_fallback](std::error_code ec) mutable {
                        if (--*remaining_ptr == 0) {
                            if (auto hop = std::move(*hptr)) {
                                detail::complete_on_associated(std::move(*hop), ex_fallback, ec);
                            }
                        }
                    };
                    for (auto &ch : channels) {
                        self->ch_.refc[ch]++;
                        self->issue_sub("SUBSCRIBE", ch, done);
                    }
                });
            }
        },
        token);
}

template <typename CompletionToken>
auto RedisAsyncConnection::async_psubscribe(std::vector<std::string> patterns, CompletionToken &&token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [w = weak_from_this(), patterns = std::move(patterns)](auto handler) mutable {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, patterns = std::move(patterns), handler = std::move(handler)]() mutable {
                    auto ex_fallback = self->strand_.get_inner_executor();
                    if (!self->ctx_) {
                        detail::complete_on_associated(std::move(handler), ex_fallback, make_error(error_category::errc::not_connected));
                        return;
                    }
                    size_t remaining = patterns.size();
                    if (remaining == 0) {
                        detail::complete_on_associated(std::move(handler), ex_fallback, std::error_code{});
                        return;
                    }
                    auto remaining_ptr = std::make_shared<size_t>(remaining);
                    // capture handler and complete it on its associated executor exactly once
                    auto hptr = std::make_shared<std::optional<decltype(handler)>>(std::move(handler));
                    auto done = [remaining_ptr, hptr, ex_fallback](std::error_code ec) mutable {
                        if (--*remaining_ptr == 0) {
                            if (auto hop = std::move(*hptr)) {
                                detail::complete_on_associated(std::move(*hop), ex_fallback, ec);
                            }
                        }
                    };
                    for (auto &p : patterns) {
                        self->pch_.refc[p]++;
                        self->issue_sub("PSUBSCRIBE", p, done);
                    }
                });
            }
        },
        token);
}

template <typename CompletionToken>
auto RedisAsyncConnection::async_unsubscribe(std::vector<std::string> channels, CompletionToken &&token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [w = weak_from_this(), channels = std::move(channels)](auto handler) mutable {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, channels = std::move(channels), handler = std::move(handler)]() mutable {
                    auto ex_fallback = self->strand_.get_inner_executor();
                    if (!self->ctx_) {
                        detail::complete_on_associated(std::move(handler), ex_fallback, make_error(error_category::errc::not_connected));
                        return;
                    }
                    size_t remaining = channels.size();
                    if (remaining == 0) {
                        detail::complete_on_associated(std::move(handler), ex_fallback, std::error_code{});
                        return;
                    }
                    auto remaining_ptr = std::make_shared<size_t>(remaining);
                    // capture handler and complete it on its associated executor exactly once
                    auto hptr = std::make_shared<std::optional<decltype(handler)>>(std::move(handler));
                    auto done = [remaining_ptr, hptr, ex_fallback](std::error_code ec) mutable {
                        if (--*remaining_ptr == 0) {
                            if (auto hop = std::move(*hptr)) {
                                detail::complete_on_associated(std::move(*hop), ex_fallback, ec);
                            }
                        }
                    };
                    for (auto &ch : channels) {
                        auto it = self->ch_.refc.find(ch);
                        if (it != self->ch_.refc.end() && --(it->second) <= 0) {
                            self->ch_.refc.erase(it);
                            self->issue_unsub("UNSUBSCRIBE", ch, done);
                        } else {
                            // No active subscription, just complete
                            detail::complete_on_associated(done, ex_fallback, std::error_code{});
                        }
                    }
                });
            }
        },
        token);
}

template <typename CompletionToken>
auto RedisAsyncConnection::async_punsubscribe(std::vector<std::string> patterns, CompletionToken &&token) {
    return asio::async_initiate<CompletionToken, void(std::error_code)>(
        [w = weak_from_this(), patterns = std::move(patterns)](auto handler) mutable {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, patterns = std::move(patterns), handler = std::move(handler)]() mutable {
                    auto ex_fallback = self->strand_.get_inner_executor();
                    if (!self->ctx_) {
                        detail::complete_on_associated(std::move(handler), ex_fallback, make_error(error_category::errc::not_connected));
                        return;
                    }
                    size_t remaining = patterns.size();
                    if (remaining == 0) {
                        detail::complete_on_associated(std::move(handler), ex_fallback, std::error_code{});
                        return;
                    }
                    auto remaining_ptr = std::make_shared<size_t>(remaining);
                    // capture handler and complete it on its associated executor exactly once
                    auto hptr = std::make_shared<std::optional<decltype(handler)>>(std::move(handler));
                    auto done = [remaining_ptr, hptr, ex_fallback](std::error_code ec) mutable {
                        if (--*remaining_ptr == 0) {
                            if (auto hop = std::move(*hptr)) {
                                detail::complete_on_associated(std::move(*hop), ex_fallback, ec);
                            }
                        }
                    };
                    for (auto &p : patterns) {
                        auto it = self->pch_.refc.find(p);
                        if (it != self->pch_.refc.end() && --(it->second) <= 0) {
                            self->pch_.refc.erase(it);
                            self->issue_unsub("PUNSUBSCRIBE", p, done);
                        } else {
                            // No active subscription, just complete
                            detail::complete_on_associated(done, ex_fallback, std::error_code{});
                        }
                    }
                });
            }
        },
        token);
}

template <typename CompletionToken>
auto RedisAsyncConnection::async_receive_publish(CompletionToken &&token) {
    return pub_channel_.async_receive(std::forward<CompletionToken>(token));
}

template <typename CompletionToken>
auto RedisAsyncConnection::async_command(const std::vector<std::string> &argv, CompletionToken &&token) {
    using Sig = void(std::error_code, RedisValue);
    return asio::async_initiate<CompletionToken, Sig>(
        [w = weak_from_this(), argv](auto handler) mutable {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, argv, handler = std::move(handler)]() mutable {
                    auto ex_fallback = self->strand_.get_inner_executor();
                    if (!self->ctx_) {
                        detail::complete_on_associated(std::move(handler), ex_fallback,
                                                       make_error(error_category::errc::not_connected), RedisValue{});
                        return;
                    }
                    if (!argv.empty())
                        REDIS_TRACE_RT(self->log_, "command '{}' argc={} (payload elided)", argv.front(), argv.size());
                    std::vector<const char *> cargv;
                    cargv.reserve(argv.size());
                    std::vector<size_t> alen;
                    alen.reserve(argv.size());
                    for (auto &s : argv) {
                        cargv.push_back(s.data());
                        alen.push_back(s.size());
                    }
                    using Handler = decltype(handler);
                    struct Ctx {
                        std::weak_ptr<RedisAsyncConnection> w;
                        Handler h;
                        boost::asio::any_io_executor ex;
                    };
                    auto ex_for_handler = boost::asio::get_associated_executor(handler, ex_fallback);
                    auto *baton = new Ctx{self->weak_from_this(), std::move(handler), ex_for_handler};

                    if (redisAsyncCommandArgv(self->ctx_, [](redisAsyncContext *, void *r, void *priv) {
                            std::unique_ptr<Ctx> holder(static_cast<Ctx *>(priv));
                            if (!r) {
                                detail::complete_on_associated(std::move(holder->h), holder->ex,
                                                               make_error(error_category::errc::protocol_error), RedisValue{});
                                return;
                            }
                            detail::complete_on_associated(std::move(holder->h), holder->ex,
                                                           std::error_code{}, RedisValue::fromRaw(static_cast<redisReply *>(r))); }, baton, (int)cargv.size(), cargv.data(), reinterpret_cast<const size_t *>(alen.data())) != REDIS_OK) {
                        std::unique_ptr<Ctx> holder(baton);
                        detail::complete_on_associated(std::move(holder->h), holder->ex,
                                                       make_error(error_category::errc::protocol_error), RedisValue{});
                    }
                });
            }
        },
        token);
}

} // namespace redis_asio
