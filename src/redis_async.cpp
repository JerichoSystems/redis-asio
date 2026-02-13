#include "redis_async.hpp"
#include "hiredis_asio_adapter.hpp"
#include "redis_value.hpp"

#include <boost/system/error_code.hpp>
#include <cassert>
#include <cstring>
#include <random>

namespace redis_asio {

using namespace std::chrono_literals;

namespace {
std::string_view reply_first_string(redisReply *r) {
    if (!r)
        return {};
    redisReply *head = nullptr;
    if (r->type == REDIS_REPLY_ARRAY && r->elements > 0)
        head = r->element[0];
    else if (r->type == REDIS_REPLY_PUSH && r->elements > 0)
        head = r->element[0];
    if (head && head->type == REDIS_REPLY_STRING)
        return {head->str, static_cast<size_t>(head->len)};
    return {};
}
} // namespace

static inline std::chrono::milliseconds next_backoff(std::chrono::milliseconds cur,
                                                     std::chrono::milliseconds initial,
                                                     std::chrono::milliseconds maxv) {
    if (cur.count() == 0)
        return initial;
    auto next = cur * 2;
    if (next > maxv)
        next = maxv;
    return next;
}

static inline std::chrono::milliseconds jitter(std::chrono::milliseconds base,
                                               std::chrono::milliseconds plus_minus) {
    if (plus_minus.count() == 0)
        return base;
    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<long long> d(-plus_minus.count(), plus_minus.count());
    auto j = d(rng);
    auto ms = base.count() + j;
    return std::chrono::milliseconds{ms > 0 ? ms : 1};
}

std::shared_ptr<RedisAsyncConnection>
RedisAsyncConnection::create(executor_type exec, std::shared_ptr<Logger> logger, size_t max_backlog) {
    return std::shared_ptr<RedisAsyncConnection>(new RedisAsyncConnection(exec, std::move(logger), max_backlog));
}

RedisAsyncConnection::RedisAsyncConnection(executor_type exec, std::shared_ptr<Logger> log, size_t max_backlog)
    : strand_(asio::make_strand(exec)),
      reconnect_timer_(strand_),
      ping_timer_(strand_),
      pub_channel_(strand_, max_backlog),
      log_(std::move(log)) {}

std::string_view RedisAsyncConnection::state_name(ConnectionState state) noexcept {
    switch (state) {
    case ConnectionState::disconnected:
        return "disconnected";
    case ConnectionState::connecting_tcp:
        return "connecting_tcp";
    case ConnectionState::handshaking_resp3:
        return "handshaking_resp3";
    case ConnectionState::ready:
        return "ready";
    case ConnectionState::stopping:
        return "stopping";
    }
    return "unknown";
}

void RedisAsyncConnection::transition_state(ConnectionState next, std::string_view reason) {
    if (state_ == next)
        return;
    REDIS_DEBUG_RT(log_, "state {} -> {} ({})", state_name(state_), state_name(next), reason);
    state_ = next;
}

bool RedisAsyncConnection::should_queue_until_ready() const noexcept {
    return opts_.command_policy == ConnectOptions::CommandPolicy::queue_until_ready
           && (state_ == ConnectionState::connecting_tcp || state_ == ConnectionState::handshaking_resp3);
}

void RedisAsyncConnection::fail_pre_ready_queue(std::error_code ec) {
    while (!pre_ready_queue_.empty()) {
        auto queued = std::move(pre_ready_queue_.front());
        pre_ready_queue_.pop_front();
        if (queued.completion) {
            detail::complete_on_associated(std::move(queued.completion), strand_, ec, RedisValue{});
        }
    }
}

void RedisAsyncConnection::enqueue_or_reject_command(std::vector<std::string> argv, CommandHandler completion) {
    if (pre_ready_queue_.size() >= opts_.pre_ready_queue_limit) {
        REDIS_WARN_RT(log_, "pre-ready queue overflow (limit={})", opts_.pre_ready_queue_limit);
        detail::complete_on_associated(std::move(completion), strand_, make_error(error_category::errc::queue_overflow), RedisValue{});
        return;
    }
    pre_ready_queue_.push_back(PendingCommand{std::move(argv), std::move(completion)});
    REDIS_TRACE_RT(log_, "queued pre-ready command count={}", pre_ready_queue_.size());
}

void RedisAsyncConnection::submit_command_ready(std::vector<std::string> argv, CommandHandler completion) {
    if (!ctx_ || !is_ready_state()) {
        detail::complete_on_associated(std::move(completion), strand_, make_error(error_category::errc::not_connected), RedisValue{});
        return;
    }
    if (!argv.empty())
        REDIS_TRACE_RT(log_, "command '{}' argc={} (payload elided)", argv.front(), argv.size());

    std::vector<const char *> cargv;
    cargv.reserve(argv.size());
    std::vector<size_t> alen;
    alen.reserve(argv.size());
    for (const auto &s : argv) {
        cargv.push_back(s.data());
        alen.push_back(s.size());
    }

    auto *baton = new CommandBaton{std::move(completion)};
    if (async_command_argv_fn_(ctx_,
                               [](redisAsyncContext *, void *r, void *priv) {
                                   std::unique_ptr<CommandBaton> holder(static_cast<CommandBaton *>(priv));
                                   if (!r) {
                                       detail::complete_on_associated(std::move(holder->completion), boost::asio::system_executor{},
                                                                      make_error(error_category::errc::transport_closed), RedisValue{});
                                       return;
                                   }
                                   detail::complete_on_associated(std::move(holder->completion), boost::asio::system_executor{}, std::error_code{},
                                                                  RedisValue::fromRaw(static_cast<redisReply *>(r)));
                               },
                               baton,
                               static_cast<int>(cargv.size()),
                               cargv.data(),
                               reinterpret_cast<const size_t *>(alen.data()))
        != REDIS_OK) {
        std::unique_ptr<CommandBaton> holder(baton);
        detail::complete_on_associated(std::move(holder->completion), strand_, make_error(error_category::errc::submission_failed), RedisValue{});
    }
}

void RedisAsyncConnection::drain_pre_ready_queue() {
    if (!is_ready_state() || !ctx_ || pre_ready_queue_.empty())
        return;

    REDIS_TRACE_RT(log_, "draining {} queued pre-ready command(s)", pre_ready_queue_.size());
    while (is_ready_state() && ctx_ && !pre_ready_queue_.empty()) {
        auto queued = std::move(pre_ready_queue_.front());
        pre_ready_queue_.pop_front();
        submit_command_ready(std::move(queued.argv), std::move(queued.completion));
    }
}

void RedisAsyncConnection::shutdown_from_dtor_() noexcept {
    // best-effort, synchronous; no posts, no shared_from_this
    stopping_ = true;
    transition_state(ConnectionState::stopping, "destructor");
    connected_.store(false, std::memory_order_relaxed);
    reconnect_timer_.cancel();
    ping_timer_.cancel();
    pub_channel_.close();
    fail_pre_ready_queue(make_error(error_category::errc::stopped));

    if (ctx_) {
        // detach our static callbacks and user-data so late invokes are nops
        set_connect_callback_fn_(ctx_, nullptr);
        set_disconnect_callback_fn_(ctx_, nullptr);
        set_push_callback_fn_(ctx_, nullptr);
        ctx_->data = nullptr;
        // force free: hiredis guarantees pending callbacks get NULL replies
        async_free_fn_(ctx_);
        ctx_ = nullptr;
    }

    if (sslctx_) {
        redisFreeSSLContext(sslctx_); // we own it; safe to free now
        sslctx_ = nullptr;
    }
    // ensure adapter stops waiting on the fd before it is destroyed
    adapter_.reset();
    // Best-effort notify waiters (no executor affinity guarantees).
    auto cw = std::move(connect_waiters_);
    connect_waiters_.clear();
    auto dw = std::move(disconnect_waiters_);
    disconnect_waiters_.clear();
    for (auto &[id, h] : cw) {
        try {
            h(make_error(error_category::errc::stopped));
        } catch (...) {
        }
    }
    for (auto &[id, h] : dw) {
        try {
            h(make_error(error_category::errc::stopped));
        } catch (...) {
        }
    }
}
void RedisAsyncConnection::stop() {
    asio::dispatch(strand_, [self = shared_from_this()] {
        if (self->stopping_)
            return;
        self->stopping_ = true;
        self->transition_state(ConnectionState::stopping, "stop()");
        self->connected_.store(false, std::memory_order_relaxed);
        self->pub_channel_.close();
        self->fail_pre_ready_queue(make_error(error_category::errc::stopped));

        auto cw = std::move(self->connect_waiters_);
        self->connect_waiters_.clear();
        auto dw = std::move(self->disconnect_waiters_);
        self->disconnect_waiters_.clear();
        for (auto &[id, h] : cw) {
            try {
                detail::complete_on_associated(std::move(h), self->strand_, make_error(error_category::errc::stopped));
            } catch (...) {
            }
        }
        for (auto &[id, h] : dw) {
            try {
                detail::complete_on_associated(std::move(h), self->strand_, make_error(error_category::errc::stopped));
            } catch (...) {
            }
        }
        self->reconnect_timer_.cancel();
        self->ping_timer_.cancel();
        if (self->sslctx_) {
            redisFreeSSLContext(self->sslctx_);
            self->sslctx_ = nullptr;
        }
        if (self->adapter_)
            self->adapter_->stop();
        self->adapter_.reset();
        if (self->ctx_) {
            self->set_connect_callback_fn_(self->ctx_, nullptr);
            self->set_disconnect_callback_fn_(self->ctx_, nullptr);
            self->set_push_callback_fn_(self->ctx_, nullptr);
            self->ctx_->data = nullptr;
            self->async_disconnect_fn_(self->ctx_);
            self->ctx_ = nullptr;
        }
    });
}

void RedisAsyncConnection::do_connect(ConnectOptions opts) {
    opts_ = std::move(opts);
    auto attempt = [w = weak_from_this()] {
        if (auto self = w.lock()) {
            if (self->stopping_)
                return;
            self->transition_state(ConnectionState::connecting_tcp, "do_connect()");
            self->connected_.store(false, std::memory_order_relaxed);
            REDIS_INFO_RT(self->log_, "connecting to {}:{} TLS={}", self->opts_.host, self->opts_.port, self->opts_.tls.use_tls);

            if (self->ctx_) {
                // Avoid redisAsyncFree() during reconnect races.
                self->set_connect_callback_fn_(self->ctx_, nullptr);
                self->set_disconnect_callback_fn_(self->ctx_, nullptr);
                self->set_push_callback_fn_(self->ctx_, nullptr);
                self->ctx_->data = nullptr;
                self->async_disconnect_fn_(self->ctx_);
                self->ctx_ = nullptr;
            }
            if (self->sslctx_) {
                redisFreeSSLContext(self->sslctx_); // we own it; safe to free now
                self->sslctx_ = nullptr;
            }

            self->ctx_ = self->async_connect_fn_(self->opts_.host.c_str(), self->opts_.port);
            if (!self->ctx_ || self->ctx_->err) {
                int err = self->ctx_ ? self->ctx_->err : -1;
                REDIS_WARN_RT(self->log_, "connect failed immediately: {}", err);
                self->on_disconnected(err);
                return;
            }
            ++self->ctx_generation_;
            self->ctx_->data = self.get();

            self->set_connect_callback_fn_(self->ctx_, &RedisAsyncConnection::handle_connect);
            self->set_disconnect_callback_fn_(self->ctx_, &RedisAsyncConnection::handle_disconnect);
            // redisAsyncSetPushCallback(self->ctx_, &RedisAsyncConnection::handle_push);

            if (self->opts_.tls.use_tls) {
                redisSSLContextError ssl_err = REDIS_SSL_CTX_NONE;
                redisSSLOptions ssl_opts{};
                if (!self->opts_.tls.ca_file.empty())
                    ssl_opts.cacert_filename = self->opts_.tls.ca_file.c_str();
                if (!self->opts_.tls.ca_path.empty())
                    ssl_opts.capath = self->opts_.tls.ca_path.c_str();
                if (!self->opts_.tls.cert_file.empty())
                    ssl_opts.cert_filename = self->opts_.tls.cert_file.c_str();
                if (!self->opts_.tls.key_file.empty())
                    ssl_opts.private_key_filename = self->opts_.tls.key_file.c_str();
                ssl_opts.server_name = self->opts_.host.c_str(); // SNI
                ssl_opts.verify_mode = self->opts_.tls.verify_peer ? REDIS_SSL_VERIFY_PEER : REDIS_SSL_VERIFY_NONE;

                redisSSLContext *ssl = redisCreateSSLContextWithOptions(&ssl_opts, &ssl_err);
                if (!ssl) {
                    REDIS_ERROR_RT(self->log_, "SSL ctx create failed: {}", redisSSLContextGetError(ssl_err));
                    self->on_disconnected(-1);
                    return;
                }
                // Note: redisInitiateSSLWithContext expects a redisContext*, use &ctx_->c
                if (redisInitiateSSLWithContext(&self->ctx_->c, ssl) != REDIS_OK) {
                    REDIS_ERROR_RT(self->log_, "TLS initiate failed: {}", redisSSLContextGetError(ssl_err));
                    redisFreeSSLContext(ssl); // we own it; safe to free now
                    self->on_disconnected(-1);
                    return;
                }
                self->sslctx_ = ssl; // keep it alive until ctx_ is freed
            }
            self->adapter_ = std::make_shared<HiredisAsioAdapter>(self->strand_, self->ctx_);
            self->adapter_->start();
        }
    };
    asio::dispatch(strand_, std::move(attempt));
}

void RedisAsyncConnection::schedule_reconnect() {
    if (stopping_)
        return;
    backoff_ = next_backoff(backoff_, opts_.reconnect_initial, opts_.reconnect_max);
    REDIS_DEBUG_RT(log_, "scheduling reconnect in {} ms", backoff_.count());
    reconnect_timer_.expires_after(backoff_);
    reconnect_timer_.async_wait([w = weak_from_this()](auto ec) {
        if (ec)
            return;
        // stop() may race with a ready timer callback.
        if (auto self = w.lock()) {
            if (self->stopping_)
                return;
            self->do_connect(self->opts_);
        }
    });
}

void RedisAsyncConnection::on_connected() {
    REDIS_DEBUG_RT(log_, "socket connected to {}:{} TLS={}", opts_.host, opts_.port, opts_.tls.use_tls);
    transition_state(ConnectionState::handshaking_resp3, "socket connected");
    send_handshake_hello();
}

void RedisAsyncConnection::on_disconnected(int status) {
    if (status != REDIS_OK && ctx_ && ctx_->errstr)
        REDIS_WARN_RT(log_, "disconnected: status={} errstr={}", status, ctx_ && ctx_->errstr ? ctx_->errstr : "");
    else if (ctx_ && ctx_->errstr)
        REDIS_DEBUG_RT(log_, "disconnected: status={} errstr={}", status, ctx_ && ctx_->errstr ? ctx_->errstr : "");

    if (sslctx_) {
        redisFreeSSLContext(sslctx_); // we own it; safe to free now
        sslctx_ = nullptr;
    }
    if (adapter_)
        adapter_->stop();
    adapter_.reset();
    connected_.store(false, std::memory_order_relaxed);
    hello_summary_.clear();
    if (!stopping_) {
        transition_state(ConnectionState::disconnected, "on_disconnected()");
    }
    ping_timer_.cancel();

    auto d = std::move(disconnect_waiters_);
    disconnect_waiters_.clear();
    for (auto &[id, h] : d) {
        REDIS_TRACE_RT(log_, "notifying disconnect waiter {}", id);
        detail::complete_on_associated(std::move(h), strand_, make_error(error_category::errc::stopped));
    }
    connect_inflight_ = false;
    fail_pre_ready_queue(make_error(error_category::errc::transport_closed));
    // Drop all subscription batons (stops further callbacks referencing freed state)
    ch_batons_.clear();
    pch_batons_.clear();

    if (status != REDIS_OK) {
        health_ = Health::unhealthy;
        ping_failures_ = 0;
        REDIS_ERROR_RT(log_, "connection error, scheduling reconnect: status={}", status);
        schedule_reconnect();
        return;
    }
    pub_channel_.close();
}

void RedisAsyncConnection::handle_connect(const redisAsyncContext *c, int status) {
    auto *self = static_cast<RedisAsyncConnection *>(c->data);
    if (!self)
        return;
    asio::dispatch(self->strand_, [self, c, status] {
        if (self->ctx_ != c)
            return;
        if (status != REDIS_OK) {
            self->ctx_ = nullptr;
            self->on_disconnected(status);
            return;
        }
        self->on_connected();
    });
}

void RedisAsyncConnection::handle_disconnect(const redisAsyncContext *c, int status) {
    auto *self = static_cast<RedisAsyncConnection *>(c->data);
    if (!self)
        return;
    asio::dispatch(self->strand_, [self, c, status] {
        if (self->ctx_ != c)
            return;
        self->ctx_ = nullptr;
        self->on_disconnected(status);
    });
}

static PublishMessage parse_pubsub_reply(redisReply *r) {
    PublishMessage pm;
    if (!r)
        return pm;
    if (r->type == REDIS_REPLY_PUSH && r->elements >= 3) {
        auto *kind = r->element[0];
        if (kind && kind->type == REDIS_REPLY_STRING) {
            std::string_view k{kind->str, static_cast<size_t>(kind->len)};
            if (k == "message" && r->elements >= 3) {
                pm.channel = {r->element[1]->str, static_cast<size_t>(r->element[1]->len)};
                pm.payload = {r->element[2]->str, static_cast<size_t>(r->element[2]->len)};
            } else if (k == "pmessage" && r->elements >= 4) {
                pm.pattern = std::string{r->element[1]->str, static_cast<size_t>(r->element[1]->len)};
                pm.channel = {r->element[2]->str, static_cast<size_t>(r->element[2]->len)};
                pm.payload = {r->element[3]->str, static_cast<size_t>(r->element[3]->len)};
            }
        }
    }
    if (r->type == REDIS_REPLY_ARRAY && r->elements >= 3) {
        auto *kind = r->element[0];
        if (kind && kind->type == REDIS_REPLY_STRING) {
            std::string_view k{kind->str, static_cast<size_t>(kind->len)};
            if (k == "message" && r->elements >= 3) {
                pm.channel = {r->element[1]->str, static_cast<size_t>(r->element[1]->len)};
                pm.payload = {r->element[2]->str, static_cast<size_t>(r->element[2]->len)};
            } else if (k == "pmessage" && r->elements >= 4) {
                pm.pattern = std::string{r->element[1]->str, static_cast<size_t>(r->element[1]->len)};
                pm.channel = {r->element[2]->str, static_cast<size_t>(r->element[2]->len)};
                pm.payload = {r->element[3]->str, static_cast<size_t>(r->element[3]->len)};
            }
        }
    }
    return pm;
}
//
// void RedisAsyncConnection::handle_push(redisAsyncContext *c, void *r) {
//     auto *self = static_cast<RedisAsyncConnection *>(c->data);
//     if (!self)
//         return;
//     redisReply *reply = static_cast<redisReply *>(r);
//     REDIS_ERROR_RT(self->log_, "push received: type={} elements={}", reply->type, reply->elements);
//     auto pm = parse_pubsub_reply(reply);
//     if (!pm.channel.empty() || pm.pattern) {
//         self->pub_channel_.try_send(boost::system::error_code{}, std::move(pm));
//         self->ping_failures_ = 0;
//         self->health_ = Health::healthy;
//     }
// }

void RedisAsyncConnection::send_handshake_hello() {
    if (!ctx_) {
        on_disconnected(-1);
        return;
    }

    // Compose HELLO 3 with optional AUTH + SETNAME in one round-trip
    std::vector<std::string> argv;
    argv.reserve(6);
    argv.emplace_back("HELLO");
    argv.emplace_back("3");
    if (opts_.password) {
        argv.emplace_back("AUTH");
        argv.emplace_back(opts_.username ? *opts_.username : std::string{"default"});
        argv.emplace_back(*opts_.password);
    }
    if (opts_.client_name && !opts_.client_name->empty()) {
        argv.emplace_back("SETNAME");
        argv.emplace_back(*opts_.client_name);
    }

    std::vector<const char *> cargv;
    cargv.reserve(argv.size());
    std::vector<size_t> alen;
    alen.reserve(argv.size());
    for (auto &s : argv) {
        cargv.push_back(s.data());
        alen.push_back(s.size());
    }

    if (async_command_argv_fn_(ctx_,
                               [](redisAsyncContext *c, void *r, void * /*priv*/) {
                                   auto *self = static_cast<RedisAsyncConnection *>(c->data);
                                   if (!self)
                                       return;
                                   if (self->ctx_ != c)
                                       return;

                                   redisReply *reply = static_cast<redisReply *>(r);
                                   std::error_code ec;
                                   std::string summary;
                                   if (!reply) {
                                       ec = make_error(error_category::errc::transport_closed);
                                   } else if (reply->type == REDIS_REPLY_ERROR) {
                                       ec = make_error(error_category::errc::protocol_error);
                                   } else {
                                       RedisValue rv = RedisValue::fromRaw(reply);
                                       if (auto *kv = std::get_if<RedisValue::KVList>(&rv.payload)) {
                                           std::string server;
                                           std::string version;
                                           std::string role;
                                           std::string mode;
                                           long long proto = 0;
                                           for (auto &[k, v] : *kv) {
                                               if (k == "server") {
                                                   if (auto s = string_like(v))
                                                       server = *s;
                                               } else if (k == "version") {
                                                   if (auto s = string_like(v))
                                                       version = *s;
                                               } else if (k == "proto") {
                                                   if (auto p = std::get_if<long long>(&v.payload))
                                                       proto = *p;
                                               } else if (k == "role") {
                                                   if (auto s = string_like(v))
                                                       role = *s;
                                               } else if (k == "mode") {
                                                   if (auto s = string_like(v))
                                                       mode = *s;
                                               }
                                           }
                                           summary = server + " " + version + " proto=" + std::to_string(proto)
                                                     + (role.empty() ? "" : (" role=" + role))
                                                     + (mode.empty() ? "" : (" mode=" + mode));
                                       } else if (auto s = string_like(rv)) {
                                           summary = *s;
                                       }
                                   }
                                   asio::dispatch(self->strand_, [self, ec, summary = std::move(summary)]() mutable {
                                       self->handle_handshake_result(ec, std::move(summary));
                                   });
                               },
                               nullptr,
                               static_cast<int>(cargv.size()),
                               cargv.data(),
                               reinterpret_cast<const size_t *>(alen.data()))
        != REDIS_OK) {
        REDIS_ERROR_RT(log_, "HELLO submit failed");
        handle_handshake_result(make_error(error_category::errc::submission_failed), {});
    }
}

void RedisAsyncConnection::handle_handshake_result(std::error_code ec, std::string summary) {
    if (ec) {
        REDIS_ERROR_RT(log_, "HELLO failed: {}", ec.message());
        on_disconnected(-1);
        return;
    }
    hello_summary_ = std::move(summary);
    connected_.store(true, std::memory_order_relaxed);
    backoff_ = {};
    transition_state(ConnectionState::ready, "HELLO ok");
    REDIS_TRACE_RT(log_, "HELLO ok: {}", hello_summary_);
    restore_subscriptions();
    start_keepalive();
    drain_pre_ready_queue();
    auto waiters = std::move(connect_waiters_);
    connect_waiters_.clear();
    for (auto &[id, h] : waiters) {
        (void)id;
        detail::complete_on_associated(std::move(h), strand_, std::error_code{});
    }
}

void RedisAsyncConnection::restore_subscriptions() {
    // Re-issue SUBSCRIBE & PSUBSCRIBE for all ref-counted subjects
    for (auto &[ch, cnt] : ch_.refc) {
        if (cnt > 0)
            issue_sub("SUBSCRIBE", ch, {});
    }
    for (auto &[p, cnt] : pch_.refc) {
        if (cnt > 0)
            issue_sub("PSUBSCRIBE", p, {});
    }
}

void RedisAsyncConnection::issue_unsub(const char *verb,
                                       std::string_view subject,
                                       asio::any_completion_handler<void(std::error_code)> cb) {
    if (!ctx_) {
        if (cb)
            cb(make_error(error_category::errc::not_connected));
        return;
    }

    const bool is_unsub = ::strcasecmp(verb, "UNSUBSCRIBE") == 0;
    const bool is_punsub = ::strcasecmp(verb, "PUNSUBSCRIBE") == 0;

    // For UNSUB/PUNSUB, create or replace a long-lived baton stored in the map.
    std::unique_ptr<UnsubBaton> baton;
    UnsubBaton *priv = nullptr;
    if (is_unsub || is_punsub) {
        baton = std::make_unique<UnsubBaton>();
        baton->w = weak_from_this();
        baton->on_ack = std::move(cb);
        baton->subject = std::string(subject);
        baton->is_pattern = is_punsub;
        priv = baton.get();
    } else {
        assert(is_unsub || is_punsub); // only UNSUBSCRIBE or UNPSUBSCRIBE supported here
    }
    // Helper: parse first element string of a reply (ARRAY or PUSH)
    // IMPORTANT: For UNSUB/PUNSUB, hiredis invokes THIS callback only on error
    // We keep baton_raw valid in the map until (P)UNSUBSCRIBE is acked (or error).
    assert(baton);
    const char *argvs[2] = {verb, baton->subject.c_str()};
    size_t arglens[2] = {std::strlen(verb), baton->subject.size()};
    if (async_command_argv_fn_(ctx_, &RedisAsyncConnection::handle_unsub_reply, priv, 2, argvs, arglens) != REDIS_OK) {
        REDIS_ERROR_RT(log_, "{} submit failed for '{}'", verb, subject);
        if (baton) {
            auto handler = std::move(baton->on_ack);
            if (handler) {
                detail::complete_on_associated(std::move(handler), strand_, make_error(error_category::errc::submission_failed));
            }
        }
        return;
    }
    const std::string key = baton->subject;
    if (is_punsub)
        pch_unsub_batons_[key] = std::move(baton);
    else if (is_unsub)
        ch_unsub_batons_[key] = std::move(baton);
}

void RedisAsyncConnection::handle_unsub_reply(redisAsyncContext *c, void *r, void *priv) {
    auto *self = c ? static_cast<RedisAsyncConnection *>(c->data) : nullptr;
    redisReply *reply = static_cast<redisReply *>(r);
    const std::string_view kind = reply_first_string(reply);

    if (priv) {
        auto *baton = static_cast<UnsubBaton *>(priv);
        if (auto keep_self = baton->w.lock()) {
            auto *strong_self = keep_self.get();
            if (!reply) {
                asio::dispatch(keep_self->strand_, [self = keep_self, pat = baton->is_pattern, subj = baton->subject]() {
                    if (pat)
                        self->pch_unsub_batons_.erase(subj);
                    else
                        self->ch_unsub_batons_.erase(subj);
                    // TODO Perhaps check the unsub ack?
                });
                return;
            }

            if (kind == "unsubscribe" || kind == "punsubscribe") {
                REDIS_WARN_RT(strong_self->log_, "unsub unsubscribe called... weird....: {}", baton->subject);
                asio::dispatch(keep_self->strand_, [self = keep_self, pat = baton->is_pattern, subj = baton->subject]() {
                    // TODO Perhaps check the unsub ack?
                    if (pat)
                        self->pch_unsub_batons_.erase(subj);
                    else
                        self->ch_unsub_batons_.erase(subj);
                });
                return;
            }

            // Other notifications (ignore)
            return;
        }

        if (self) {
            if (baton->is_pattern)
                self->pch_unsub_batons_.erase(baton->subject);
            else
                self->ch_unsub_batons_.erase(baton->subject);
        }
        return;
    }

    if (self) {
        REDIS_CRITICAL_RT(self->log_, "unsub callback called without baton, kind={}", kind);
    }
}

void RedisAsyncConnection::issue_sub(const char *verb,
                                     std::string_view subject,
                                     asio::any_completion_handler<void(std::error_code)> cb) {
    if (!ctx_) {
        if (cb)
            cb(make_error(error_category::errc::not_connected));
        return;
    }

    const bool is_sub = ::strcasecmp(verb, "SUBSCRIBE") == 0;
    const bool is_psub = ::strcasecmp(verb, "PSUBSCRIBE") == 0;

    // For SUB/PSUB, create or replace a long-lived baton stored in the map.
    std::unique_ptr<SubBaton> baton;
    SubBaton *priv = nullptr;
    if (is_sub || is_psub) {
        baton = std::make_unique<SubBaton>();
        baton->w = weak_from_this();
        baton->on_ack = std::move(cb);
        baton->subject = std::string(subject);
        baton->is_pattern = is_psub;
        priv = baton.get();
    } else {
        assert(is_sub || is_psub); // only SUBSCRIBE or PSUBSCRIBE supported here
    }
    // Helper: parse first element string of a reply (ARRAY or PUSH)

    // IMPORTANT: For SUB/PSUB, hiredis invokes THIS callback repeatedly for publishes.
    // We keep baton_raw valid in the map until (P)UNSUBSCRIBE is acked (or error).
    assert(baton);
    const char *argvs[2] = {verb, baton->subject.c_str()};
    size_t arglens[2] = {std::strlen(verb), baton->subject.size()};
    if (async_command_argv_fn_(ctx_, &RedisAsyncConnection::handle_sub_reply, priv, 2, argvs, arglens) != REDIS_OK) {
        REDIS_ERROR_RT(log_, "{} submit failed for '{}'", verb, subject);
        if (baton) {
            auto handler = std::move(baton->on_ack);
            if (handler) {
                detail::complete_on_associated(std::move(handler), strand_, make_error(error_category::errc::submission_failed));
            }
        }
        return;
    }
    const std::string key = baton->subject;
    if (is_psub)
        pch_batons_[key] = std::move(baton);
    else if (is_sub)
        ch_batons_[key] = std::move(baton);
}

void RedisAsyncConnection::handle_sub_reply(redisAsyncContext *c, void *r, void *priv) {
    auto *self = c ? static_cast<RedisAsyncConnection *>(c->data) : nullptr;
    redisReply *reply = static_cast<redisReply *>(r);
    const std::string_view kind = reply_first_string(reply);

    if (priv) {
        auto *baton = static_cast<SubBaton *>(priv);
        if (auto keep_self = baton->w.lock()) {
            if (!reply) {
                asio::dispatch(keep_self->strand_, [self = keep_self, pat = baton->is_pattern, subj = baton->subject]() {
                    if (pat)
                        self->pch_batons_.erase(subj);
                    else
                        self->ch_batons_.erase(subj);
                    // TODO Perhaps check the unsub ack?
                });
                return;
            }

            if (!baton->acked && (kind == "subscribe" || kind == "psubscribe")) {
                baton->acked = true;
                if (baton->on_ack) {
                    detail::complete_on_associated(std::move(baton->on_ack), keep_self->strand_, std::error_code{});
                }
                return;
            }

            if (kind == "message" || kind == "pmessage") {
                auto pm = parse_pubsub_reply(reply);
                keep_self->pub_channel_.try_send(boost::system::error_code{}, std::move(pm));
                keep_self->ping_failures_ = 0;
                keep_self->health_ = Health::healthy;
                return;
            }

            if (kind == "unsubscribe" || kind == "punsubscribe") {
                asio::dispatch(keep_self->strand_, [self = keep_self, pat = baton->is_pattern, subj = baton->subject]() {
                    // TODO Should this be erased already?
                    if (pat)
                        self->pch_batons_.erase(subj);
                    else
                        self->ch_batons_.erase(subj);

                    if (pat) {
                        auto it = self->pch_unsub_batons_.find(subj);
                        if (it != self->pch_unsub_batons_.end() && it->second && it->second->on_ack && !it->second->acked) {
                            detail::complete_on_associated(std::move(it->second->on_ack), self->strand_, std::error_code{});
                        }
                        self->pch_unsub_batons_.erase(subj);
                    } else {
                        auto it = self->ch_unsub_batons_.find(subj);
                        if (it != self->ch_unsub_batons_.end() && it->second && it->second->on_ack && !it->second->acked) {
                            detail::complete_on_associated(std::move(it->second->on_ack), self->strand_, std::error_code{});
                        }
                        self->ch_unsub_batons_.erase(subj);
                    }
                });
                return;
            }

            // Other notifications (ignore)
            return;
        }

        if (self) {
            if (baton->is_pattern)
                self->pch_batons_.erase(baton->subject);
            else
                self->ch_batons_.erase(baton->subject);
        }
        return;
    }

    if (self) {
        REDIS_CRITICAL_RT(self->log_, "sub callback called without baton, kind={}", kind);
    }
}

void RedisAsyncConnection::start_keepalive() {
    ping_failures_ = 0;
    schedule_next_ping();
}

void RedisAsyncConnection::schedule_next_ping() {
    if (stopping_)
        return;
    auto delay = jitter(opts_.keepalive_period, opts_.keepalive_jitter);
    ping_timer_.expires_after(delay);
    ping_timer_.async_wait([w = weak_from_this()](auto ec) {
        if (ec)
            return; // canceled
        if (auto self = w.lock()) {
            if (!self->ctx_) {
                self->schedule_next_ping();
                return;
            }
            // Send PING and reschedule based on response
            static const char *kPingArgv[] = {"PING"};
            static const size_t kPingArgLen[] = {4};
            if (self->async_command_argv_fn_(self->ctx_,
                                             [](redisAsyncContext *c, void *r, void * /*priv*/) {
                                                 auto *self = static_cast<RedisAsyncConnection *>(c->data);
                                                 if (!self || self->ctx_ != c)
                                                     return;
                                                 std::error_code ec;
                                                 if (!r)
                                                     ec = make_error(error_category::errc::transport_closed);
                                                 asio::dispatch(self->strand_, [self, ec] {
                                                     if (ec) {
                                                         if (++self->ping_failures_ >= 3) {
                                                             REDIS_WARN_RT(self->log_, "ping failed: {}", ec.message());
                                                             self->on_disconnected(-1);
                                                             return;
                                                         }
                                                         self->health_ = Health::suspect;
                                                     } else {
                                                         self->ping_failures_ = 0;
                                                         self->health_ = Health::healthy;
                                                     }
                                                     self->schedule_next_ping();
                                                 });
                                             },
                                             nullptr,
                                             1,
                                             kPingArgv,
                                             kPingArgLen)
                != REDIS_OK) {
                REDIS_WARN_RT(self->log_, "PING submit failed");
                self->on_disconnected(-1);
            }
        }
    });
}

} // namespace redis_asio
