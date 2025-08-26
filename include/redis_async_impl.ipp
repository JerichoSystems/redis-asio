#pragma once

#include "redis_async.hpp"

// Implementations for templates that must remain header-visible

template <typename CompletionToken>
auto RedisAsyncConnection::async_connect(ConnectOptions opts, CompletionToken &&token) {
    using Sig = void(std::error_code, bool);
    return asio::async_initiate<CompletionToken, Sig>(
        [w = weak_from_this(), opts = std::move(opts)](auto handler) mutable {
            if (auto self = w.lock()) {
                asio::dispatch(self->strand_, [self, opts = std::move(opts), handler = std::move(handler)]() mutable {
                    auto slot = asio::get_associated_cancellation_slot(handler);
                    if (self->is_connected()) {
                        // complete on handler's associated executor, not on our strand
                        detail::complete_on_associated(std::move(handler), self->strand_, std::error_code{}, true);
                        return;
                    }
                    // store a wrapper that will later complete on the handler's executor
                    auto ex = self->strand_;
                    auto id = self->add_connect_waiter([h = std::move(handler), ex](std::error_code ec) mutable {
                        detail::complete_on_associated(std::move(h), ex, ec, false);
                    });
                    if (self->connect_inflight_)
                        return;
                    self->connect_inflight_ = true;
                    self->do_connect(std::move(opts));

                    // Wire user cancellation → erase + complete aborted
                    if (slot.is_connected() && !slot.has_handler()) {
                        slot.assign([w = self->weak_from_this(), id](asio::cancellation_type_t t) {
                            if (t == asio::cancellation_type::none)
                                return;
                            if (auto s = w.lock()) {
                                asio::dispatch(s->strand_, [s, id]() mutable {
                                    if (auto oh = s->erase_connect_waiter(id)) {
                                        detail::complete_on_associated(
                                            std::move(oh), s->strand_, make_error(error_category::errc::stopped));
                                    }
                                });
                            }
                        });
                    }
                });
            } else {
                // Connection object already destroyed; complete with operation_aborted.
                detail::complete_on_associated(std::move(handler), boost::asio::system_executor{}, make_error(error_category::errc::stopped), false);
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
                    auto slot = asio::get_associated_cancellation_slot(handler);
                    if (self->is_connected()) {
                        detail::complete_on_associated(std::move(handler), self->strand_, std::error_code{});
                        return;
                    }
                    if (self->stopping_) {
                        detail::complete_on_associated(std::move(handler), self->strand_, make_error(error_category::errc::stopped));
                        return;
                    }
                    auto ex = self->strand_;
                    auto id = self->add_connect_waiter([h = std::move(handler), ex](std::error_code ec) mutable {
                        detail::complete_on_associated(std::move(h), ex, ec);
                    });
                    // Wire user cancellation → erase + complete aborted
                    if (slot.is_connected() && !slot.has_handler()) {
                        slot.assign([w = self->weak_from_this(), id](asio::cancellation_type_t t) {
                            if (t == asio::cancellation_type::none)
                                return;
                            if (auto s = w.lock()) {
                                asio::dispatch(s->strand_, [s, id]() mutable {
                                    if (auto oh = s->erase_connect_waiter(id)) {
                                        detail::complete_on_associated(
                                            std::move(oh), s->strand_, make_error(error_category::errc::stopped));
                                    }
                                });
                            }
                        });
                    }
                });
            } else {
                // Connection object already destroyed; complete with operation_aborted.
                detail::complete_on_associated(std::move(handler), boost::asio::system_executor{}, make_error(error_category::errc::stopped));
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
                    auto slot = asio::get_associated_cancellation_slot(handler);
                    if (!self->is_connected()) {
                        detail::complete_on_associated(std::move(handler), self->strand_, std::error_code{});
                        return;
                    }
                    if (self->stopping_) {
                        detail::complete_on_associated(std::move(handler), self->strand_, make_error(error_category::errc::stopped));
                        return;
                    }
                    auto ex = self->strand_;
                    auto id = self->add_disconnect_waiter([h = std::move(handler), ex](std::error_code ec) mutable {
                        detail::complete_on_associated(std::move(h), ex, ec);
                    });

                    // Wire user cancellation → erase + complete aborted
                    if (slot.is_connected() && !slot.has_handler()) {
                        slot.assign([w = self->weak_from_this(), id](asio::cancellation_type_t t) {
                            if (t == asio::cancellation_type::none)
                                return;
                            if (auto s = w.lock()) {
                                asio::dispatch(s->strand_, [s, id]() mutable {
                                    if (auto oh = s->erase_disconnect_waiter(id)) {
                                        detail::complete_on_associated(
                                            std::move(oh), s->strand_, make_error(error_category::errc::stopped));
                                    }
                                });
                            }
                        });
                    }
                });
            } else {
                // Connection object already destroyed; complete with operation_aborted.
                detail::complete_on_associated(std::move(handler), boost::asio::system_executor{}, make_error(error_category::errc::stopped));
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
                    auto ex_fallback = self->strand_;
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
            } else {
                // Connection object already destroyed; complete with operation_aborted.
                detail::complete_on_associated(std::move(handler), boost::asio::system_executor{}, make_error(error_category::errc::stopped));
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
                    auto ex_fallback = self->strand_;
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
            } else {
                // Connection object already destroyed; complete with operation_aborted.
                detail::complete_on_associated(std::move(handler), boost::asio::system_executor{}, make_error(error_category::errc::stopped));
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
                    auto ex_fallback = self->strand_;
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
            } else {
                // Connection object already destroyed; complete with operation_aborted.
                detail::complete_on_associated(std::move(handler), boost::asio::system_executor{}, make_error(error_category::errc::stopped));
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
                    auto ex_fallback = self->strand_;
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
            } else {
                // Connection object already destroyed; complete with operation_aborted.
                detail::complete_on_associated(std::move(handler), boost::asio::system_executor{}, make_error(error_category::errc::stopped));
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
                    auto ex_fallback = self->strand_;
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
            } else {
                // Connection object already destroyed; complete with operation_aborted.
                detail::complete_on_associated(std::move(handler), boost::asio::system_executor{}, make_error(error_category::errc::stopped), RedisValue{});
            }
        },
        token);
}