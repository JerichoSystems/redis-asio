#include "hiredis_asio_adapter.hpp"
#include <boost/system/error_code.hpp>
#include <utility>

namespace redis_asio {

HiredisAsioAdapter::HiredisAsioAdapter(executor_type exec, redisAsyncContext *ctx)
    : exec_(exec), ctx_(ctx), fd_(ctx->c.fd), sd_(exec) {
    sd_.assign(fd_);
    ctx_->ev.data = this;
    ctx_->ev.addRead = [](void *privdata) {
        auto adapter = static_cast<HiredisAsioAdapter *>(privdata);
        adapter->enable_read();
    };
    ctx_->ev.delRead = [](void *privdata) {
        auto adapter = static_cast<HiredisAsioAdapter *>(privdata);
        adapter->disable_read();
    };
    ctx_->ev.addWrite = [](void *privdata) {
        auto adapter = static_cast<HiredisAsioAdapter *>(privdata);
        adapter->enable_write();
    };
    ctx_->ev.delWrite = [](void *privdata) {
        auto adapter = static_cast<HiredisAsioAdapter *>(privdata);
        adapter->disable_write();
    };
    ctx_->ev.cleanup = [](void *privdata) {
        auto adapter = static_cast<HiredisAsioAdapter *>(privdata);
        if (adapter)
            adapter->stop();
    };
}

HiredisAsioAdapter::~HiredisAsioAdapter() {
    stop();
    //  Do not close the fd; hiredis owns it. Release to avoid double-close.
    if (sd_.is_open())
        sd_.release();
}

void HiredisAsioAdapter::start() {
    enable_read();
    enable_write();
}

void HiredisAsioAdapter::stop() {
    auto *ctx = std::exchange(ctx_, nullptr);
    reading_ = writing_ = false;
    boost::system::error_code ec;
    sd_.cancel(ec);
    if (sd_.is_open()) {
        (void)sd_.release();
    }
    if (ctx) {
        ctx->ev.addRead = nullptr;
        ctx->ev.delRead = nullptr;
        ctx->ev.addWrite = nullptr;
        ctx->ev.delWrite = nullptr;
        ctx->ev.cleanup = nullptr;
        ctx->ev.data = nullptr;
    }
}

void HiredisAsioAdapter::enable_read() {
    if (!ctx_ || reading_)
        return;
    reading_ = true;
    start_wait_read();
}

void HiredisAsioAdapter::disable_read() { reading_ = false; }

void HiredisAsioAdapter::enable_write() {
    if (!ctx_ || writing_)
        return;
    writing_ = true;
    start_wait_write();
}

void HiredisAsioAdapter::disable_write() { writing_ = false; }

void HiredisAsioAdapter::start_wait_read() {
    if (!reading_)
        return;
    sd_.async_wait(asio::posix::descriptor_base::wait_read, [w = weak_from_this()](auto ec) {
        if (auto self = w.lock()) {
            if (ec || !self->ctx_ || !self->reading_) {
                self->reading_ = false;
                return;
            }
            redisAsyncHandleRead(self->ctx_);
            if (self->reading_)
                self->start_wait_read();
        }
    });
}
void HiredisAsioAdapter::start_wait_write() {
    if (!writing_)
        return;
    sd_.async_wait(asio::posix::descriptor_base::wait_write, [w = weak_from_this()](auto ec) {
        if (auto self = w.lock()) {
            if (ec || !self->ctx_) {
                self->writing_ = false;
                return;
            }
            if (!self->writing_)
                return;
            self->writing_ = false;
            redisAsyncHandleWrite(self->ctx_);
            if (self->writing_)
                self->start_wait_write();
        }
    });
}

} // namespace redis_asio
