#include "hiredis_asio_adapter.hpp"
#include <boost/system/error_code.hpp>
#include <cassert>

namespace redis_asio {

HiredisAsioAdapter::HiredisAsioAdapter(executor_type exec, redisAsyncContext *ctx)
    : exec_(exec), ctx_(ctx), fd_(ctx->c.fd), sd_(exec) {
    sd_.assign(fd_);
}

HiredisAsioAdapter::~HiredisAsioAdapter() {
    //      stop();
    //    reading_ = writing_ = false;
    //    boost::system::error_code ec; sd_.cancel(ec);
    //  Do not close the fd; hiredis owns it. Release to avoid double-close.
    sd_.release();
}

void HiredisAsioAdapter::start() {
    start_wait_read();
    start_wait_write();
}

void HiredisAsioAdapter::stop() {
    reading_ = writing_ = false;
    boost::system::error_code ec;
    sd_.cancel(ec);
    ctx_ = nullptr; // Hiredis will handle cleanup
}

void HiredisAsioAdapter::start_wait_read() {
    reading_ = true;
    sd_.async_wait(asio::posix::descriptor_base::wait_read, [w = weak_from_this()](auto ec) {
        if (auto self = w.lock()) {
            if (ec || !self->ctx_) {
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
    writing_ = true;
    sd_.async_wait(asio::posix::descriptor_base::wait_write, [w = weak_from_this()](auto ec) {
        if (auto self = w.lock()) {
            if (ec || !self->ctx_) {
                self->writing_ = false;
                return;
            }
            redisAsyncHandleWrite(self->ctx_);
            if (self->writing_)
                self->start_wait_write();
        }
    });
}

} // namespace redis_asio
