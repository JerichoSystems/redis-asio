#pragma once
/**
 * @file hiredis_asio_adapter.hpp
 * @brief Minimal Boost.Asio adapter for hiredis async context.
 */
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <hiredis/async.h>
#include <memory>

namespace redis_asio {
namespace asio = boost::asio;

class HiredisAsioAdapter : public std::enable_shared_from_this<HiredisAsioAdapter> {
  public:
    using executor_type = asio::any_io_executor;

    HiredisAsioAdapter(executor_type exec, redisAsyncContext *ctx);
    ~HiredisAsioAdapter();

    executor_type get_executor() const noexcept { return exec_; }
    void start();
    void stop();

  private:
    void start_wait_read();
    void start_wait_write();

  private:
    executor_type exec_;
    redisAsyncContext *ctx_{};
    int fd_{-1};
    asio::posix::stream_descriptor sd_;
    bool reading_{false};
    bool writing_{false};
};

} // namespace redis_asio
