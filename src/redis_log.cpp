#include "redis_log.hpp"

#include <atomic>
#include <cstdlib> // std::getenv
#include <iostream>
#include <mutex> // once_flag, mutex (fallback sink)
#include <string>
#include <string_view>

#if defined(__has_include)
    #if __has_include(<syncstream>)
        #include <syncstream>
        #if defined(__cpp_lib_syncbuf) && (__cpp_lib_syncbuf >= 201803L)
            #define REDIS_HAVE_SYNCBUF 1
        #endif
    #endif
#endif

#if defined(_WIN32)
    #include <io.h>
    #include <windows.h>
    #define ISATTY _isatty
    #define FILENO _fileno
#else
    #include <unistd.h>
    #define ISATTY isatty
    #define FILENO fileno
#endif

namespace redis_asio {
namespace {

// --- small sink that guarantees atomic writes without building a std::string ---
struct Sink {
    template <class F>
    void write(F &&f) noexcept {
#if defined(REDIS_HAVE_SYNCBUF)
        std::osyncstream out(std::clog); // atomic chunking
        f(out);
        // flush on ~osyncstream
#else
        static std::mutex m;
        std::lock_guard<std::mutex> lk(m);
        f(std::clog);
        std::clog.flush();
#endif
    }
};

static constexpr std::string_view level_name(Logger::Level l) noexcept {
    switch (l) {
    case Logger::Level::trace:
        return "trace";
    case Logger::Level::debug:
        return "debug";
    case Logger::Level::info:
        return "info";
    case Logger::Level::warn:
        return "warn";
    case Logger::Level::err:
        return "err";
    case Logger::Level::critical:
        return "critical";
    case Logger::Level::off:
        return "off";
    }
    return "unknown";
}

struct NullLogger final : Logger {
    void log(Level, std::string_view) noexcept override {}
    bool should_log(Level) const noexcept override { return false; }
};

struct ClogLogger final : Logger {
    explicit ClogLogger(Level min, std::string name)
        : min_(min), name_(std::move(name)), color_(should_colorize()) {
#if defined(_WIN32)
        if (color_)
            enable_win_vt();
#endif
    }

    void log(Level lvl, std::string_view msg) noexcept override {
        if (lvl < min_.load(std::memory_order_relaxed))
            return;

        sink_.write([&](std::ostream &out) {
            if (color_) {
                const auto c = color_codes(lvl);
                out << '[' << name_ << "] "
                    << c.open << '[' << level_name(lvl) << ']' << c.close
                    << ' ' << msg << '\n';
            } else {
                out << '[' << name_ << "] [" << level_name(lvl) << "] " << msg << '\n';
            }
        });
    }

    bool should_log(Level lvl) const noexcept override {
        return lvl >= min_.load(std::memory_order_relaxed);
    }

    void set_min(Level l) noexcept { min_.store(l, std::memory_order_relaxed); }

    // --- color support ---
    static bool should_colorize() noexcept {
#ifdef REDIS_LOG_FORCE_COLOR
        return true;
#endif
#ifdef REDIS_LOG_DISABLE_COLOR
        return false;
#endif
        if (std::getenv("NO_COLOR"))
            return false; // https://no-color.org/
        bool tty = ISATTY(FILENO(stderr));
        if (!tty)
            tty = ISATTY(FILENO(stdout));
        const char *term = std::getenv("TERM");
        if (term && std::string_view(term) == "dumb")
            return false;
        return tty;
    }

    struct AnsiPair {
        const char *open;
        const char *close;
    };
    static constexpr AnsiPair color_codes(Level lvl) noexcept {
        switch (lvl) {
        case Level::trace:
            return {"\x1b[2m", "\x1b[0m"}; // dim
        case Level::debug:
            return {"\x1b[36m", "\x1b[0m"}; // cyan
        case Level::info:
            return {"\x1b[32m", "\x1b[0m"}; // green
        case Level::warn:
            return {"\x1b[33m", "\x1b[0m"}; // yellow
        case Level::err:
            return {"\x1b[31m", "\x1b[0m"}; // red
        case Level::critical:
            return {"\x1b[1;31m", "\x1b[0m"}; // bold red
        case Level::off:
            break;
        }
        return {"", ""};
    }

#if defined(_WIN32)
    static void enable_win_vt() noexcept {
        static std::once_flag once;
        std::call_once(once, [] {
            HANDLE h = GetStdHandle(STD_ERROR_HANDLE);
            if (h == INVALID_HANDLE_VALUE)
                return;
            DWORD mode = 0;
            if (!GetConsoleMode(h, &mode))
                return;
            mode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
            SetConsoleMode(h, mode);
        });
    }
#endif

    Sink sink_;
    std::atomic<Level> min_;
    std::string name_;
    bool color_;
};

} // namespace

std::shared_ptr<Logger> make_null_logger() {
    static auto s = std::make_shared<NullLogger>();
    return s;
}

std::shared_ptr<Logger> make_clog_logger(Logger::Level min_level, std::string name) {
    return std::make_shared<ClogLogger>(min_level, std::move(name));
}

} // namespace redis_asio