#pragma once
#include <format>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

// =============================================================
// redis_log.hpp (modernized, runtime-macro source-location)
//
// Features
//  - Type-safe formatting via std::format (single implementation)
//  - No formatting cost unless the level is enabled
//  - Optional compile-time log-level gating (strip low levels)
//  - Level helpers (tracef/debugf/...) + optional macro sugar
//  - Accepts Logger* or std::shared_ptr<Logger>
//
// Configuration macros (define before including):
//   REDIS_LOG_MIN_LEVEL
//       Compile-time strip below this level (enum value).
//       Default: ::redis_asio::Logger::Level::trace
//
//   REDIS_LOG_MIN_LEVEL_I
//       Integer mirror of REDIS_LOG_MIN_LEVEL for preprocessor gating of macros
//       (0=trace,1=debug,2=info,3=warn,4=err,5=critical,6=off). Defaults to 0.
//       Define BOTH when you want macros to be true no-ops without evaluating args.
//
//   REDIS_LOG_DISABLE_MACROS
//       If defined, level macros (REDIS_TRACE/...) expand to no-ops ((void)0)
//       instead of forwarding to the functions.
// =============================================================

namespace redis_asio {

struct Logger {
    // Based on spdlog's level names/order.
    enum class Level { trace,
                       debug,
                       info,
                       warn,
                       err,
                       critical,
                       off };
    virtual ~Logger() = default;
    virtual void log(Level lvl, std::string_view msg) noexcept = 0;
    virtual bool should_log(Level lvl) const noexcept { return lvl != Level::off; }
};

// Factories (provide your own implementations elsewhere)
std::shared_ptr<Logger> make_null_logger();
std::shared_ptr<Logger> make_clog_logger(Logger::Level min_level = Logger::Level::info,
                                         std::string name = "redis_asio");

// ---------- configuration defaults ----------
#ifndef REDIS_LOG_MIN_LEVEL
    #define REDIS_LOG_MIN_LEVEL ::redis_asio::Logger::Level::trace
#endif

// Integer mirror for preprocessor (#if) gating of macros.
#ifndef REDIS_LOG_MIN_LEVEL_I
    #define REDIS_LOG_MIN_LEVEL_I 0 /* trace */
#endif

// Numeric level mapping (keep in sync with Logger::Level)
#define REDIS_LOG_LVL_TRACE 0
#define REDIS_LOG_LVL_DEBUG 1
#define REDIS_LOG_LVL_INFO 2
#define REDIS_LOG_LVL_WARN 3
#define REDIS_LOG_LVL_ERR 4
#define REDIS_LOG_LVL_CRITICAL 5
#define REDIS_LOG_LVL_OFF 6

// Sanity: ensure enum order matches the numeric macros (compile-time checks)
static_assert(static_cast<int>(Logger::Level::trace) == REDIS_LOG_LVL_TRACE);
static_assert(static_cast<int>(Logger::Level::debug) == REDIS_LOG_LVL_DEBUG);
static_assert(static_cast<int>(Logger::Level::info) == REDIS_LOG_LVL_INFO);
static_assert(static_cast<int>(Logger::Level::warn) == REDIS_LOG_LVL_WARN);
static_assert(static_cast<int>(Logger::Level::err) == REDIS_LOG_LVL_ERR);
static_assert(static_cast<int>(Logger::Level::critical) == REDIS_LOG_LVL_CRITICAL);
static_assert(static_cast<int>(Logger::Level::off) == REDIS_LOG_LVL_OFF);

// Location prefixing threshold for *runtime* macros (*_RT)
// Default: print file:line for WARN and above.
#ifndef REDIS_LOG_LOC_MIN_LEVEL_I
    #define REDIS_LOG_LOC_MIN_LEVEL_I 3 /* warn */
#endif
// Optional toggles:
//   REDIS_LOG_LOC_SHOW_FUNC   -> include function name
//   REDIS_LOG_LOC_SHOW_COLUMN -> include column number

// ---------- small utilities ----------
inline Logger *to_ptr(Logger *p) noexcept { return p; }
inline Logger *to_ptr(const std::shared_ptr<Logger> &sp) noexcept { return sp.get(); }

constexpr int to_int(Logger::Level l) noexcept { return static_cast<int>(l); }

template <Logger::Level L>
inline constexpr bool level_enabled_v = (to_int(L) >= to_int(REDIS_LOG_MIN_LEVEL));

// ---------- core implementation ----------
namespace detail {

template <class LoggerHolder, class... Args>
inline void logf_impl(LoggerHolder &&holder,
                      Logger::Level lvl,
                      std::string_view fmt,
                      Args &&...args) noexcept {
    Logger *lg = to_ptr(holder);
    if (!lg || !lg->should_log(lvl))
        return;

    try {
        // NOTE: std::make_format_args in libstdc++ expects lvalues.
        // Pass named parameters as lvalues (no std::forward here).
        std::string msg = std::vformat(fmt, std::make_format_args(args...));
        lg->log(lvl, msg);
    } catch (...) {
        // Keep noexcept boundary: swallow formatting errors.
    }
}

// helper: optionally prefix location (only for runtime macros)
inline std::string maybe_prefix_location(std::string msg,
                                         Logger::Level lvl,
                                         const std::source_location &loc) {
    if (static_cast<int>(lvl) < REDIS_LOG_LOC_MIN_LEVEL_I)
        return msg;
    try {
#ifdef REDIS_LOG_LOC_SHOW_FUNC
    #ifdef REDIS_LOG_LOC_SHOW_COLUMN
        return std::format("{}:{}:{} {}: {}", loc.file_name(), loc.line(), loc.column(), loc.function_name(), msg);
    #else
        return std::format("{}:{} {}: {}", loc.file_name(), loc.line(), loc.function_name(), msg);
    #endif
#else
    #ifdef REDIS_LOG_LOC_SHOW_COLUMN
        return std::format("{}:{}:{}: {}", loc.file_name(), loc.line(), loc.column(), msg);
    #else
        return std::format("{}:{}: {}", loc.file_name(), loc.line(), msg);
    #endif
#endif
    } catch (...) {
        return msg;
    }
}

// MVP: runtime-format entry point using std::format_args + source_location
inline void logf_rt_impl(Logger *lg,
                         Logger::Level lvl,
                         std::string_view fmt,
                         std::format_args args,
                         const std::source_location &loc) noexcept {
    if (!lg || !lg->should_log(lvl))
        return;
    try {
        std::string msg = std::vformat(fmt, args);
        lg->log(lvl, maybe_prefix_location(std::move(msg), lvl, loc));
    } catch (...) {
        // swallow formatting errors to keep noexcept
    }
}

// Pack-based helper that safely accepts rvalues by keeping lvalues alive
// until vformat() is called.
template <class LoggerHolder, class... Args>
inline void logf_rt_pack(LoggerHolder &&holder,
                         Logger::Level lvl,
                         std::string_view fmt,
                         const std::source_location &loc,
                         Args &&...args) noexcept {
    Logger *lg = to_ptr(holder);
    if (!lg || !lg->should_log(lvl))
        return;
    try {
        auto tup = std::forward_as_tuple(std::forward<Args>(args)...);
        std::string msg;
        std::apply([&](auto &...elems) {
            msg = std::vformat(fmt, std::make_format_args(elems...));
        },
                   tup);
        lg->log(lvl, maybe_prefix_location(std::move(msg), lvl, loc));
    } catch (...) {
        // swallow formatting errors to keep noexcept
    }
}

} // namespace detail

// Runtime-format API wrappers (non-ambiguous names)

// 1) Low-level: accept prebuilt std::format_args (callers must ensure lvalues)
template <class LoggerHolder>
inline void logf_rt(LoggerHolder &&holder,
                    Logger::Level lvl,
                    std::string_view fmt,
                    std::format_args args,
                    const std::source_location &loc = std::source_location::current()) noexcept {
    detail::logf_rt_impl(to_ptr(holder), lvl, fmt, args, loc);
}

// 2) Convenience: accept arbitrary values (including rvalues). loc BEFORE args to avoid
//    ambiguity with the overload above.
template <class LoggerHolder, class... Args>
inline void logf_rt(LoggerHolder &&holder,
                    Logger::Level lvl,
                    std::string_view fmt,
                    const std::source_location &loc,
                    Args &&...args) noexcept {
    detail::logf_rt_pack(std::forward<LoggerHolder>(holder), lvl, fmt, loc,
                         std::forward<Args>(args)...);
}

// Level-specific runtime wrappers
template <class LoggerHolder, class... Args>
inline void tracef_rt(LoggerHolder &&holder,
                      std::string_view fmt,
                      const std::source_location &loc,
                      Args &&...args) noexcept {
    logf_rt(std::forward<LoggerHolder>(holder), Logger::Level::trace, fmt, loc,
            std::forward<Args>(args)...);
}

template <class LoggerHolder, class... Args>
inline void debugf_rt(LoggerHolder &&holder,
                      std::string_view fmt,
                      const std::source_location &loc,
                      Args &&...args) noexcept {
    logf_rt(std::forward<LoggerHolder>(holder), Logger::Level::debug, fmt, loc,
            std::forward<Args>(args)...);
}

template <class LoggerHolder, class... Args>
inline void infof_rt(LoggerHolder &&holder,
                     std::string_view fmt,
                     const std::source_location &loc,
                     Args &&...args) noexcept {
    logf_rt(std::forward<LoggerHolder>(holder), Logger::Level::info, fmt, loc,
            std::forward<Args>(args)...);
}

template <class LoggerHolder, class... Args>
inline void warnf_rt(LoggerHolder &&holder,
                     std::string_view fmt,
                     const std::source_location &loc,
                     Args &&...args) noexcept {
    logf_rt(std::forward<LoggerHolder>(holder), Logger::Level::warn, fmt, loc,
            std::forward<Args>(args)...);
}

template <class LoggerHolder, class... Args>
inline void errorf_rt(LoggerHolder &&holder,
                      std::string_view fmt,
                      const std::source_location &loc,
                      Args &&...args) noexcept {
    logf_rt(std::forward<LoggerHolder>(holder), Logger::Level::err, fmt, loc,
            std::forward<Args>(args)...);
}

template <class LoggerHolder, class... Args>
inline void criticalf_rt(LoggerHolder &&holder,
                         std::string_view fmt,
                         const std::source_location &loc,
                         Args &&...args) noexcept {
    logf_rt(std::forward<LoggerHolder>(holder), Logger::Level::critical, fmt, loc,
            std::forward<Args>(args)...);
}

// ---------- generic dynamic-level API ----------

template <class LoggerHolder, class... Args>
inline void logf(LoggerHolder &&holder,
                 Logger::Level lvl,
                 std::string_view fmt,
                 Args &&...args) noexcept {
    detail::logf_impl(std::forward<LoggerHolder>(holder), lvl, fmt,
                      std::forward<Args>(args)...);
}

// ---------- level-specific helpers (compile-time gated) ----------

template <class LoggerHolder, class... Args>
inline void tracef(LoggerHolder &&holder,
                   std::string_view fmt,
                   Args &&...args) noexcept {
    if constexpr (level_enabled_v<Logger::Level::trace>) {
        detail::logf_impl(std::forward<LoggerHolder>(holder), Logger::Level::trace, fmt,
                          std::forward<Args>(args)...);
    } else {
        (void)holder;
        (void)fmt;
        (void)sizeof...(args);
    }
}

template <class LoggerHolder, class... Args>
inline void debugf(LoggerHolder &&holder,
                   std::string_view fmt,
                   Args &&...args) noexcept {
    if constexpr (level_enabled_v<Logger::Level::debug>) {
        detail::logf_impl(std::forward<LoggerHolder>(holder), Logger::Level::debug, fmt,
                          std::forward<Args>(args)...);
    } else {
        (void)holder;
        (void)fmt;
        (void)sizeof...(args);
    }
}

template <class LoggerHolder, class... Args>
inline void infof(LoggerHolder &&holder,
                  std::string_view fmt,
                  Args &&...args) noexcept {
    if constexpr (level_enabled_v<Logger::Level::info>) {
        detail::logf_impl(std::forward<LoggerHolder>(holder), Logger::Level::info, fmt,
                          std::forward<Args>(args)...);
    } else {
        (void)holder;
        (void)fmt;
        (void)sizeof...(args);
    }
}

template <class LoggerHolder, class... Args>
inline void warnf(LoggerHolder &&holder,
                  std::string_view fmt,
                  Args &&...args) noexcept {
    if constexpr (level_enabled_v<Logger::Level::warn>) {
        detail::logf_impl(std::forward<LoggerHolder>(holder), Logger::Level::warn, fmt,
                          std::forward<Args>(args)...);
    } else {
        (void)holder;
        (void)fmt;
        (void)sizeof...(args);
    }
}

template <class LoggerHolder, class... Args>
inline void errorf(LoggerHolder &&holder,
                   std::string_view fmt,
                   Args &&...args) noexcept {
    if constexpr (level_enabled_v<Logger::Level::err>) {
        detail::logf_impl(std::forward<LoggerHolder>(holder), Logger::Level::err, fmt,
                          std::forward<Args>(args)...);
    } else {
        (void)holder;
        (void)fmt;
        (void)sizeof...(args);
    }
}

template <class LoggerHolder, class... Args>
inline void criticalf(LoggerHolder &&holder,
                      std::string_view fmt,
                      Args &&...args) noexcept {
    if constexpr (level_enabled_v<Logger::Level::critical>) {
        detail::logf_impl(std::forward<LoggerHolder>(holder), Logger::Level::critical, fmt,
                          std::forward<Args>(args)...);
    } else {
        (void)holder;
        (void)fmt;
        (void)sizeof...(args);
    }
}

// ---------- macro sugar (always defined; can be no-ops) ----------
// These macros preserve call-site style and can be compiled to true no-ops
// without evaluating arguments when the level is below REDIS_LOG_MIN_LEVEL_I.
// If REDIS_LOG_DISABLE_MACROS is set, ALL macros are no-ops regardless of level.

#ifdef REDIS_LOG_DISABLE_MACROS

    #define REDIS_LOGF(logger, lvl, fmt, ...) ((void)0)
    #define REDIS_TRACE(logger, fmt, ...) ((void)0)
    #define REDIS_DEBUG(logger, fmt, ...) ((void)0)
    #define REDIS_INFO(logger, fmt, ...) ((void)0)
    #define REDIS_WARN(logger, fmt, ...) ((void)0)
    #define REDIS_ERROR(logger, fmt, ...) ((void)0)
    #define REDIS_CRITICAL(logger, fmt, ...) ((void)0)

#else

    #define REDIS_LOGF(logger, lvl, fmt, ...) \
        ::redis_asio::logf((logger), (lvl), (fmt)__VA_OPT__(, __VA_ARGS__))

    #if REDIS_LOG_LVL_TRACE >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_TRACE(logger, fmt, ...) \
            ::redis_asio::tracef((logger), (fmt)__VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_TRACE(logger, fmt, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_DEBUG >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_DEBUG(logger, fmt, ...) \
            ::redis_asio::debugf((logger), (fmt)__VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_DEBUG(logger, fmt, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_INFO >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_INFO(logger, fmt, ...) \
            ::redis_asio::infof((logger), (fmt)__VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_INFO(logger, fmt, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_WARN >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_WARN(logger, fmt, ...) \
            ::redis_asio::warnf((logger), (fmt)__VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_WARN(logger, fmt, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_ERR >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_ERROR(logger, fmt, ...) \
            ::redis_asio::errorf((logger), (fmt)__VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_ERROR(logger, fmt, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_CRITICAL >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_CRITICAL(logger, fmt, ...) \
            ::redis_asio::criticalf((logger), (fmt)__VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_CRITICAL(logger, fmt, ...) ((void)0)
    #endif

#endif // REDIS_LOG_DISABLE_MACROS

// ---------- runtime-format macro sugar (MVP) ----------
// These let you pass a runtime std::string/std::string_view as the format string
// while still expanding arguments safely (including rvalues).
#ifdef REDIS_LOG_DISABLE_MACROS
    #define REDIS_LOGF_RT(logger, lvl, fmt_sv, ...) ((void)0)
    #define REDIS_TRACE_RT(logger, fmt_sv, ...) ((void)0)
    #define REDIS_DEBUG_RT(logger, fmt_sv, ...) ((void)0)
    #define REDIS_INFO_RT(logger, fmt_sv, ...) ((void)0)
    #define REDIS_WARN_RT(logger, fmt_sv, ...) ((void)0)
    #define REDIS_ERROR_RT(logger, fmt_sv, ...) ((void)0)
    #define REDIS_CRITICAL_RT(logger, fmt_sv, ...) ((void)0)
#else
    #define REDIS_LOGF_RT(logger, lvl, fmt_sv, ...) \
        ::redis_asio::logf_rt((logger), (lvl), (fmt_sv), std::source_location::current() __VA_OPT__(, __VA_ARGS__))

    #if REDIS_LOG_LVL_TRACE >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_TRACE_RT(logger, fmt_sv, ...) \
            ::redis_asio::tracef_rt((logger), (fmt_sv), std::source_location::current() __VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_TRACE_RT(logger, fmt_sv, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_DEBUG >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_DEBUG_RT(logger, fmt_sv, ...) \
            ::redis_asio::debugf_rt((logger), (fmt_sv), std::source_location::current() __VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_DEBUG_RT(logger, fmt_sv, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_INFO >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_INFO_RT(logger, fmt_sv, ...) \
            ::redis_asio::infof_rt((logger), (fmt_sv), std::source_location::current() __VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_INFO_RT(logger, fmt_sv, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_WARN >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_WARN_RT(logger, fmt_sv, ...) \
            ::redis_asio::warnf_rt((logger), (fmt_sv), std::source_location::current() __VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_WARN_RT(logger, fmt_sv, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_ERR >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_ERROR_RT(logger, fmt_sv, ...) \
            ::redis_asio::errorf_rt((logger), (fmt_sv), std::source_location::current() __VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_ERROR_RT(logger, fmt_sv, ...) ((void)0)
    #endif

    #if REDIS_LOG_LVL_CRITICAL >= REDIS_LOG_MIN_LEVEL_I
        #define REDIS_CRITICAL_RT(logger, fmt_sv, ...) \
            ::redis_asio::criticalf_rt((logger), (fmt_sv), std::source_location::current() __VA_OPT__(, __VA_ARGS__))
    #else
        #define REDIS_CRITICAL_RT(logger, fmt_sv, ...) ((void)0)
    #endif
#endif // runtime-format macros

} // namespace redis_asio
