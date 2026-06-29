<!-- ![CI](https://github.com/jerichosystems/redisasync/actions/workflows/ci.yml/badge.svg) -->
[![CircleCI](https://dl.circleci.com/status-badge/img/gh/JerichoSystems/redis-asio/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/JerichoSystems/redis-asio/tree/main)
# redis_asio

Asynchronous C++ client library for Redis, using Boost.Asio and Hiredis.

## Features
- Modern C++ (C++23)
- Asynchronous API using Boost.Asio
- Hiredis backend
- Example and test suite included
- Optional primary auto-failover monitor for non-cluster Redis/Valkey primary-replica deployments

## Primary Auto-Failover

`ConnectOptions::auto_failover` is disabled by default. When enabled, the connection is treated as a primary-only connection for cluster-mode-disabled Redis/Valkey primary-replica deployments:

```cpp
redis_asio::ConnectOptions opts;
opts.auto_failover.enabled = true;
opts.auto_failover.primary_check_interval = std::chrono::seconds(5);
opts.auto_failover.primary_check_jitter = std::chrono::milliseconds(500);
opts.auto_failover.primary_check_timeout = std::chrono::seconds(2);
```

The connection checks the server role from `HELLO 3`, polls `ROLE` while ready, and schedules a reconnect with the configured reconnect backoff when the role is not `master`, a role probe fails or times out, or a normal command returns a role error such as `READONLY`.

The command that receives `READONLY` is not retried by the library. It completes with `ec == 0` and a `RedisValue` whose type is `RedisValue::Type::Error`, then the connection reconnects so subsequent commands use the primary endpoint. Callers must inspect Redis error replies and apply their own retry policy for commands that are safe to replay.

Automatic replay is intentionally not enabled by default because Redis commands are not uniformly idempotent. If an application wants retries after failover, it should wrap known-safe commands or add explicit per-command retry policy at the application boundary.

### Handling READONLY replies

`async_command()` separates transport/API failures from Redis command replies. A successful `std::error_code` means the client received a Redis reply; it does not mean Redis accepted the command. Redis error replies, including `READONLY`, are returned as `RedisValue::Type::Error`.

When auto-failover is enabled, a `READONLY` reply is treated as a stale-primary signal for the connection. The command that received `READONLY` still completes with that Redis error reply and is not replayed. After that completion, the connection reconnects using the configured reconnect backoff so later commands can land on the primary.

```cpp
using boost::asio::as_tuple;
using boost::asio::use_awaitable;

auto [ec, reply] = co_await conn->async_command({"SET", "k", "v"}, as_tuple(use_awaitable));
if (ec) {
    // Transport, submission, connection-state, or client-side failure.
    co_return;
}

if (reply.type == redis_asio::RedisValue::Type::Error) {
    const auto *message = std::get_if<std::string>(&reply.payload);
    if (message && message->starts_with("READONLY")) {
        // The connection will reconnect when auto-failover is enabled.
        // Retry only if this command is safe for your application to replay.
        co_await conn->async_wait_connected(use_awaitable);
    }
    co_return;
}
```

Application retry wrappers should be explicit and bounded. Retry only commands whose replay semantics are acceptable for the application, usually after `async_wait_connected()` completes. For example, a blind `SET key value` may be safe for some workloads, while `INCR`, queue pops, Lua scripts, transactions, or multi-command workflows can duplicate effects or violate higher-level invariants if replayed automatically.

Keep role polling enabled for primary-bound connections even if write commands are retried by the application. Some traffic, especially Pub/Sub flows, may not reliably surface a stale-primary problem through a write error, so the background `ROLE` check is still part of failover detection.

## Integration Tests

Redis-backed tests self-provision Valkey containers through Docker by default. The test process starts and reuses a single-node Valkey container for normal Redis integration coverage, starts a native-TLS Valkey container for encrypted connection coverage, and starts an isolated two-node primary/replica Valkey topology for auto-failover tests.

If Docker is unavailable, Docker-backed tests are skipped. To debug against a manually managed Redis/Valkey instance instead, set `REDIS_ASIO_USE_EXTERNAL_REDIS=1` plus the `REDIS_*` environment variables from `tests/redis_async_tests.cpp`.

For socket/TLS parity, run the Redis async suite once normally and once against the managed TLS Valkey runtime:

```sh
ctest --test-dir build --output-on-failure -R '^redis_async_tests$'
REDIS_ASIO_TEST_TLS=1 ctest --test-dir build --output-on-failure -R '^redis_async_tests$'
```

The failover tests use a suite-owned stable TCP proxy endpoint in front of the current primary. This lets existing connections remain pinned to the demoted node while new reconnects land on the promoted node, exercising real `READONLY` handling and `ROLE` polling behavior without depending on an external Redis service.

TLS tests use native Valkey TLS with short-lived test certificates generated by `openssl`; the old stunnel compose file remains useful for manual debugging but is not required by the automated suite.

## Requirements
- CMake >= 3.21
- C++23 compiler (GCC >= 13, Clang >= 16, MSVC >= 2022)
- Boost (Asio)
- Hiredis & Hiredis SSL
- GTest (for tests)
- Docker (for Redis/Valkey-backed tests)
- OpenSSL CLI (for TLS test certificate generation)

## Coverage

- Local run: `cmake -S . -B build -DREDIS_ASIO_BUILD_TESTS=ON -DREDIS_ASIO_ENABLE_COVERAGE=ON`, then `cmake --build build --target coverage`. Reports appear in `build/coverage/index.html` and `coverage.xml`.
- CI: the CircleCI Linux/macOS Debug jobs always publish the same HTML/XML coverage artifacts; grab the latest run from the [CircleCI pipelines page](https://app.circleci.com/pipelines/github/JerichoSystems/redis-asio?branch=main) and download `coverage/index.html`.
- benchmark (for benches)

All dependencies can be installed via vcpkg or your system package manager.

## Building

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Running Examples

After building, run the example(s) from the build directory:

```sh
./build/psub_async
```

## Running Tests

To build and run the tests:

```sh
cmake -B build -DREDIS_ASIO_BUILD_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

The test executables are not installed by default. To package them alongside the library, enable both
`REDIS_ASIO_BUILD_TESTS` and `REDIS_ASIO_INSTALL_TESTS` when configuring CMake.

## License

MIT License. See [`LICENSE`](LICENSE) file.
