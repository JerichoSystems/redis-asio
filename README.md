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

## Integration Tests

Redis-backed tests self-provision Valkey containers through Docker by default. The test process starts and reuses a single-node Valkey container for normal Redis integration coverage, and starts an isolated two-node primary/replica Valkey topology for auto-failover tests.

If Docker is unavailable, Docker-backed tests are skipped. To debug against a manually managed Redis/Valkey instance instead, set `REDIS_ASIO_USE_EXTERNAL_REDIS=1` plus the `REDIS_*` environment variables from `tests/redis_async_tests.cpp`.

The failover tests use a suite-owned stable TCP proxy endpoint in front of the current primary. This lets existing connections remain pinned to the demoted node while new reconnects land on the promoted node, exercising real `READONLY` handling and `ROLE` polling behavior without depending on an external Redis service.

## Requirements
- CMake >= 3.21
- C++23 compiler (GCC >= 13, Clang >= 16, MSVC >= 2022)
- Boost (Asio)
- Hiredis & Hiredis SSL
- GTest (for tests)
- Docker (for Redis/Valkey-backed tests)

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
