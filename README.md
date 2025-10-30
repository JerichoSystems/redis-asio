<!-- ![CI](https://github.com/jerichosystems/redisasync/actions/workflows/ci.yml/badge.svg) -->
[![CircleCI](https://dl.circleci.com/status-badge/img/gh/JerichoSystems/redis-asio/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/JerichoSystems/redis-asio/tree/main)
# redis_asio

Asynchronous C++ client library for Redis, using Boost.Asio and Hiredis.

## Features
- Modern C++ (C++23)
- Asynchronous API using Boost.Asio
- Hiredis backend
- Example and test suite included

## Requirements
- CMake >= 3.21
- C++23 compiler (GCC >= 13, Clang >= 16, MSVC >= 2022)
- Boost (Asio)
- Hiredis & Hiredis SSL
- GTest (for tests)
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
