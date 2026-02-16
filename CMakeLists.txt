cmake_minimum_required(VERSION 3.21)
project(redis_asio VERSION 0.1.12 LANGUAGES C CXX)

if(POLICY CMP0167)
    # Keep FindBoost module behavior and silence the dev warning.
    cmake_policy(SET CMP0167 OLD)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Options
option(REDIS_ASIO_BUILD_TESTS "Build unit tests" OFF)
option(REDIS_ASIO_INSTALL_TESTS "Install unit test executables" OFF)
option(REDIS_ASIO_BUILD_EXAMPLES "Build examples" OFF)
option(REDIS_ASIO_SANITIZE "Enable ASan/UBSan" OFF)
option(REDIS_ASIO_BUILD_BENCHMARKS "Build benchmarks (Google Benchmark)" OFF)
option(REDIS_ASIO_ENABLE_COVERAGE "Enable code coverage instrumentation and reporting" OFF)
option(BUILD_SHARED_LIBS "Build shared libraries" ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Common settings
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG") # Add debug flag

function(redis_asio_enable_sanitizers target)
    if(REDIS_ASIO_SANITIZE)
        target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g -O1)
        target_link_options(${target} PRIVATE -fsanitize=address,undefined)
    endif()
endfunction()

function(redis_asio_enable_coverage target)
    if(REDIS_ASIO_ENABLE_COVERAGE)
        if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            message(FATAL_ERROR "Coverage instrumentation requires GCC or Clang")
        endif()
        target_compile_options(${target} PRIVATE --coverage -O0 -g)
        target_link_options(${target} PRIVATE --coverage)
    endif()
endfunction()

# Dependencies: Boost (for Asio), hiredis, hiredis_ssl
include(FetchContent)
find_package(Threads REQUIRED)

# Prefer CONFIG packages so vcpkg works out-of-the-box:
find_package(Boost CONFIG REQUIRED COMPONENTS asio)
find_package(hiredis CONFIG REQUIRED) # provides hiredis::hiredis
find_package(hiredis_ssl CONFIG REQUIRED) # provides hiredis::hiredis_ssl

add_library(redis_asio
    src/redis_log.cpp
    src/hiredis_asio_adapter.cpp
    src/redis_async.cpp)

target_include_directories(redis_asio
    PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(redis_asio
    PUBLIC Threads::Threads
    PRIVATE Boost::asio hiredis::hiredis hiredis::hiredis_ssl)
add_library(redis_asio::redis_asio ALIAS redis_asio)
set_target_properties(redis_asio PROPERTIES
    EXPORT_NAME redis_asio
    SOVERSION ${PROJECT_VERSION_MAJOR}
    VERSION ${PROJECT_VERSION})
redis_asio_enable_sanitizers(redis_asio)
redis_asio_enable_coverage(redis_asio)

if(REDIS_ASIO_ENABLE_COVERAGE)
    find_program(GCOVR_EXECUTABLE gcovr)
    if(NOT GCOVR_EXECUTABLE)
        message(FATAL_ERROR "gcovr executable not found; install gcovr to enable coverage reports")
    endif()
    add_custom_target(coverage
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/coverage
        COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}
        COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_BINARY_DIR} ctest --output-on-failure
        COMMAND ${GCOVR_EXECUTABLE}
                --root "${CMAKE_SOURCE_DIR}"
                --object-directory "${CMAKE_BINARY_DIR}"
                --filter "${CMAKE_SOURCE_DIR}"
                --exclude "${CMAKE_SOURCE_DIR}/tests/.*"
                --exclude "${CMAKE_SOURCE_DIR}/benchmarks/.*"
                --exclude "${CMAKE_SOURCE_DIR}/examples/.*"
                --exclude "${CMAKE_BINARY_DIR}/.*"
                --exclude '.*/vcpkg_installed/.*'
                --xml ${CMAKE_BINARY_DIR}/coverage/coverage.xml
                --html-details ${CMAKE_BINARY_DIR}/coverage/index.html
                --print-summary
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report with gcovr"
        VERBATIM)
endif()

if(REDIS_ASIO_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

    set(REDIS_ASIO_TEST_TARGETS "")

    function(redis_asio_add_gtest target)
        cmake_parse_arguments(ARG "" "" "SOURCES;DEFINES;LIBS" ${ARGN})
        if(NOT ARG_SOURCES)
            message(FATAL_ERROR "redis_asio_add_gtest(${target}) requires SOURCES")
        endif()
        add_executable(${target} ${ARG_SOURCES})
        target_link_libraries(${target} PRIVATE GTest::gtest_main ${ARG_LIBS})
        target_include_directories(${target}
            PUBLIC
                $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
                ${Boost_INCLUDE_DIRS})
        target_compile_definitions(${target} PRIVATE REDIS_ASIO_TEST_ACCESS)
        if(ARG_DEFINES)
            target_compile_definitions(${target} PRIVATE ${ARG_DEFINES})
        endif()
        redis_asio_enable_sanitizers(${target})
        redis_asio_enable_coverage(${target})
        set(local_targets ${REDIS_ASIO_TEST_TARGETS})
        list(APPEND local_targets ${target})
        set(REDIS_ASIO_TEST_TARGETS "${local_targets}" PARENT_SCOPE)
    endfunction()

    redis_asio_add_gtest(redis_async_tests
        SOURCES tests/redis_async_tests.cpp
        LIBS Boost::asio redis_asio)

    redis_asio_add_gtest(redis_value_tests
        SOURCES tests/redis_value_tests.cpp
        LIBS Boost::asio redis_asio)

    redis_asio_add_gtest(hiredis_asio_adapter_tests
        SOURCES tests/hiredis_asio_adapter_tests.cpp
        LIBS Boost::asio redis_asio)

    redis_asio_add_gtest(redis_log_on_tests
        SOURCES tests/redis_log_on_tests.cpp src/redis_log.cpp
        DEFINES
            REDIS_LOG_MIN_LEVEL=::redis_asio::Logger::Level::trace
            REDIS_LOG_MIN_LEVEL_I=0
            REDIS_LOG_SOURCE_MIN_LEVEL=::redis_asio::Logger::Level::warn
            REDIS_LOG_FORCE_COLOR)

    redis_asio_add_gtest(redis_log_off_tests
        SOURCES tests/redis_log_off_tests.cpp src/redis_log.cpp
        DEFINES
            REDIS_LOG_MIN_LEVEL=::redis_asio::Logger::Level::info
            REDIS_LOG_MIN_LEVEL_I=2
            REDIS_LOG_SOURCE_MIN_LEVEL=::redis_asio::Logger::Level::warn
            REDIS_LOG_DISABLE_COLOR)

    redis_asio_add_gtest(redis_log_rt_tests
        SOURCES tests/redis_log_rt_tests.cpp src/redis_log.cpp
        DEFINES
            REDIS_LOG_MIN_LEVEL=::redis_asio::Logger::Level::trace
            REDIS_LOG_MIN_LEVEL_I=0
            REDIS_LOG_SOURCE_MIN_LEVEL=::redis_asio::Logger::Level::warn
            REDIS_LOG_FORCE_COLOR)

    redis_asio_add_gtest(redis_asio_tests
        SOURCES tests/redis_asio_tests.cpp
        DEFINES
            REDIS_LOG_MIN_LEVEL=::redis_asio::Logger::Level::trace
            REDIS_LOG_MIN_LEVEL_I=0
            REDIS_LOG_SOURCE_MIN_LEVEL=::redis_asio::Logger::Level::warn
            REDIS_LOG_FORCE_COLOR)

    include(GoogleTest)
    foreach(test_target IN LISTS REDIS_ASIO_TEST_TARGETS)
        gtest_discover_tests(${test_target})
        if(REDIS_ASIO_INSTALL_TESTS)
            install(TARGETS ${test_target} RUNTIME DESTINATION bin)
        endif()
    endforeach()
endif()

if(REDIS_ASIO_BUILD_EXAMPLES)
    add_executable(psub_async examples/psub_async.cpp)
    target_include_directories(psub_async
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    target_link_libraries(psub_async PRIVATE Boost::asio redis_asio)
    redis_asio_enable_sanitizers(psub_async)
    redis_asio_enable_coverage(psub_async)
    install(TARGETS psub_async RUNTIME DESTINATION bin)
    install(DIRECTORY examples/ DESTINATION share/${PROJECT_NAME}/examples
        FILES_MATCHING PATTERN "*.cpp" PATTERN "CMakeLists.txt")
endif()

if(REDIS_ASIO_BUILD_BENCHMARKS)
    find_package(benchmark CONFIG QUIET)
    if(NOT benchmark_FOUND)
        message(FATAL_ERROR "Google Benchmark (benchmark::benchmark) not found. Install it via vcpkg or disable REDIS_ASIO_BUILD_BENCHMARKS.")
    endif()
    add_executable(redis_value_bench benchmarks/redis_value_bench.cpp)
    target_include_directories(redis_value_bench
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        ${Boost_INCLUDE_DIRS})
    target_link_libraries(redis_value_bench PRIVATE redis_asio benchmark::benchmark)
    redis_asio_enable_sanitizers(redis_value_bench)
    redis_asio_enable_coverage(redis_value_bench)

    add_executable(redis_async_bench benchmarks/redis_async_bench.cpp)
    target_include_directories(redis_async_bench
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        ${Boost_INCLUDE_DIRS})
    target_link_libraries(redis_async_bench PRIVATE redis_asio benchmark::benchmark Threads::Threads)
    redis_asio_enable_sanitizers(redis_async_bench)
    redis_asio_enable_coverage(redis_async_bench)

    # add_test(NAME RedisValueBench COMMAND redis_value_bench --benchmark_out_format=console)
    # add_test(NAME RedisAsyncBench COMMAND redis_async_bench --benchmark_out_format=console)
    # set_tests_properties(RedisValueBench PROPERTIES TIMEOUT 20)
    # set_tests_properties(RedisAsyncBench PROPERTIES TIMEOUT 300)

    install(TARGETS redis_value_bench RUNTIME DESTINATION bin)
    install(TARGETS redis_async_bench RUNTIME DESTINATION bin)
endif()

# ---------- Install & CMake package export ----------
install(TARGETS redis_asio
    EXPORT redis_asioTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp" PATTERN "*.ipp")

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/redis_asioConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/redis_asioConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/redis_asioConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redis_asio)

install(EXPORT redis_asioTargets
    NAMESPACE redis_asio::
    FILE redis_asioTargets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redis_asio)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/redis_asioConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/redis_asioConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redis_asio)

configure_file(cmake/redis_asio.pc.in "${CMAKE_CURRENT_BINARY_DIR}/redis_asio.pc" @ONLY)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/redis_asio.pc" DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

# Optional: CPack to create tarballs
include(CPack)
set(CPACK_PACKAGE_NAME "redis_asio")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})

set(CPACK_GENERATOR "TGZ")
set(CPACK_PACKAGE_FILE_NAME "redis_asio-${PROJECT_VERSION}-${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")
