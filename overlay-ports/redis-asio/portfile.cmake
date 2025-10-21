vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
    tests REDIS_ASIO_BUILD_TESTS
    examples REDIS_ASIO_BUILD_EXAMPLES
    benchmarks REDIS_ASIO_BUILD_BENCHMARKS
    sanitize REDIS_ASIO_SANITIZE
)

# In dev (overlay in the project tree), build from the local sources.
if(EXISTS "${CURRENT_PORT_DIR}/../../CMakeLists.txt")
    set(SOURCE_PATH "${CURRENT_PORT_DIR}/../..") # project root
else()
    # For tagged releases in the registry:
    vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO jerichosystems/redis-asio
        REF v0.1.8
        SHA512 0 # fill when publishing a tag
    )
endif()

if(VCPKG_LIBRARY_LINKAGE STREQUAL "dynamic")
    set(_build_shared ON)
else()
    set(_build_shared OFF)
endif()

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
    -DREDIS_ASIO_BUILD_TESTS=OFF # default off; features flip them on
    -DREDIS_ASIO_BUILD_EXAMPLES=OFF
    -DREDIS_ASIO_BUILD_BENCHMARKS=OFF
    -DREDIS_ASIO_SANITIZE=OFF
    -DBUILD_SHARED_LIBS=${_build_shared}
    ${FEATURE_OPTIONS}
)
vcpkg_cmake_build()
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME redis_asio CONFIG_PATH lib/cmake/redis_asio)
vcpkg_fixup_pkgconfig()
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")

# Optionally install test/example executables under tools/<port>
if("tests" IN_LIST FEATURES)
    # Ensure the target exists in your CMake when REDIS_ASIO_BUILD_TESTS=ON
    # vcpkg_cmake_build(TARGETS redis_asio_tests)
    vcpkg_copy_tools(TOOL_NAMES redis_asio_tests SEARCH_DIR ${CURRENT_PACKAGES_DIR}/bin AUTO_CLEAN)
    vcpkg_copy_tools(TOOL_NAMES redis_async_tests SEARCH_DIR ${CURRENT_PACKAGES_DIR}/bin AUTO_CLEAN)
    vcpkg_copy_tools(TOOL_NAMES redis_log_on_tests SEARCH_DIR ${CURRENT_PACKAGES_DIR}/bin AUTO_CLEAN)
    vcpkg_copy_tools(TOOL_NAMES redis_log_off_tests SEARCH_DIR ${CURRENT_PACKAGES_DIR}/bin AUTO_CLEAN)
    vcpkg_copy_tools(TOOL_NAMES redis_log_rt_tests SEARCH_DIR ${CURRENT_PACKAGES_DIR}/bin AUTO_CLEAN)
    vcpkg_copy_tools(TOOL_NAMES redis_value_tests SEARCH_DIR ${CURRENT_PACKAGES_DIR}/bin AUTO_CLEAN)
endif()

if("benchmarks" IN_LIST FEATURES)
    # Ensure the target exists in your CMake when REDIS_ASIO_BUILD_TESTS=ON
    # vcpkg_cmake_build(TARGETS redis_asio_tests)
    vcpkg_copy_tools(TOOL_NAMES redis_async_bench SEARCH_DIR ${CURRENT_PACKAGES_DIR}/bin AUTO_CLEAN)
    vcpkg_copy_tools(TOOL_NAMES redis_value_bench SEARCH_DIR ${CURRENT_PACKAGES_DIR}/bin AUTO_CLEAN)
endif()

if("examples" IN_LIST FEATURES)
    # vcpkg_cmake_build(TARGETS psub_async)
    vcpkg_copy_tools(TOOL_NAMES psub_async AUTO_CLEAN)
endif()

# prune duplicate headers from debug package
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
