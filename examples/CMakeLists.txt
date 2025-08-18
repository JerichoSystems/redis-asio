cmake_minimum_required(VERSION 3.21)
project(redis_asio_examples LANGUAGES CXX)

find_package(boost_asio CONFIG REQUIRED)

add_executable(psub_async psub_async.cpp)
target_link_libraries(psub_async PRIVATE Boost::asio redis_asio)

target_include_directories(psub_async PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

# Optionally, add more examples here as needed
