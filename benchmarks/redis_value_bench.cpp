#include <benchmark/benchmark.h>
#include <hiredis/hiredis.h>
#include <cstdlib>
#include <cstring>
#include "redis_value.hpp"

using redis_asio::RedisValue;

static void BM_FromRaw_ArrayOfSmallStrings(benchmark::State& state) {
    const int N = static_cast<int>(state.range(0));
    for (auto _ : state) {
        // Build hiredis reply: ["0123456789", ...] (N items)
        auto root = new redisReply{};
        root->type = REDIS_REPLY_ARRAY;
        root->elements = N;
        root->element = static_cast<redisReply**>(calloc(N, sizeof(redisReply*)));
        for (int i = 0; i < N; ++i) {
            auto* e = new redisReply{};
            e->type = REDIS_REPLY_STRING;
            e->str = const_cast<char*>("0123456789");
            e->len = 10;
            root->element[i] = e;
        }

        RedisValue v = RedisValue::fromRaw(root);
        benchmark::DoNotOptimize(v);
        benchmark::ClobberMemory();

        for (int i = 0; i < N; ++i) delete root->element[i];
        free(root->element);
        delete root;
    }
}
BENCHMARK(BM_FromRaw_ArrayOfSmallStrings)->Arg(4)->Arg(16)->Arg(64)->Arg(256);

static void BM_ToString_Scalars(benchmark::State& state) {
    RedisValue s; s.type = RedisValue::Type::String; s.payload = std::string( (size_t)state.range(0), 'x');
    for (auto _ : state) {
        auto t = s.toString();
        benchmark::DoNotOptimize(t);
    }
}
BENCHMARK(BM_ToString_Scalars)->Arg(16)->Arg(128)->Arg(1024);

static void BM_FromRaw_Maps(benchmark::State& state) {
    const int pairs = static_cast<int>(state.range(0));
    for (auto _ : state) {
        auto *root = new redisReply{};
        root->type = REDIS_REPLY_MAP;
        root->elements = pairs * 2;
        root->element = static_cast<redisReply **>(calloc(static_cast<size_t>(pairs) * 2, sizeof(redisReply *)));

        for (int i = 0; i < pairs; ++i) {
            auto *key = new redisReply{};
            key->type = REDIS_REPLY_STRING;
            key->str = const_cast<char *>("key");
            key->len = 3;
            root->element[i * 2] = key;

            auto *val = new redisReply{};
            val->type = REDIS_REPLY_STRING;
            val->len = 16;
            val->str = static_cast<char *>(malloc(static_cast<size_t>(val->len)));
            memset(val->str, 'a' + (i % 26), static_cast<size_t>(val->len));
            root->element[i * 2 + 1] = val;
        }

        RedisValue v = RedisValue::fromRaw(root);
        benchmark::DoNotOptimize(v);
        benchmark::ClobberMemory();

        for (int i = 0; i < pairs * 2; ++i) {
            if (root->element[i]->str && root->element[i]->len > 3) {
                free(root->element[i]->str);
            }
            delete root->element[i];
        }
        free(root->element);
        delete root;
    }
}
BENCHMARK(BM_FromRaw_Maps)->Arg(4)->Arg(32)->Arg(128);
BENCHMARK_MAIN();
