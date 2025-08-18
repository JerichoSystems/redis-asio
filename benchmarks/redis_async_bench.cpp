#include <benchmark/benchmark.h>
#include <atomic>
#include <boost/asio.hpp>
#include <future>
#include "redis_async.hpp"
#include "redis_value.hpp"

namespace asio = boost::asio;
using namespace std::chrono_literals;

static redis_asio::ConnectOptions opts_from_env() {
    redis_asio::ConnectOptions o;
    if (const char *h = std::getenv("REDIS_HOST")) o.host = h;
    if (const char *p = std::getenv("REDIS_PORT")) o.port = static_cast<uint16_t>(std::atoi(p));
    if (const char *u = std::getenv("REDIS_USER")) o.username = std::string(u);
    if (const char *pw = std::getenv("REDIS_PASS")) o.password = std::string(pw);
    if (const char *nm = std::getenv("REDIS_NAME")) o.client_name = std::string(nm);
    if (const char *t = std::getenv("REDIS_TLS")) o.tls.use_tls = (*t=='1');
    if (const char *vf = std::getenv("REDIS_TLS_VERIFY")) o.tls.verify_peer = (*vf!='0');
    if (const char *ca = std::getenv("REDIS_CAFILE")) o.tls.ca_file = ca;
    if (const char *cp = std::getenv("REDIS_CAPATH")) o.tls.ca_path = cp;
    if (const char *cf = std::getenv("REDIS_CERT")) o.tls.cert_file = cf;
    if (const char *kf = std::getenv("REDIS_KEY")) o.tls.key_file = kf;
    return o;
}

struct EchoFixture {
    std::unique_ptr<asio::io_context> ioc;
    std::thread thr;
    std::shared_ptr<redis_asio::RedisAsyncConnection> c;

    void SetUp() {
        ioc = std::make_unique<asio::io_context>();
        c = redis_asio::RedisAsyncConnection::create(ioc->get_executor());

        // Run the io_context on a thread

        // Connect synchronously via promise
        std::promise<std::tuple<std::error_code,bool>> p;
        c->async_connect(opts_from_env(),
            [&](std::error_code ec, bool already){ p.set_value({ec,already}); });
        thr = std::thread([this] { ioc->run(); });
        auto [ec, already] = p.get_future().get();
        if (ec) throw std::runtime_error("connect failed: " + ec.message());
        (void)already;
    }
    void TearDown() {
        c->stop();
        ioc->stop();
        if (thr.joinable()) thr.join();
    }
};

static void BM_Redis_Echo_SingleRoundtrip(benchmark::State& state) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    EchoFixture fx; fx.SetUp();

    std::string payload((size_t)state.range(0), 'x');

    for (auto _ : state) {
        std::promise<void> p;
        std::vector<std::string> cmd;
        cmd.reserve(2);
        cmd.emplace_back("ECHO");
        cmd.emplace_back(payload); // avoid inline to_string in initializer_list (GCC ICE workaround)
        fx.c->async_command(cmd, [&](std::error_code ec, redis_asio::RedisValue v){
            if (ec) { p.set_value(); return; }
            auto s = redis_asio::string_like(v);
            if (!s || *s != payload) {
                // mark as error, but keep benchmark flowing
            }
            p.set_value();
        });
        p.get_future().wait();
    }

    fx.TearDown();
}
// payload sizes
BENCHMARK(BM_Redis_Echo_SingleRoundtrip)->Arg(1)->Arg(16)->Arg(256)->Arg(4096)->Arg(65536);

// ---------- Pipeline Echo: N in-flight per iteration ----------
static void BM_Redis_Echo_PipelineBatch(benchmark::State& state) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    EchoFixture fx; fx.SetUp();

    const int batch = static_cast<int>(state.range(0));
    std::string payload(16, 'x');

    struct Latch {
        std::atomic<int> n;
        std::promise<void> done;
        explicit Latch(int v) : n(v) {}
        void hit() {
            if (n.fetch_sub(1, std::memory_order_acq_rel) == 1)
                done.set_value();
        }
    };

    for (auto _ : state) {
        Latch latch(batch);
        for (int i = 0; i < batch; ++i) {
            std::vector<std::string> cmd;
            cmd.reserve(2);
            cmd.emplace_back("ECHO");
            cmd.emplace_back(payload);
            fx.c->async_command(cmd, [&](std::error_code, redis_asio::RedisValue) {
                latch.hit();
            });
        }
        latch.done.get_future().wait();
    }

    fx.TearDown();
}

// Typical fanout sizes
BENCHMARK(BM_Redis_Echo_PipelineBatch)->Arg(1)->Arg(8)->Arg(32)->Arg(128)->Arg(512);

// ---------- SET + GET roundtrip (payload size varied) ----------
static void BM_Redis_SetGet_Roundtrip(benchmark::State& state) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    EchoFixture fx; fx.SetUp();

    const std::string key = "bench:key";
    std::string payload(static_cast<size_t>(state.range(0)), 'x');

    for (auto _ : state) {
        std::promise<void> p;

        // SET
        std::vector<std::string> setcmd;
        setcmd.reserve(3);
        setcmd.emplace_back("SET");
        setcmd.emplace_back(key);
        setcmd.emplace_back(payload);

        fx.c->async_command(setcmd, [&](std::error_code ec_set, redis_asio::RedisValue) {
            if (ec_set) { p.set_value(); return; }

            // GET (chain after SET completes to avoid races)
            std::vector<std::string> getcmd;
            getcmd.reserve(2);
            getcmd.emplace_back("GET");
            getcmd.emplace_back(key);

            fx.c->async_command(getcmd, [&](std::error_code ec_get, redis_asio::RedisValue v) {
                (void)ec_get;
                auto s = redis_asio::string_like(v);
                if (!s || *s != payload) {
                    // mismatch -> still complete to avoid stalling the run
                }
                p.set_value();
            });
        });

        p.get_future().wait();
    }

    fx.TearDown();
}

BENCHMARK(BM_Redis_SetGet_Roundtrip)->Arg(1)->Arg(16)->Arg(256)->Arg(4096)->Arg(65536);

// ---------- Pub/Sub single-message latency (payload size varied) ----------
struct PubSubFixture {
    std::unique_ptr<asio::io_context> ioc;
    std::thread thr;
    std::shared_ptr<redis_asio::RedisAsyncConnection> sub;
    std::shared_ptr<redis_asio::RedisAsyncConnection> pub;
    std::string channel = "bench.q";

    void SetUp() {
        ioc = std::make_unique<asio::io_context>();
        sub = redis_asio::RedisAsyncConnection::create(ioc->get_executor());
        pub = redis_asio::RedisAsyncConnection::create(ioc->get_executor());

        // Connect both
        std::promise<std::tuple<std::error_code,bool>> ps, pp;
        sub->async_connect(opts_from_env(), [&](std::error_code ec, bool a){ ps.set_value({ec,a}); });
        pub->async_connect(opts_from_env(), [&](std::error_code ec, bool a){ pp.set_value({ec,a}); });

        thr = std::thread([this]{ ioc->run(); });

        auto [ecs, as] = ps.get_future().get(); (void)as;
        auto [ecp, ap] = pp.get_future().get(); (void)ap;
        if (ecs) throw std::runtime_error("sub connect failed: " + ecs.message());
        if (ecp) throw std::runtime_error("pub connect failed: " + ecp.message());

        // Subscribe (wait until ack)
        std::promise<std::error_code> ack;
        sub->async_subscribe({channel}, [&](std::error_code ec){ ack.set_value(ec); });
        if (auto ec = ack.get_future().get()) {
            throw std::runtime_error("subscribe failed: " + ec.message());
        }
    }

    void TearDown() {
        if (sub) sub->stop();
        if (pub) pub->stop();
        if (ioc) ioc->stop();
        if (thr.joinable()) thr.join();
    }
};

static void BM_Redis_PubSub_Roundtrip(benchmark::State& state) {
    redis_asio::RedisAsyncConnection::initOpenSSL();
    PubSubFixture fx; fx.SetUp();

    std::string payload(static_cast<size_t>(state.range(0)), 'x');

    for (auto _ : state) {
        std::promise<void> done;

        // Post receive first to avoid races
        fx.sub->async_receive_publish([&](std::error_code ec, redis_asio::PublishMessage msg){
            (void)ec;
            // Optional checks (cheap)
            // if (!ec && msg.payload == payload && msg.channel == fx.channel) {}
            done.set_value();
        });

        // Publish
        std::vector<std::string> cmd;
        cmd.reserve(3);
        cmd.emplace_back("PUBLISH");
        cmd.emplace_back(fx.channel);
        cmd.emplace_back(payload);
        fx.pub->async_command(cmd, [](std::error_code, redis_asio::RedisValue){});

        done.get_future().wait();
    }

    fx.TearDown();
}

BENCHMARK(BM_Redis_PubSub_Roundtrip)->Arg(1)->Arg(16)->Arg(256)->Arg(4096);

BENCHMARK_MAIN();
