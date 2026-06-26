#pragma once

#include "redis_async.hpp"

#include <boost/asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

namespace redis_asio_tests {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using namespace std::chrono_literals;

inline std::string trim(std::string s) {
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r' || s.back() == ' ' || s.back() == '\t'))
        s.pop_back();
    std::size_t first = 0;
    while (first < s.size() && (s[first] == '\n' || s[first] == '\r' || s[first] == ' ' || s[first] == '\t'))
        ++first;
    if (first)
        s.erase(0, first);
    return s;
}

inline std::string shell_quote(std::string_view value) {
    std::string out = "'";
    for (char ch : value) {
        if (ch == '\'')
            out += "'\\''";
        else
            out.push_back(ch);
    }
    out.push_back('\'');
    return out;
}

inline std::string run_capture(const std::string &cmd, int *status = nullptr) {
    std::array<char, 256> buf{};
    std::string out;
    FILE *pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        if (status)
            *status = -1;
        return {};
    }
    while (fgets(buf.data(), static_cast<int>(buf.size()), pipe))
        out += buf.data();
    int rc = pclose(pipe);
    if (status)
        *status = rc;
    return trim(out);
}

inline bool run_ok(const std::string &cmd) {
    int status = 0;
    (void)run_capture(cmd + " >/dev/null 2>&1", &status);
    return status == 0;
}

inline bool docker_available() {
    static const bool available = run_ok("docker version --format '{{.Server.Version}}'");
    return available;
}

inline bool openssl_available() {
    static const bool available = run_ok("openssl version");
    return available;
}

inline std::string unique_name(std::string_view prefix) {
    static std::atomic<unsigned long long> seq{0};
    std::ostringstream os;
    os << prefix << "-" << getpid() << "-" << seq.fetch_add(1);
    return os.str();
}

struct DockerContainer {
    std::string id;

    DockerContainer() = default;
    explicit DockerContainer(std::string container_id) : id(std::move(container_id)) {}
    DockerContainer(const DockerContainer &) = delete;
    DockerContainer &operator=(const DockerContainer &) = delete;
    DockerContainer(DockerContainer &&other) noexcept : id(std::move(other.id)) {}
    DockerContainer &operator=(DockerContainer &&other) noexcept {
        if (this != &other) {
            stop();
            id = std::move(other.id);
        }
        return *this;
    }
    ~DockerContainer() { stop(); }

    void stop() {
        if (id.empty())
            return;
        (void)run_ok("docker rm -f " + shell_quote(id));
        id.clear();
    }

    bool exec_ok(const std::string &args) const {
        return run_ok("docker exec " + shell_quote(id) + " " + args);
    }

    std::string exec_capture(const std::string &args, int *status = nullptr) const {
        return run_capture("docker exec " + shell_quote(id) + " " + args + " 2>&1", status);
    }

    uint16_t mapped_port(uint16_t container_port = 6379) const {
        int status = 0;
        auto out = run_capture("docker port " + shell_quote(id) + " " + std::to_string(container_port) + "/tcp 2>&1", &status);
        if (status != 0)
            throw std::runtime_error("docker port failed: " + out);
        std::istringstream lines(out);
        std::string line;
        while (std::getline(lines, line)) {
            line = trim(line);
            if (line.empty())
                continue;
            auto colon = line.rfind(':');
            if (colon == std::string::npos)
                continue;
            return static_cast<uint16_t>(std::stoi(line.substr(colon + 1)));
        }
        throw std::runtime_error("docker port did not return a host port");
    }

    void wait_ready() const {
        for (int i = 0; i < 120; ++i) {
            if (exec_ok("valkey-cli -p 6379 ping"))
                return;
            std::this_thread::sleep_for(250ms);
        }
        throw std::runtime_error("Valkey container did not become ready");
    }

    void wait_tls_ready() const {
        for (int i = 0; i < 120; ++i) {
            if (exec_ok("valkey-cli --tls --insecure -p 6379 ping"))
                return;
            std::this_thread::sleep_for(250ms);
        }
        throw std::runtime_error("TLS Valkey container did not become ready");
    }
};

inline DockerContainer run_valkey_container(const std::string &name,
                                            const std::vector<std::string> &extra_args = {},
                                            const std::optional<std::string> &network = std::nullopt,
                                            const std::optional<std::string> &alias = std::nullopt) {
    std::ostringstream cmd;
    cmd << "docker run -d --rm --name " << shell_quote(name);
    if (network)
        cmd << " --network " << shell_quote(*network);
    if (alias)
        cmd << " --network-alias " << shell_quote(*alias);
    cmd << " -p 127.0.0.1::6379 valkey/valkey:8-alpine valkey-server"
        << " --appendonly no --save " << shell_quote("") << " --protected-mode no";
    for (const auto &arg : extra_args)
        cmd << " " << shell_quote(arg);
    cmd << " 2>&1";

    int status = 0;
    auto id = run_capture(cmd.str(), &status);
    if (status != 0 || id.empty())
        throw std::runtime_error("docker run failed: " + id);
    DockerContainer container{id};
    container.wait_ready();
    return container;
}

struct TestCertificates {
    std::filesystem::path dir;
    std::filesystem::path server_crt;
    std::filesystem::path server_key;

    TestCertificates() {
        auto base = std::filesystem::temp_directory_path() / unique_name("redis-asio-tls");
        std::filesystem::create_directories(base);
        dir = base;
        server_crt = dir / "server.crt";
        server_key = dir / "server.key";

        auto cmd = "openssl req -x509 -nodes -newkey rsa:2048 -days 1"
                   " -keyout "
                   + shell_quote(server_key.string())
                   + " -out " + shell_quote(server_crt.string())
                   + " -subj " + shell_quote("/CN=localhost")
                   + " -addext " + shell_quote("subjectAltName=DNS:localhost,IP:127.0.0.1")
                   + " 2>&1";
        int status = 0;
        auto out = run_capture(cmd, &status);
        if (status != 0)
            throw std::runtime_error("openssl certificate generation failed: " + out);

        std::filesystem::permissions(dir,
                                     std::filesystem::perms::owner_all | std::filesystem::perms::group_read
                                         | std::filesystem::perms::group_exec | std::filesystem::perms::others_read
                                         | std::filesystem::perms::others_exec,
                                     std::filesystem::perm_options::replace);
        std::filesystem::permissions(server_crt,
                                     std::filesystem::perms::owner_read | std::filesystem::perms::group_read
                                         | std::filesystem::perms::others_read,
                                     std::filesystem::perm_options::replace);
        std::filesystem::permissions(server_key,
                                     std::filesystem::perms::owner_read | std::filesystem::perms::group_read
                                         | std::filesystem::perms::others_read,
                                     std::filesystem::perm_options::replace);
    }

    TestCertificates(const TestCertificates &) = delete;
    TestCertificates &operator=(const TestCertificates &) = delete;

    ~TestCertificates() {
        if (!dir.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    }
};

inline DockerContainer run_valkey_tls_container(const std::string &name, const TestCertificates &certs) {
    std::ostringstream cmd;
    cmd << "docker run -d --rm --name " << shell_quote(name)
        << " -v " << shell_quote(certs.dir.string() + ":/tls:ro")
        << " -p 127.0.0.1::6379 valkey/valkey:8-alpine valkey-server"
        << " --port 0"
        << " --tls-port 6379"
        << " --tls-cert-file /tls/server.crt"
        << " --tls-key-file /tls/server.key"
        << " --tls-ca-cert-file /tls/server.crt"
        << " --tls-auth-clients no"
        << " --appendonly no --save " << shell_quote("")
        << " --protected-mode no"
        << " 2>&1";

    int status = 0;
    auto id = run_capture(cmd.str(), &status);
    if (status != 0 || id.empty())
        throw std::runtime_error("docker run TLS Valkey failed: " + id);
    DockerContainer container{id};
    container.wait_tls_ready();
    return container;
}

struct ManagedRedis {
    bool initialized{false};
    bool available{false};
    std::string skip_reason;
    std::optional<DockerContainer> container;
    redis_asio::ConnectOptions options;

    void ensure_started() {
        if (initialized)
            return;
        initialized = true;

        if (const char *external = std::getenv("REDIS_ASIO_USE_EXTERNAL_REDIS"); external && std::strcmp(external, "1") == 0) {
            options.host = std::getenv("REDIS_HOST") ? std::getenv("REDIS_HOST") : "127.0.0.1";
            options.port = std::getenv("REDIS_PORT") ? static_cast<uint16_t>(std::atoi(std::getenv("REDIS_PORT"))) : 6379;
            available = true;
            return;
        }

        if (!docker_available()) {
            skip_reason = "Docker is not available";
            return;
        }

        try {
            auto name = unique_name("redis-asio-valkey");
            container.emplace(run_valkey_container(name));
            options.host = "127.0.0.1";
            options.port = container->mapped_port();
            available = true;
        } catch (const std::exception &ex) {
            skip_reason = ex.what();
            container.reset();
            available = false;
        }
    }
};

inline ManagedRedis &managed_redis() {
    static ManagedRedis instance;
    instance.ensure_started();
    return instance;
}

inline bool redis_runtime_available() {
    return managed_redis().available;
}

inline std::string redis_runtime_skip_reason() {
    return managed_redis().skip_reason.empty() ? "Redis test runtime is unavailable" : managed_redis().skip_reason;
}

inline redis_asio::ConnectOptions redis_runtime_options() {
    return managed_redis().options;
}

struct ManagedTlsRedis {
    bool initialized{false};
    bool available{false};
    std::string skip_reason;
    std::optional<TestCertificates> certs;
    std::optional<DockerContainer> container;
    redis_asio::ConnectOptions options;

    void ensure_started() {
        if (initialized)
            return;
        initialized = true;

        if (!docker_available()) {
            skip_reason = "Docker is not available";
            return;
        }
        if (!openssl_available()) {
            skip_reason = "openssl is not available";
            return;
        }

        try {
            certs.emplace();
            auto name = unique_name("redis-asio-valkey-tls");
            container.emplace(run_valkey_tls_container(name, *certs));
            options.host = "127.0.0.1";
            options.port = container->mapped_port();
            options.tls.use_tls = true;
            options.tls.verify_peer = true;
            options.tls.ca_file = certs->server_crt.string();
            available = true;
        } catch (const std::exception &ex) {
            skip_reason = ex.what();
            container.reset();
            certs.reset();
            available = false;
        }
    }
};

inline ManagedTlsRedis &managed_tls_redis() {
    static ManagedTlsRedis instance;
    instance.ensure_started();
    return instance;
}

inline bool tls_redis_runtime_available() {
    return managed_tls_redis().available;
}

inline std::string tls_redis_runtime_skip_reason() {
    return managed_tls_redis().skip_reason.empty() ? "TLS Redis test runtime is unavailable" : managed_tls_redis().skip_reason;
}

inline redis_asio::ConnectOptions tls_redis_runtime_options() {
    return managed_tls_redis().options;
}

class TcpProxy {
  public:
    TcpProxy(std::string host, uint16_t port)
        : work_(asio::make_work_guard(ioc_)),
          acceptor_(ioc_, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0)) {
        set_target(std::move(host), port);
        start_accept();
        thread_ = std::thread([this] { ioc_.run(); });
    }

    TcpProxy(const TcpProxy &) = delete;
    TcpProxy &operator=(const TcpProxy &) = delete;

    ~TcpProxy() {
        work_.reset();
        boost::system::error_code ec;
        acceptor_.close(ec);
        ioc_.stop();
        if (thread_.joinable())
            thread_.join();
    }

    uint16_t port() const {
        return acceptor_.local_endpoint().port();
    }

    void set_target(std::string host, uint16_t port) {
        std::lock_guard lock(target_mutex_);
        target_host_ = std::move(host);
        target_port_ = port;
    }

  private:
    struct Session : std::enable_shared_from_this<Session> {
        tcp::socket client;
        tcp::socket server;
        std::array<char, 8192> c2s{};
        std::array<char, 8192> s2c{};
        std::string host;
        uint16_t port;

        Session(tcp::socket accepted, asio::io_context &ioc, std::string target_host, uint16_t target_port)
            : client(std::move(accepted)), server(ioc), host(std::move(target_host)), port(target_port) {}

        void start() {
            auto self = shared_from_this();
            server.async_connect(tcp::endpoint(asio::ip::make_address(host), port), [self](boost::system::error_code ec) {
                if (ec)
                    return;
                self->pump_client();
                self->pump_server();
            });
        }

        void close() {
            boost::system::error_code ec;
            client.close(ec);
            server.close(ec);
        }

        void pump_client() {
            auto self = shared_from_this();
            client.async_read_some(asio::buffer(c2s), [self](boost::system::error_code ec, std::size_t n) {
                if (ec) {
                    self->close();
                    return;
                }
                asio::async_write(self->server, asio::buffer(self->c2s.data(), n), [self](boost::system::error_code ec, std::size_t) {
                    if (ec) {
                        self->close();
                        return;
                    }
                    self->pump_client();
                });
            });
        }

        void pump_server() {
            auto self = shared_from_this();
            server.async_read_some(asio::buffer(s2c), [self](boost::system::error_code ec, std::size_t n) {
                if (ec) {
                    self->close();
                    return;
                }
                asio::async_write(self->client, asio::buffer(self->s2c.data(), n), [self](boost::system::error_code ec, std::size_t) {
                    if (ec) {
                        self->close();
                        return;
                    }
                    self->pump_server();
                });
            });
        }
    };

    void start_accept() {
        acceptor_.async_accept([this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::string host;
                uint16_t port = 0;
                {
                    std::lock_guard lock(target_mutex_);
                    host = target_host_;
                    port = target_port_;
                }
                std::make_shared<Session>(std::move(socket), ioc_, std::move(host), port)->start();
            }
            if (acceptor_.is_open())
                start_accept();
        });
    }

    asio::io_context ioc_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    tcp::acceptor acceptor_;
    std::thread thread_;
    mutable std::mutex target_mutex_;
    std::string target_host_{"127.0.0.1"};
    uint16_t target_port_{0};
};

struct ValkeyReplicationFixture {
    bool available{false};
    std::string skip_reason;
    std::string network;
    std::optional<DockerContainer> primary;
    std::optional<DockerContainer> replica;
    std::optional<TcpProxy> proxy;

    ValkeyReplicationFixture() {
        if (!docker_available()) {
            skip_reason = "Docker is not available";
            return;
        }
        try {
            network = unique_name("redis-asio-net");
            if (!run_ok("docker network create " + shell_quote(network)))
                throw std::runtime_error("docker network create failed");

            primary.emplace(run_valkey_container(unique_name("redis-asio-primary"), {}, network, "primary"));
            replica.emplace(run_valkey_container(unique_name("redis-asio-replica"), {"--replicaof", "primary", "6379"}, network, "replica"));
            wait_role(*primary, "master");
            wait_role(*replica, "slave");
            proxy.emplace("127.0.0.1", primary->mapped_port());
            available = true;
        } catch (const std::exception &ex) {
            skip_reason = ex.what();
            proxy.reset();
            replica.reset();
            primary.reset();
            if (!network.empty())
                (void)run_ok("docker network rm " + shell_quote(network));
        }
    }

    ValkeyReplicationFixture(const ValkeyReplicationFixture &) = delete;
    ValkeyReplicationFixture &operator=(const ValkeyReplicationFixture &) = delete;

    ~ValkeyReplicationFixture() {
        proxy.reset();
        replica.reset();
        primary.reset();
        if (!network.empty())
            (void)run_ok("docker network rm " + shell_quote(network));
    }

    redis_asio::ConnectOptions proxy_options() const {
        redis_asio::ConnectOptions opts;
        opts.host = "127.0.0.1";
        opts.port = proxy->port();
        return opts;
    }

    void flip_roles() {
        if (!available)
            return;
        if (!replica->exec_ok("valkey-cli -p 6379 replicaof no one"))
            throw std::runtime_error("replicaof no one failed");
        wait_role(*replica, "master");
        if (!primary->exec_ok("valkey-cli -p 6379 replicaof replica 6379"))
            throw std::runtime_error("primary replicaof replica failed");
        wait_role(*primary, "slave");
        proxy->set_target("127.0.0.1", replica->mapped_port());
    }

    void wait_replica_value(std::string_view key, std::string_view expected) const {
        wait_value(*replica, key, expected);
    }

    static void wait_role(const DockerContainer &container, std::string_view expected) {
        for (int i = 0; i < 120; ++i) {
            auto role = container.exec_capture("valkey-cli -p 6379 role");
            if (role.find(expected) != std::string::npos)
                return;
            std::this_thread::sleep_for(250ms);
        }
        throw std::runtime_error("Valkey role did not become " + std::string(expected));
    }

    static void wait_value(const DockerContainer &container, std::string_view key, std::string_view expected) {
        for (int i = 0; i < 120; ++i) {
            auto value = container.exec_capture("valkey-cli -p 6379 get " + shell_quote(key));
            if (value == expected)
                return;
            std::this_thread::sleep_for(250ms);
        }
        throw std::runtime_error("Valkey value did not replicate for key " + std::string(key));
    }
};

} // namespace redis_asio_tests
