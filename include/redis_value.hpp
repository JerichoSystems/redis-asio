#pragma once
#include <expected>
#include <format>
#include <hiredis/hiredis.h>
#include <sstream>
#include <string>
#include <system_error>
#include <variant>
#include <vector>

namespace redis_asio {

// Provided by redis_async.hpp
const std::error_category &category();
std::error_code protocol_error();

struct RedisValue {
    enum class Type : int {
        Nil = REDIS_REPLY_NIL,
        String = REDIS_REPLY_STRING,
        Error = REDIS_REPLY_ERROR,
        Integer = REDIS_REPLY_INTEGER,
        Status = REDIS_REPLY_STATUS,
        Double = REDIS_REPLY_DOUBLE,
        Bool = REDIS_REPLY_BOOL,
        Array = REDIS_REPLY_ARRAY,
        Push = REDIS_REPLY_PUSH,
        Map = REDIS_REPLY_MAP,
        Set = REDIS_REPLY_SET,
        Attr = REDIS_REPLY_ATTR,
        Bignum = REDIS_REPLY_BIGNUM,
        Verb = REDIS_REPLY_VERB
    } type{Type::Nil};

    using Array = std::vector<RedisValue>;
    using KVList = std::vector<std::pair<std::string, RedisValue>>;

    std::variant<
        std::monostate,
        std::string,
        bool,
        long long,
        double,
        Array,
        KVList>
        payload{};

    static RedisValue fromRaw(redisReply *r) {
        RedisValue v;
        if (!r)
            return v;
        switch (r->type) {
        case REDIS_REPLY_STRING:
            v.type = Type::String;
            v.payload = std::string(r->str, r->len);
            break;
        case REDIS_REPLY_STATUS:
            v.type = Type::Status;
            v.payload = std::string(r->str, r->len);
            break;
        case REDIS_REPLY_ERROR:
            v.type = Type::Error;
            v.payload = std::string(r->str, r->len);
            break;
        case REDIS_REPLY_INTEGER:
            v.type = Type::Integer;
            v.payload = static_cast<long long>(r->integer);
            break;
        case REDIS_REPLY_NIL:
            v.type = Type::Nil;
            v.payload = std::monostate{};
            break;
        case REDIS_REPLY_DOUBLE:
            v.type = Type::Double;
            v.payload = r->dval;
            break;
        case REDIS_REPLY_BOOL:
            v.type = Type::Bool;
            v.payload = (r->integer != 0);
            break;
        case REDIS_REPLY_ARRAY:
        case REDIS_REPLY_PUSH:
        case REDIS_REPLY_SET: {
            v.type = (r->type == REDIS_REPLY_ARRAY ? Type::Array : (r->type == REDIS_REPLY_PUSH ? Type::Push : Type::Set));
            Array a;
            a.reserve(r->elements);
            for (size_t i = 0; i < r->elements; ++i)
                a.push_back(fromRaw(r->element[i]));
            v.payload = std::move(a);
            break;
        }
        case REDIS_REPLY_MAP:
        case REDIS_REPLY_ATTR: {
            v.type = (r->type == REDIS_REPLY_MAP ? Type::Map : Type::Attr);
            KVList m;
            m.reserve(r->elements / 2);
            for (size_t i = 0; i + 1 < r->elements; i += 2) {
                std::string key{r->element[i]->str, static_cast<size_t>(r->element[i]->len)};
                auto val = fromRaw(r->element[i + 1]);
                m.emplace_back(std::move(key), std::move(val));
            }
            v.payload = std::move(m);
            break;
        }
        case REDIS_REPLY_BIGNUM:
            v.type = Type::Bignum;
            v.payload = std::string(r->str, r->len);
            break;
        case REDIS_REPLY_VERB:
            v.type = Type::Verb;
            v.payload = std::string(r->str, r->len);
            break;
        default:
            break;
        }
        return v;
    }

    std::string toString() const {
        return std::visit([&](auto &&x) -> std::string {
            using T = std::decay_t<decltype(x)>;
            if constexpr (std::is_same_v<T, std::monostate>)
                return "nil";
            else if constexpr (std::is_same_v<T, std::string>) {
                switch (type) {
                case Type::String:
                    return x;
                case Type::Status:
                    return std::string{"<status:"} + x + ">";
                case Type::Error:
                    return std::string{"<error:"} + x + ">";
                case Type::Bignum:
                    return std::string{"<bignum:"} + x + ">";
                case Type::Verb:
                    return std::string{"<verb:"} + x + ">";
                default:
                    return x;
                }
            } else if constexpr (std::is_same_v<T, bool>)
                return x ? "true" : "false";
            else if constexpr (std::is_same_v<T, long long>)
                return std::to_string(x);
            else if constexpr (std::is_same_v<T, double>) {
                std::ostringstream ss;
                ss << x;
                return ss.str();
            } else if constexpr (std::is_same_v<T, Array>) {
                std::ostringstream ss;
                ss << '[';
                for (size_t i = 0; i < x.size(); ++i) {
                    if (i)
                        ss << ", ";
                    ss << x[i].toString();
                }
                ss << ']';
                return ss.str();
            } else if constexpr (std::is_same_v<T, KVList>) {
                std::ostringstream ss;
                ss << '{';
                for (size_t i = 0; i < x.size(); ++i) {
                    if (i)
                        ss << ", ";
                    ss << x[i].first << ':' << x[i].second.toString();
                }
                ss << '}';
                return ss.str();
            } else
                return "<unknown>";
        },
                          payload);
    }

    // implicit conversion
    operator std::string() const {
        return toString();
    }

    // TODO think of other options
    operator bool() const {
        return type != Type::Nil && type != Type::Error;
    }
};

inline std::expected<std::string, std::error_code> string_like(const RedisValue &rv) {
    using T = RedisValue::Type;
    if (rv.type == T::String || rv.type == T::Status || rv.type == T::Bignum || rv.type == T::Verb) {
        if (auto p = std::get_if<std::string>(&rv.payload))
            return *p;
    }
    return std::unexpected(protocol_error());
}
} // namespace redis_asio

template <>
struct std::formatter<redis_asio::RedisValue> : std::formatter<std::string> {
    // parse is inherited from formatter<std::string>
    template <typename FormatContext>
    auto format(redis_asio::RedisValue const &rv, FormatContext &ctx) const {
        // format via the std::string‐formatter
        return std::formatter<std::string>::format(rv.toString(), ctx);
    }
};
