#include <gtest/gtest.h>
#include <hiredis/hiredis.h>

#include "redis_async.hpp"
#include "redis_log.hpp"
#include "redis_value.hpp"

using redis_asio::RedisValue;

TEST(RedisValue, FromRawBasicScalars) {
    // String
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_STRING;
        r->str = const_cast<char *>("hello");
        r->len = 5;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::String);
        EXPECT_EQ(std::get<std::string>(v.payload), "hello");
        delete r;
    }
    // Integer
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_INTEGER;
        r->integer = 42;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Integer);
        EXPECT_EQ(std::get<long long>(v.payload), 42);
        delete r;
    }
    // Nil
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_NIL;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Nil);
        EXPECT_TRUE(std::holds_alternative<std::monostate>(v.payload));
        delete r;
    }
    // Bool
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_BOOL;
        r->integer = 1;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Bool);
        EXPECT_TRUE(std::get<bool>(v.payload));
        delete r;
    }
    // Double
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_DOUBLE;
        r->dval = 3.5;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Double);
        EXPECT_DOUBLE_EQ(std::get<double>(v.payload), 3.5);
        delete r;
    }
    // Error
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_ERROR;
        r->str = const_cast<char *>("ERR oops");
        r->len = 8;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Error);
        EXPECT_NE(v.toString().find("error:ERR oops"), std::string::npos);
        delete r;
    }
    // Status
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_STATUS;
        r->str = const_cast<char *>("OK");
        r->len = 2;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Status);
        EXPECT_EQ(std::get<std::string>(v.payload), "OK");
        EXPECT_NE(v.toString().find("<status:OK>"), std::string::npos);
        delete r;
    }
    // Bignum
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_BIGNUM;
        r->str = const_cast<char *>("12345678901234567890");
        r->len = 20;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Bignum);
        EXPECT_EQ(std::get<std::string>(v.payload), "12345678901234567890");
        delete r;
    }
    // Verb
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_VERB;
        r->str = const_cast<char *>("txt");
        r->len = 3;
        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Verb);
        EXPECT_NE(v.toString().find("<verb:txt>"), std::string::npos);
        delete r;
    }
}

TEST(RedisValue, FromRawArraysSetsPush) {
    // Array: ["a", 7]
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_ARRAY;
        r->elements = 2;
        r->element = static_cast<redisReply **>(calloc(2, sizeof(redisReply *)));
        r->element[0] = new redisReply{};
        r->element[0]->type = REDIS_REPLY_STRING;
        r->element[0]->str = const_cast<char *>("a");
        r->element[0]->len = 1;
        r->element[1] = new redisReply{};
        r->element[1]->type = REDIS_REPLY_INTEGER;
        r->element[1]->integer = 7;

        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Array);
        auto &arr = std::get<RedisValue::Array>(v.payload);
        ASSERT_EQ(arr.size(), 2u);
        EXPECT_EQ(arr[0].type, RedisValue::Type::String);
        EXPECT_EQ(std::get<std::string>(arr[0].payload), "a");
        EXPECT_EQ(arr[1].type, RedisValue::Type::Integer);
        EXPECT_EQ(std::get<long long>(arr[1].payload), 7);

        delete r->element[0];
        delete r->element[1];
        free(r->element);
        delete r;
    }
    // Push: ["push","channel","payload"] (shape varies, but treat as Array)
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_PUSH;
        r->elements = 3;
        r->element = static_cast<redisReply **>(calloc(3, sizeof(redisReply *)));
        const char *s0 = "message";
        const char *s1 = "c";
        const char *s2 = "p";
        r->element[0] = new redisReply{};
        r->element[0]->type = REDIS_REPLY_STRING;
        r->element[0]->str = const_cast<char *>(s0);
        r->element[0]->len = 7;
        r->element[1] = new redisReply{};
        r->element[1]->type = REDIS_REPLY_STRING;
        r->element[1]->str = const_cast<char *>(s1);
        r->element[1]->len = 1;
        r->element[2] = new redisReply{};
        r->element[2]->type = REDIS_REPLY_STRING;
        r->element[2]->str = const_cast<char *>(s2);
        r->element[2]->len = 1;

        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Push);
        auto &arr = std::get<RedisValue::Array>(v.payload);
        ASSERT_EQ(arr.size(), 3u);
        EXPECT_EQ(std::get<std::string>(arr[1].payload), "c");
        EXPECT_EQ(std::get<std::string>(arr[2].payload), "p");

        for (size_t i = 0; i < 3; i++)
            delete r->element[i];
        free(r->element);
        delete r;
    }
}

TEST(RedisValue, FromRawMapsAndAttr) {
    // Map: {"k":"v"}
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_MAP;
        r->elements = 2;
        r->element = static_cast<redisReply **>(calloc(2, sizeof(redisReply *)));
        r->element[0] = new redisReply{};
        r->element[0]->type = REDIS_REPLY_STRING;
        r->element[0]->str = const_cast<char *>("k");
        r->element[0]->len = 1;
        r->element[1] = new redisReply{};
        r->element[1]->type = REDIS_REPLY_STRING;
        r->element[1]->str = const_cast<char *>("v");
        r->element[1]->len = 1;

        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Map);
        auto &kv = std::get<RedisValue::KVList>(v.payload);
        ASSERT_EQ(kv.size(), 1u);
        EXPECT_EQ(kv[0].first, "k");
        EXPECT_EQ(std::get<std::string>(kv[0].second.payload), "v");

        delete r->element[0];
        delete r->element[1];
        free(r->element);
        delete r;
    }

    // Attr: behaves like Map in our representation
    {
        auto r = new redisReply{};
        r->type = REDIS_REPLY_ATTR;
        r->elements = 2;
        r->element = static_cast<redisReply **>(calloc(2, sizeof(redisReply *)));
        r->element[0] = new redisReply{};
        r->element[0]->type = REDIS_REPLY_STRING;
        r->element[0]->str = const_cast<char *>("meta");
        r->element[0]->len = 4;
        r->element[1] = new redisReply{};
        r->element[1]->type = REDIS_REPLY_INTEGER;
        r->element[1]->integer = 9;

        RedisValue v = RedisValue::fromRaw(r);
        EXPECT_EQ(v.type, RedisValue::Type::Attr);
        auto &kv = std::get<RedisValue::KVList>(v.payload);
        ASSERT_EQ(kv.size(), 1u);
        EXPECT_EQ(kv[0].first, "meta");
        EXPECT_EQ(std::get<long long>(kv[0].second.payload), 9);

        delete r->element[0];
        delete r->element[1];
        free(r->element);
        delete r;
    }
}

TEST(RedisValue, ToStringAndHelpers) {
    // toString shapes
    {
        RedisValue s;
        s.type = RedisValue::Type::String;
        s.payload = std::string("foo");
        EXPECT_EQ(s.toString(), "foo");
        RedisValue st;
        st.type = RedisValue::Type::Status;
        st.payload = std::string("PONG");
        EXPECT_NE(st.toString().find("<status:PONG>"), std::string::npos);
        RedisValue e;
        e.type = RedisValue::Type::Error;
        e.payload = std::string("ERR bad");
        EXPECT_NE(e.toString().find("<error:ERR bad>"), std::string::npos);
    }
    // string_like helper
    {
        RedisValue s;
        s.type = RedisValue::Type::String;
        s.payload = std::string("abc");
        auto ok = redis_asio::string_like(s);
        ASSERT_TRUE(ok.has_value());
        EXPECT_EQ(*ok, "abc");

        RedisValue i;
        i.type = RedisValue::Type::Integer;
        i.payload = 5LL;
        auto bad = redis_asio::string_like(i);
        EXPECT_FALSE(bad.has_value());
    }
    // implicit string conversion
    {
        RedisValue s;
        s.type = RedisValue::Type::String;
        s.payload = std::string("xyz");
        std::string t = s;
        EXPECT_EQ(t, "xyz");
    }
    // implicit bool conversion
    {
        RedisValue n;
        n.type = RedisValue::Type::Nil;
        n.payload = std::monostate{};
        EXPECT_FALSE(static_cast<bool>(n));
        RedisValue b;
        b.type = RedisValue::Type::Bool;
        b.payload = true;
        EXPECT_TRUE(static_cast<bool>(b));
    }
}
