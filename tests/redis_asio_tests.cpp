// asio_assoc_gtest.cpp
#include <boost/asio.hpp>
#include <boost/version.hpp>
#include <gtest/gtest.h>
#if BOOST_VERSION >= 108300
    #include <boost/asio/any_completion_handler.hpp>
#endif
#include <functional>

namespace asio = boost::asio;
using boost::system::error_code;
/**
 * @brief Tests for Boost.Asio's associated traits, especially with move-only.
 * Purpose of this file is just to determine if executor and cancellation_slot
 * associations are preserved or lost in various scenarios.
 */

//------------------------------------------------------------------------------
// Helper handler used in multiple tests
struct RawHandler {
    void operator()(const error_code &) const {}
};

//------------------------------------------------------------------------------
// Custom handler that stores associations such that move clears the source.
// We then specialize associated traits for it (slot + executor).
struct HotPotatoHandler {
    std::shared_ptr<asio::cancellation_signal> sig;
    std::shared_ptr<asio::any_io_executor> ex;

    void operator()(const error_code &) const {}

    HotPotatoHandler() = default;
    HotPotatoHandler(std::shared_ptr<asio::cancellation_signal> s,
                     asio::any_io_executor e)
        : sig(std::move(s)), ex(std::make_shared<asio::any_io_executor>(std::move(e))) {}

    HotPotatoHandler(HotPotatoHandler &&o) noexcept
        : sig(std::move(o.sig)), ex(std::move(o.ex)) {
        o.sig.reset();
        o.ex.reset();
    }
    HotPotatoHandler &operator=(HotPotatoHandler &&o) noexcept {
        if (this != &o) {
            sig = std::move(o.sig);
            ex = std::move(o.ex);
            o.sig.reset();
            o.ex.reset();
        }
        return *this;
    }
};

// Specialize associated traits for HotPotatoHandler.
// 1) Cancellation slot (same as earlier idea)
namespace boost {
namespace asio {
template <>
struct associated_cancellation_slot<HotPotatoHandler, cancellation_slot> {
    using type = cancellation_slot;
    static type get(const HotPotatoHandler &h,
                    const cancellation_slot & = cancellation_slot()) noexcept {
        return h.sig ? h.sig->slot() : cancellation_slot{};
    }
};
} // namespace asio
} // namespace boost

// 2) Executor: specialize for any_io_executor specifically.
//    Callers who want a concrete executor should ask for any_io_executor
//    by passing an any_io_executor fallback.
namespace boost {
namespace asio {
template <>
struct associated_executor<HotPotatoHandler, any_io_executor> {
    using type = any_io_executor;
    static type get(const HotPotatoHandler &h,
                    const any_io_executor &fallback = any_io_executor()) noexcept {
        return h.ex ? *h.ex : fallback;
    }
};
} // namespace asio
} // namespace boost

//------------------------------------------------------------------------------
// If std::move_only_function is not available, alias to std::function just to
// demonstrate the same type-erasure trait-loss behavior.
#if __has_include(<functional>) && defined(__cpp_lib_move_only_function)
using std::move_only_function; // C++23
#else
template <class Sig>
using move_only_function = std::function<Sig>;
#endif

//------------------------------------------------------------------------------
// 1) Implementation-dependent move behavior: verify moved-to keeps associations.
//    (We don't assert anything about moved-from; it varies across Asio versions.)
TEST(AsioAssoc, MovesWithHandlerAndExecutor_AssociationsPreservedInMovedTo) {
    asio::io_context io_bind, io_fb;
    asio::any_io_executor ex_bind = io_bind.get_executor();
    asio::any_io_executor ex_fb = io_fb.get_executor();
    EXPECT_TRUE(ex_bind != ex_fb); // not the same

    asio::cancellation_signal sig;
    auto h = asio::bind_executor(ex_bind,
                                 asio::bind_cancellation_slot(sig.slot(), RawHandler{}));

    // Precondition
    auto slot_before = asio::get_associated_cancellation_slot(h);
    ASSERT_TRUE(slot_before.is_connected());
    auto ex_before = asio::get_associated_executor(h, ex_fb);
    ASSERT_TRUE(ex_before == ex_bind);

    // Move
    auto h2 = std::move(h);

    // Moved-to must retain both associations
    auto slot_moved_to = asio::get_associated_cancellation_slot(h2);
    EXPECT_TRUE(slot_moved_to.is_connected());
    EXPECT_TRUE(slot_moved_to == slot_before); // same slot

    auto ex_moved_to = asio::get_associated_executor(h2, ex_fb);
    EXPECT_TRUE(ex_moved_to == ex_bind);
    EXPECT_TRUE(ex_moved_to == ex_before);

    // Cancellation still propagates via moved-to
    bool fired = false;
    slot_moved_to.assign([&](asio::cancellation_type_t) { fired = true; });
    sig.emit(asio::cancellation_type::total);
    EXPECT_TRUE(fired);
    fired = false; // reset
    slot_before.assign([&](asio::cancellation_type_t) { fired = true; });
    sig.emit(asio::cancellation_type::total);
    EXPECT_TRUE(fired);
}

//------------------------------------------------------------------------------
// 2) Type-erasure (std::function) drops both slot and executor associations.
TEST(AsioAssoc, TypeErasure_std_function_DropsSlotAndExecutor) {
    asio::io_context io_bind, io_fb;
    asio::any_io_executor ex_bind = io_bind.get_executor();
    asio::any_io_executor ex_fb = io_fb.get_executor();
    EXPECT_TRUE(ex_bind != ex_fb); // not the same

    asio::cancellation_signal sig;
    auto h = asio::bind_executor(ex_bind,
                                 asio::bind_cancellation_slot(sig.slot(), RawHandler{}));

    std::function<void(const error_code &)> f = h;

    auto slot_before = asio::get_associated_cancellation_slot(h);
    ASSERT_TRUE(slot_before.is_connected());
    auto slot_erased = asio::get_associated_cancellation_slot(f);
    EXPECT_FALSE(slot_erased.is_connected());

    auto ex_erased = asio::get_associated_executor(f, ex_fb);
    EXPECT_TRUE(ex_erased == ex_fb); // returns fallback; binding lost
}

//------------------------------------------------------------------------------
// 3) Type-erasure (std::move_only_function or fallback) also drops associations.
TEST(AsioAssoc, TypeErasure_move_only_function_DropsSlotAndExecutor) {
    asio::io_context io_bind, io_fb;
    asio::any_io_executor ex_bind = io_bind.get_executor();
    asio::any_io_executor ex_fb = io_fb.get_executor();
    EXPECT_TRUE(ex_bind != ex_fb); // not the same

    asio::cancellation_signal sig;
    auto h = asio::bind_executor(ex_bind,
                                 asio::bind_cancellation_slot(sig.slot(), RawHandler{}));


    auto slot_before = asio::get_associated_cancellation_slot(h);
    EXPECT_TRUE(slot_before.is_connected());
    move_only_function<void(const error_code &)> f = std::move(h);

    auto slot_erased = asio::get_associated_cancellation_slot(f);
    EXPECT_FALSE(slot_erased.is_connected());

    auto ex_erased = asio::get_associated_executor(f, ex_fb);
    EXPECT_TRUE(ex_erased == ex_fb); // returns fallback; binding lost
}

//------------------------------------------------------------------------------
// 4) Positive case: any_completion_handler keeps slot + executor (Boost >= 1.83).
TEST(AsioAssoc, AnyCompletionHandler_PreservesSlotAndExecutor) {
#if BOOST_VERSION >= 108300
    asio::io_context io_bind, io_fb;
    // Normalize to completion-executor on both sides
    asio::any_completion_executor ex_bind_c = io_bind.get_executor();
    asio::any_completion_executor ex_fb_c = io_fb.get_executor();
    EXPECT_TRUE(ex_bind_c != ex_fb_c); // not the same

    asio::cancellation_signal sig;
    boost::asio::any_completion_handler<void(const error_code &)> ah =
        asio::bind_executor(ex_bind_c,
                            asio::bind_cancellation_slot(sig.slot(), RawHandler{}));

    auto slot = asio::get_associated_cancellation_slot(ah);
    EXPECT_TRUE(slot.is_connected()) << "slot retained";

    auto ex = asio::get_associated_executor(ah, ex_fb_c);
    EXPECT_TRUE(ex == ex_bind_c) << "executor retained";

    bool fired = false;
    slot.assign([&](asio::cancellation_type_t) { fired = true; });
    sig.emit(asio::cancellation_type::total);
    EXPECT_TRUE(fired);
#else
    GTEST_SKIP() << "Boost too old for any_completion_handler";
#endif
}

//------------------------------------------------------------------------------
// 5) Definitive move-disconnect example: custom handler clears moved-from
//    for BOTH slot and executor. Request any_io_executor in get_associated_executor.
TEST(AsioAssoc, CustomHandler_Move_DisconnectsMovedFrom_BothSlotAndExecutor) {
    asio::io_context io_bind, io_fb;
    asio::any_io_executor ex_bind = io_bind.get_executor();
    asio::any_io_executor ex_fb = io_fb.get_executor();

    auto sig = std::make_shared<asio::cancellation_signal>();
    HotPotatoHandler h{sig, ex_bind};

    // Before move: both present
    auto slot_before = asio::get_associated_cancellation_slot(h);
    ASSERT_TRUE(slot_before.is_connected());
    auto ex_before = asio::get_associated_executor(h, ex_fb);
    ASSERT_TRUE(ex_before == ex_bind);

    // Move — our handler clears moved-from state
    auto h2 = std::move(h);

    auto slot_moved_from = asio::get_associated_cancellation_slot(h);
    auto slot_moved_to = asio::get_associated_cancellation_slot(h2);
    EXPECT_FALSE(slot_moved_from.is_connected());
    EXPECT_TRUE(slot_moved_to.is_connected());
    EXPECT_TRUE(slot_moved_from != slot_moved_to); // not the same

    auto ex_moved_from = asio::get_associated_executor(h, ex_fb);
    auto ex_moved_to = asio::get_associated_executor(h2, ex_fb);
    EXPECT_TRUE(ex_moved_from == ex_fb); // fallback (lost binding)
    EXPECT_TRUE(ex_moved_to == ex_bind); // retained
    EXPECT_TRUE(ex_moved_to != ex_moved_from); // not the same

    bool fired = false;
    slot_moved_to.assign([&](asio::cancellation_type_t) { fired = true; });
    sig->emit(asio::cancellation_type::total);
    EXPECT_TRUE(fired);
}

// --- Corrected: only assert about the moved-to object ---
TEST(AsioAssoc, CustomHandler_Move_RetainsAssociationsInMovedTo) {
  namespace asio = boost::asio;

  asio::io_context io_bind, io_fb;
  asio::any_io_executor ex_bind = io_bind.get_executor();
  asio::any_io_executor ex_fb   = io_fb.get_executor();

  auto sig = std::make_shared<asio::cancellation_signal>();
  HotPotatoHandler h{sig, ex_bind};

  // Pre-move: associations present
  ASSERT_TRUE(asio::get_associated_cancellation_slot(h).is_connected());
  ASSERT_TRUE(asio::get_associated_executor(h, ex_fb) == ex_bind);

  // Move; do not use 'h' from here on
  HotPotatoHandler h2 = std::move(h);

  // Moved-to retains both associations
  auto slot2 = asio::get_associated_cancellation_slot(h2);
  EXPECT_TRUE(slot2.is_connected());

  auto ex2 = asio::get_associated_executor(h2, ex_fb);
  EXPECT_TRUE(ex2 == ex_bind);

  // Cancellation fires exactly once on the moved-to
  bool fired = false;
  slot2.assign([&](asio::cancellation_type_t){ fired = true; });
  sig->emit(asio::cancellation_type::total);
  EXPECT_TRUE(fired);
}

// --- Double-move: only the final owner should observe cancellation ---
TEST(AsioAssoc, CustomHandler_DoubleMove_OnlyLatestReceivesCancellation) {
  namespace asio = boost::asio;

  asio::io_context io_bind, io_fb;
  asio::any_io_executor ex_bind = io_bind.get_executor();
  asio::any_io_executor ex_fb   = io_fb.get_executor();

  auto sig = std::make_shared<asio::cancellation_signal>();
  HotPotatoHandler h{sig, ex_bind};
  HotPotatoHandler h2 = std::move(h);
  HotPotatoHandler h3 = std::move(h2); // only h3 will be used/asserted

  // Executor still the one we bound initially
  auto ex3 = asio::get_associated_executor(h3, ex_fb);
  EXPECT_TRUE(ex3 == ex_bind);

  // Assign cancellation only on the final owner
  int fired = 0;
  asio::get_associated_cancellation_slot(h3)
      .assign([&](asio::cancellation_type_t){ ++fired; });

  sig->emit(asio::cancellation_type::total);
  EXPECT_EQ(fired, 1);
}