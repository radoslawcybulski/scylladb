/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/scylla_test_case.hh"

#include <seastar/core/coroutine.hh>
#include "alternator/export.hh"

namespace {
// Tests for in-memory export/import pipelines

SEASTAR_TEST_CASE(test_in_memory_roundtrip_single_item) {
    auto storage = alternator::in_memory_debug_storage();
    auto sink = alternator::create_in_memory_sink_pipeline(storage);

    auto item = rjson::parse("{\"key\": \"value\"}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    BOOST_CHECK(storage.is_write_flushed());

    std::vector<rjson::value> received;
    auto source = alternator::create_in_memory_source_pipeline(storage, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read();
    co_await source->flush_and_close();

    BOOST_CHECK(storage.is_read_flushed());
    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item));
}

SEASTAR_TEST_CASE(test_in_memory_roundtrip_multiple_items) {
    auto storage = alternator::in_memory_debug_storage();
    auto sink = alternator::create_in_memory_sink_pipeline(storage);

    auto item1 = rjson::parse("{\"id\": 1, \"name\": \"alice\"}");
    auto item2 = rjson::parse("{\"id\": 2, \"name\": \"bob\"}");
    auto item3 = rjson::parse("{\"id\": 3, \"name\": \"charlie\"}");
    co_await sink->process(item1);
    co_await sink->process(item2);
    co_await sink->process(item3);
    co_await sink->flush_and_close();

    std::vector<rjson::value> received;
    auto source = alternator::create_in_memory_source_pipeline(storage, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read();
    co_await source->flush_and_close();

    BOOST_REQUIRE_EQUAL(received.size(), 3u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item1));
    BOOST_CHECK_EQUAL(rjson::print(received[1]), rjson::print(item2));
    BOOST_CHECK_EQUAL(rjson::print(received[2]), rjson::print(item3));
}

SEASTAR_TEST_CASE(test_in_memory_roundtrip_nested_json) {
    auto storage = alternator::in_memory_debug_storage();
    auto sink = alternator::create_in_memory_sink_pipeline(storage);

    auto item = rjson::parse("{\"nested\": {\"array\": [1, 2, 3], \"obj\": {\"a\": true}}}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    std::vector<rjson::value> received;
    auto source = alternator::create_in_memory_source_pipeline(storage, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read();
    co_await source->flush_and_close();

    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item));
}

SEASTAR_TEST_CASE(test_in_memory_roundtrip_empty_storage) {
    auto storage = alternator::in_memory_debug_storage();

    std::vector<rjson::value> received;
    auto source = alternator::create_in_memory_source_pipeline(storage, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read();
    co_await source->flush_and_close();

    BOOST_CHECK(storage.is_read_flushed());
    BOOST_CHECK(received.empty());
}

// rjson::print escapes special characters (e.g. \n becomes \\n in the JSON output),
// so they don't interfere with the newline-delimited format used by the pipeline.
SEASTAR_TEST_CASE(test_in_memory_roundtrip_special_characters) {
    auto storage = alternator::in_memory_debug_storage();
    auto sink = alternator::create_in_memory_sink_pipeline(storage);

    auto item = rjson::parse("{\"msg\": \"hello\\nworld\\t\\\"quoted\\\"\"}");
    co_await sink->process(item);
    co_await sink->flush_and_close();

    std::vector<rjson::value> received;
    auto source = alternator::create_in_memory_source_pipeline(storage, [&](rjson::value v) -> seastar::future<> {
        received.push_back(std::move(v));
        co_return;
    });
    co_await source->read();
    co_await source->flush_and_close();

    BOOST_REQUIRE_EQUAL(received.size(), 1u);
    BOOST_CHECK_EQUAL(rjson::print(received[0]), rjson::print(item));
}

}
