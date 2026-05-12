/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <vector>
#include <seastar/core/future.hh>
#include "utils/rjson.hh"

namespace s3 { class client; }

namespace alternator {

// An interface encapsulating sink pipeline stage for exporting data via DynamoDB export api (ExportTableToPointInTime call).
// The pipeline is multistage processing unit, which takes `rjson::value` item, serializes and writes it depending on the configuration.
// Currently supporting only debug in-memory pipeline, serializing to raw text JSON lines. Future will add support for S3, compression and different formats (e.g. Ion, CSV).
// Call respective factory method below to construct.
// Call `process()` method for each item (they might come in random order) - they will be serialized and written to the appropriate sink.
// After all items are processed, call `flush_and_close()` to flush the pipeline and finalize the export.
// Calling `flush_and_close()` is required and needs to be done manually.
// Pipeline is single threaded and not reentry safe.
struct export_pipeline_interface {
    // Invokes whole pipeline for a single item. The future will complete once item is processed.
    // This doesn't mean the item hit external storage, but you're free to process another item.
    virtual seastar::future<> process(const rjson::value &item) = 0;

    // Flushes and closes the pipeline. The future will complete once all items are flushed and pipeline is finalized.
    // Do not call process() after calling flush_and_close().
    virtual seastar::future<> flush_and_close() = 0;

    virtual ~export_pipeline_interface() = default;
};

// An interface encapsulating source pipeline stage for reading exported data via DynamoDB import api (GetExport call).
// Added currently for debugging purposes - so we have a consistent way to read exported data without relying on connection to S3 / DynamoDB.
// Call respective factory method below to construct.
// Call `read()` method to start reading the data - it will read some available data, pass it through the decompressor
// and parser, and call the provided callback for each parsed item.
// After all data is read, call `flush_and_close()` to flush the pipeline and finalize the import.
// Calling `flush_and_close()` is required and needs to be done manually.
// Pipeline is single threaded and not reentry safe.
struct import_pipeline_interface {
    // Reads some available data from the source, feeds it through the decompression and parsing pipeline,
    // and invokes the on_item callback for each parsed item. The future completes once at least one item has been read and processed.
    virtual seastar::future<> read() = 0;

    // Flushes and closes the pipeline. The future will complete once all data is read, flushed and pipeline is finalized.
    // Do not call read() after calling flush_and_close().
    virtual seastar::future<> flush_and_close() = 0;

    virtual ~import_pipeline_interface() = default;
};

// Simple in-memory byte buffer used for testing the export pipeline without actual S3 or compression.
// Represents content of single file. Allows both exporting and importing data.
class in_memory_debug_storage {
    std::vector<std::byte> _data;
    bool _read_flushed = false;
    bool _write_flushed = false;
public:
    void append(std::span<const std::byte> bytes) {
        _data.insert(_data.end(), bytes.begin(), bytes.end());
    }
    std::span<const std::byte> data() const { return _data; }

    // for testing calling `flush` methods - pipeline will call those methods when flush / flush_and_close is called, and we want to verify that.
    void flush_read() { _read_flushed = true; }
    void flush_write() { _write_flushed = true; }
    bool is_read_flushed() const { return _read_flushed; }
    bool is_write_flushed() const { return _write_flushed; }
};

// Create in-memory sink pipeline for a single file
// You should not use in_memory_debug_storage object for sink and storage simultaneously -
// write first, then flush, then read.
std::unique_ptr<export_pipeline_interface> create_in_memory_sink_pipeline(in_memory_debug_storage&);

// Create in-memory source pipeline for a single file
// You should not use in_memory_debug_storage object for sink and storage simultaneously -
// write first, then flush, then read.
std::unique_ptr<import_pipeline_interface> create_in_memory_source_pipeline(in_memory_debug_storage &, std::function<seastar::future<>(rjson::value)> on_item);


// Create s3 sink pipeline for a single file.
std::unique_ptr<export_pipeline_interface> create_s3_sink_pipeline(seastar::shared_ptr<s3::client> client, seastar::sstring object_name);

// Create s3 source pipeline for a single file.
std::unique_ptr<import_pipeline_interface> create_s3_source_pipeline(seastar::shared_ptr<s3::client> client, seastar::sstring object_name, std::function<seastar::future<>(rjson::value)> on_item);

} // namespace alternator
