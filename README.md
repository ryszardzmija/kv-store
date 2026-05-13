# ShaleDB

> ShaleDB has been superseded by the Go rewrite at [ryszardzmija/shaledb-go](https://github.com/ryszardzmija/shaledb-go). New work is happening there, with the distribution layer as the main focus.

<div align="center">
  <img src="assets/logo.png" alt="ShaleDB Logo" width="300"/>
  <p>
    <a href="https://github.com/ryszardzmija/shaledb/actions/workflows/ci.yaml">
      <img src="https://github.com/ryszardzmija/shaledb/actions/workflows/ci.yaml/badge.svg" alt="CI"/>
    </a>
  </p>
</div>

## Overview

ShaleDB is a persistent key-value store written in Java. It implements a local storage engine with append-only segment files, in-memory hash indexes, CRC32C-validated records, tombstone deletes, size-based segment rollover, startup index rebuilds, and validated YAML configuration. The store is also available through a small gRPC API for `Get`, `Put`, and `Delete`.

## Building and running

ShaleDB requires JDK 25 or newer and uses Bazel for builds. Bazelisk is recommended because it reads `.bazelversion` and runs the pinned Bazel version for this repository.

Run the test suite with:

```bash
bazel test //:tests
```

Build the project with:

```bash
bazel build //server:server
```

Run the server with:

```bash
bazel run //server:server
```

The server reads `config/application.yaml` by default, stores segment files under `data/segments`, and exposes the gRPC API on port `50001`.

The gRPC API is defined in `api/src/main/proto/shaledb/key_value_store.proto` as `shaledb.v1.KeyValueStore`. It exposes three operations:

- `Get`
- `Put`
- `Delete`

## Project structure

The `api` module defines the `shaledb.v1.KeyValueStore` gRPC service and generated Java bindings.

The `storage` module contains the key-value store implementation. It owns the public `KeyValueStore` API, the hash-indexed storage engine, segment files, record serialization, durability handling, and configuration validation.

The `server` module contains the application entry point, configuration loading, and the gRPC service that connects the network API to the storage engine.

## Storage design

Writes are append-only. A `put` writes a normal record, while a `delete` writes a tombstone record. The mutable segment accepts new writes until it reaches the configured size limit, then ShaleDB rolls over to a new mutable segment and keeps previous segments immutable.

Each segment has an in-memory hash index that maps keys to offsets in the segment file. Reads check the mutable segment first, then immutable segments from newest to oldest, so the newest record for a key wins. This matters for deletes: a tombstone in a newer segment must shadow an older value.

Records include a CRC32C checksum, record type, key length, value length, key bytes, and value bytes. During reads and startup index rebuilds, invalid record types, oversized payloads, truncated records, and checksum mismatches are treated as corruption.

The current durability mode is `SYNC_EACH_WRITE`. Each write is flushed before the in-memory index is updated, which gives the engine a simple recovery boundary.

## Configuration

Runtime storage settings live in `config/application.yaml`. The configuration controls the maximum segment size, maximum payload size, segment directory, and durability mode. Configuration is validated at startup so missing fields or malformed YAML fail early.

## Current limitations

The current implementation should be treated as single-threaded. The gRPC service serializes storage operations through a single executor, and the storage engine does not support compaction, transactions, snapshots, or range scans. Startup rebuilds indexes by scanning segment files, so startup time grows with the amount of data on disk.
