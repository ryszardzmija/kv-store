# ShaleDB
<div align="center">
  <img src="assets/logo.png" alt="ShaleDB Logo" width="300"/>
  <br/>
  <h3>Key-value storage, from the ground up</h3>
</div>

## Overview

ShaleDB is a persistent key-value store. It currently features hash-based indexing for fast lookups and is architected
for easy extension to support more sophisticated data structures.

## Quick start

### Prerequisites

- Java SDK 25+
- Gradle

### Building the Project

```bash
./gradlew build
```

## Architecture

ShaleDB is currently organized into two main modules:

- storage: Core storage engine with indexing and serialization
- server: Thin layer over the storage engine, with remote access planned
