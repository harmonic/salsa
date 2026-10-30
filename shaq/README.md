# shaq

[![Rust CI](https://github.com/anza-xyz/shaq/actions/workflows/ci.yml/badge.svg)](https://github.com/anza-xyz/shaq/actions/workflows/ci.yml)

shaq is a **SHAred Queue**: a simple shared-memory SPSC (Single Producer Single Consumer) and MPMC (Multi Producer Multi Consumer) FIFO queue.
It is designed for efficient inter-thread or inter-process communication using lock-free queues.

`shaq` now supports two backing modes:

- File-backed shared memory via `create` / `join`, for inter-process communication.
- In-process heap-backed queues via `spsc::pair` and `mpmc::pair`, for channel-style usage without file backing.
