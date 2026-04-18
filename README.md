# StorageDB

A from-scratch LSM Tree (Log-Structured Merge-Tree) storage engine implemented in Java 25, translated and extended from a Go reference implementation. Built as a learning project to understand how databases like LevelDB, RocksDB, and Apache Cassandra store data on disk.

---

## What is an LSM Tree?

An LSM Tree is a data structure with performance characteristics that make it attractive for write-heavy workloads. Instead of updating data in-place on disk, every write first goes into a fast in-memory buffer (the Memtable). When the buffer fills up, it is flushed to an immutable sorted file on disk (an SSTable). Reads search these layers in order — memory first, then disk — and a background compaction process periodically merges SSTables to keep reads fast.

```
Write Path                          Read Path
──────────                          ─────────
put("dog", "woof")                  get("dog")
       │                                  │
       ▼                                  ▼
  WAL (durability)              1. Active Memtable
       │                                  │
       ▼                        2. Flushing Queue
  Memtable                                │
  (in-memory)                   3. Level 0 SSTables
       │                                  │
  [size limit]                   4. Level 1 SSTables
       │                                  │
       ▼                                  ▼
  Flushing Queue              return value or null
       │
       ▼
  Level 0 SSTable (disk)
       │
  [level overflow]
       │
       ▼
  Level 1 SSTable (disk)
       │
      ...
```

---

## Architecture

The engine is split into focused components following single-responsibility principles:

```
org.db.core/
├── LSMTree.java                  Coordinator — public API, thread lifecycle
│
├── flush/
│   └── MemtableFlusher.java      Memtable → SSTable flush pipeline
│
├── compaction/
│   └── CompactionManager.java    Leveled compaction, k-way merge
│
├── sstable/
│   ├── SSTable.java              Facade — lifecycle and get()
│   ├── SSTableWriter.java        Write path — build and persist SSTable
│   ├── SSTableReader.java        Read path — concurrent-safe positional reads
│   ├── SSTableLoader.java        Startup — discover and load SSTables from disk
│   ├── EntrySerializer.java      Marshall/unmarshall LSMEntry ↔ bytes
│   ├── IndexSerializer.java      Sparse index read/write
│   └── ChannelIO.java            Raw little-endian binary I/O
│
├── wal/
│   ├── WriteAheadLogger.java     Interface — dependency inversion
│   └── WriteAheadLog.java        Crash-recovery journal
│
└── Memtable.java                 In-memory sorted write buffer (TreeMap)

org.db.dto/
├── LSMEntry.java                 Sealed interface: Put | Tombstone
├── IndexEntry.java               Key + byte offset within SSTable
├── Levels.java                   SSTable list + RW lock per level
├── Command.java                  PUT | TOMBSTONE | WRITE_SST
└── MetadataAndBuffer.java        Intermediate write result
```

---

## Key Components

### Memtable

The active in-memory write buffer. Backed by a `TreeMap` (not `ConcurrentSkipListMap`) because all access is gated by an external `ReentrantReadWriteLock` in `LSMTree` — the concurrency overhead of a concurrent map buys nothing here.

- `put(key, value)` — inserts or overwrites a key
- `delete(key)` — writes a tombstone marker (not a removal)
- `get(key)` — returns the `LSMEntry` including tombstones; callers resolve
- `rangeScan(start, end)` — returns all entries in key range inclusive
- `getEntries()` — returns all entries in sorted key order for SSTable serialisation
- Tracks `sizeInBytes()` to trigger flush when the configured threshold is exceeded

### Write-Ahead Log (WAL)

Every `put` and `delete` is written to the WAL before touching the Memtable. On startup with `recoverFromWAL=true`, all WAL entries are replayed into a fresh Memtable, restoring state that was in memory when the process last crashed.

WAL checkpoints are written when a Memtable is successfully flushed to an SSTable — the checkpoint records the SSTable filename so recovery can skip replaying entries that are already safely on disk.

### SSTable

A Sorted String Table — an immutable, sorted, persistent file. Once written it is never modified; compaction creates new SSTable files and deletes the old ones.

**File layout (binary, little-endian throughout):**

```
┌─────────────────────────────────────┐
│  [8 bytes]  bloom filter size       │
│  [n bytes]  bloom filter data       │
│  [8 bytes]  index size              │
│  [n bytes]  index data              │  ← dataOffset points here
│  ┌─────────────────────────────┐    │
│  │ [8 bytes] entry size        │    │
│  │ [n bytes] entry data        │    │
│  │   [8] timestamp             │    │
│  │   [1] command (0/1)         │    │
│  │   [4] key length            │    │
│  │   [n] key bytes (UTF-8)     │    │
│  │   [4] value length (PUT)    │    │
│  │   [n] value bytes  (PUT)    │    │
│  ├─────────────────────────────┤    │
│  │ [8 bytes] entry size        │    │
│  │ ...                         │    │
│  └─────────────────────────────┘    │
└─────────────────────────────────────┘
```

**Read path for a single key:**

1. **Bloom filter** — O(k) hash checks. If the key is definitely absent, return immediately — no disk read.
2. **Binary search** on the sparse in-memory index — finds the closest offset at or before the target key in O(log n).
3. **Forward scan** from that offset — reads entries sequentially until the key is found or a key greater than the target is encountered.

**Concurrency:** `SSTable.get()` uses positional `FileChannel.read(ByteBuffer, position)` reads rather than `position()` + `read()`. Positional reads do not move the channel's shared cursor, making them safe to call from multiple threads on the same open `FileChannel` without any additional locking.

### Bloom Filter

A probabilistic membership test that answers "is this key definitely absent?" before any disk read. Uses three independent Murmur3-64 hash functions with different seeds and a fixed-size boolean array matching the Go reference implementation.

- False positives: possible (key appears in filter but not SSTable — triggers unnecessary disk read)
- False negatives: impossible (key in SSTable always matches filter)
- Configured at 1,000,000 slots — approximately 1% false positive rate at 50% fill

Serialised into each SSTable file as part of the header so it is restored when the SSTable is reopened after a restart.

### Leveled Compaction

SSTables are organised into levels with increasing size limits:

| Level | Max SSTables |
|-------|-------------|
| 0     | 4           |
| 1     | 8           |
| 2     | 16          |
| 3     | 32          |
| 4     | 64          |
| 5     | 128         |

When a level overflows, all SSTables at that level are merged with all SSTables at the next level into a single new SSTable. The merge uses a **k-way merge** with a min-heap (PriorityQueue) and a `TreeMap` for deduplication:

- Entries with the same key are resolved by **timestamp** — the newest write wins
- Tombstones are dropped during compaction since the merged file is at a deeper level where no older copy of the key can resurface
- Stale compaction detection: after acquiring the write lock, the engine revalidates that the source SSTables are still present before committing the merge result

**Three-phase compaction protocol:**

```
Phase 1 — read lock    Snapshot SSTable handles from src and dest levels
                       Release read lock (Java ReentrantReadWriteLock does
                       not support lock upgrading — holding a read lock while
                       acquiring a write lock deadlocks)

Phase 2 — no lock      Merge iterators, write new SSTable to disk
                       SSTables are immutable — safe without a lock

Phase 3 — write lock   Revalidate src handles still present
                       Close file handles
                       Delete old SSTable files
                       Add merged SSTable to destination level
```

### Concurrency Model

| Component | Mechanism | Reason |
|-----------|-----------|--------|
| Memtable access | `ReentrantReadWriteLock mutex` | Multiple readers, single writer |
| Flushing queue | `ReentrantReadWriteLock flushingQueueMutex` | Separate from memtable lock |
| Level SSTable lists | Per-level `ReentrantReadWriteLock` | Compaction and reads on different levels do not block each other |
| Flush channel | `LinkedBlockingQueue<Memtable>(1000)` | Mirrors Go's buffered channel |
| Compaction channel | `LinkedBlockingQueue<Integer>(100)` | Signals which level to compact |
| SSTable reads | Positional `FileChannel` reads | Thread-safe without locks |
| Sequence number | `AtomicLong` | Lock-free increment across flush and compaction threads |
| Background threads | `Thread.ofVirtual()` | Mirrors Go goroutines; virtual threads are cheap and block-friendly |
| Shutdown coordination | `CountDownLatch(2)` | Mirrors Go's `sync.WaitGroup` — both threads call `countDown()` |

**Lock ordering (always acquired in this order to prevent deadlock):**

```
mutex → levels[N].getMutex() (ascending N) → flushingQueueMutex
```

---

## Getting Started

### Prerequisites

- Java 25
- Maven or Gradle

### Opening a tree

```java
Path directory = Path.of("/data/mydb");

// Open with a 64MB memtable, no WAL recovery
LSMTree lsm = LSMTree.open(directory, 64 * 1024 * 1024, false);

// Open with WAL recovery after a crash
LSMTree lsm = LSMTree.open(directory, 64 * 1024 * 1024, true);
```

### Basic operations

```java
// Write
lsm.put("user:1001", "{\"name\":\"alice\"}".getBytes());

// Read — returns null if absent or deleted
byte[] value = lsm.get("user:1001");
if (value != null) {
    System.out.println(new String(value));
}

// Delete — writes a tombstone, not an immediate removal
lsm.delete("user:1001");

// Always close to flush the active memtable and drain background threads
lsm.close();
```

### Recommended key format

Key ordering is lexicographic. For numeric suffixes, zero-pad to ensure sort order matches numeric order:

```java
// Bad — "key-10" sorts before "key-2"
lsm.put("key-" + i, value);

// Good — "key-002" sorts correctly
lsm.put("key-%03d".formatted(i), value);
```

---

## Design Decisions

**Why Go → Java?**
The reference implementation was written in Go. Translating it to Java 25 was a deliberate exercise to compare concurrency models (goroutines vs virtual threads), type systems (sealed interfaces vs Go interfaces), and memory characteristics (GC pause sensitivity in a database context).

**Why `TreeMap` not `ConcurrentSkipListMap` for Memtable?**
`Memtable` is explicitly not thread-safe — all access is gated by `LSMTree`'s write lock. `ConcurrentSkipListMap` pays for CAS operations and memory barriers on every access for a guarantee nobody needs here. `TreeMap` is the right tool for a structure protected by an external lock.

**Why `boolean[]` not `BitSet` for BloomFilter?**
Matches the Go reference implementation's `[]bool` layout exactly, making the binary serialisation format identical. The 8x memory cost (one byte per bit rather than one bit per bit) is a known tradeoff — migrating to `BitSet` is straightforward if memory becomes a concern.

**Why positional `FileChannel` reads?**
`FileChannel.position()` followed by `channel.read()` is not atomic — two concurrent `get()` calls on the same SSTable can corrupt each other's position cursor, producing garbage data. The positional overload `channel.read(ByteBuffer, long position)` is specified by Java to be thread-safe and does not move the shared position.

**Why release read lock before write lock in compaction?**
`ReentrantReadWriteLock` does not support lock upgrading. A thread holding a read lock that attempts to acquire a write lock deadlocks — the write lock waits for all readers to release, including itself. The three-phase compaction protocol releases the read lock before acquiring the write lock, with a revalidation step after to detect stale merges.

---

## Comparison with Cassandra

This engine implements the same fundamental LSM pipeline as Apache Cassandra's storage layer (which itself originated the term "SSTable"). The key differences:

| Feature | This engine | Cassandra |
|---------|-------------|-----------|
| Data model | `key → bytes` | Partition key → rows → columns |
| Compaction strategies | Leveled only | STCS, LCS, TWCS |
| SSTable components | Single file | Data, Index, Filter, Summary, Statistics, TOC |
| Index structure | Sparse flat index | Two-level (Summary → Index) |
| Tombstone GC | Immediate on compaction | After `gc_grace_seconds` (default 10 days) |
| Distribution | Single node | Multi-node with consistent hashing |
| Caching | None | Key cache, row cache |
| Compaction throttling | None | `compaction_throughput_mb_per_sec` |

---

## Running Tests

```bash
mvn test
```

Integration tests use a fixed directory and clean it before each test run:

```java
private static final Path TEST_DIR = Path.of("/Users/raunaq/Documents/Project/Storage");
private static final long SMALL_MEMTABLE_SIZE = 256; // triggers flush quickly in tests
```

Test categories:

| Category | What is verified |
|----------|-----------------|
| Basic operations | put, get, delete, overwrite ordering |
| Persistence | WAL recovery, SSTable reload after close |
| Memtable flushing | Flush triggers, clean shutdown drain |
| Compaction | Data correctness after merge, tombstone removal, latest value wins |
| Concurrent access | No corruption under parallel reads/writes, no deadlocks |
| Edge cases | Empty values, binary data, lexicographic key ordering |

---

## References

- [The Log-Structured Merge-Tree (O'Neil et al., 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) — original LSM paper
- [goLSM](https://github.com/JyotinderSingh/goLSM) — Go based LSMTree reference
- [LevelDB Implementation Notes](https://github.com/google/leveldb/blob/main/doc/impl.md) — Google's reference implementation
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki) — production LSM engine
- [Designing Data-Intensive Applications](https://dataintensive.net/) (Kleppmann) — Chapter 3 covers LSM trees and SSTables in depth
- [Apache Cassandra Storage Engine](https://cassandra.apache.org/doc/latest/cassandra/architecture/storage_engine.html) — production LSM at scale
