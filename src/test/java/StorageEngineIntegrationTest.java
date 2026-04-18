import org.db.core.LSMTree;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for LSMTree.
 *
 * Uses @TempDir to create a fresh directory for each test —
 * mirrors Go's t.TempDir() pattern.
 *
 * Test categories:
 *   1. Basic operations      — Put, Get, Delete
 *   2. Persistence           — WAL recovery, SSTable reload
 *   3. Memtable flushing     — flush triggers, size thresholds
 *   4. Compaction            — level overflow, merged correctness
 *   5. Concurrent access     — parallel reads and writes
 *   6. Edge cases            — empty values, large keys, overwrite ordering
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StorageEngineIntegrationTest {

    // Small memtable size so flush triggers are easy to hit in tests
    private static final long SMALL_MEMTABLE_SIZE = 256;
    private static final long LARGE_MEMTABLE_SIZE = 64 * 1024 * 1024; // 64MB

    private static final Path TEST_DIR = Path.of("/Users/raunaq/Documents/Project/Storage");

    private LSMTree lsm;

    @BeforeEach
    void setUp() throws IOException {
        lsm = LSMTree.open(TEST_DIR, LARGE_MEMTABLE_SIZE, false);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (lsm != null) {
            lsm.close();
        }
    }

    // ── 1. Basic operations ───────────────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("Put and Get returns correct value")
    void testPutAndGet() throws IOException {
        lsm.put("name", "alice".getBytes());

        byte[] result = lsm.get("name");

        assertNotNull(result);
        assertEquals("alice", new String(result));
    }

    @Test
    @Order(2)
    @DisplayName("Get returns null for missing key")
    void testGetMissingKey() throws IOException {
        byte[] result = lsm.get("nonexistent");

        assertNull(result);
    }

    @Test
    @Order(3)
    @DisplayName("Delete makes key return null")
    void testDelete() throws IOException {
        lsm.put("key", "value".getBytes());
        lsm.delete("key");

        byte[] result = lsm.get("key");

        assertNull(result);
    }

    @Test
    @Order(4)
    @DisplayName("Overwrite returns latest value")
    void testOverwrite() throws IOException {
        lsm.put("key", "v1".getBytes());
        lsm.put("key", "v2".getBytes());
        lsm.put("key", "v3".getBytes());

        byte[] result = lsm.get("key");

        assertNotNull(result);
        assertEquals("v3", new String(result));
    }

    @Test
    @Order(5)
    @DisplayName("Put after Delete returns new value")
    void testPutAfterDelete() throws IOException {
        lsm.put("key", "v1".getBytes());
        lsm.delete("key");
        lsm.put("key", "v2".getBytes());

        byte[] result = lsm.get("key");

        assertNotNull(result);
        assertEquals("v2", new String(result));
    }

    @Test
    @Order(6)
    @DisplayName("Multiple keys are independent")
    void testMultipleKeys() throws IOException {
        lsm.put("a", "1".getBytes());
        lsm.put("b", "2".getBytes());
        lsm.put("c", "3".getBytes());

        assertEquals("1", new String(lsm.get("a")));
        assertEquals("2", new String(lsm.get("b")));
        assertEquals("3", new String(lsm.get("c")));
    }

    @Test
    @Order(7)
    @DisplayName("Empty value is stored and retrieved correctly")
    void testEmptyValue() throws IOException {
        lsm.put("key", new byte[0]);

        byte[] result = lsm.get("key");

        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    @Order(8)
    @DisplayName("Delete on non-existent key does not throw")
    void testDeleteNonExistentKey() {
        assertDoesNotThrow(() -> lsm.delete("ghost"));
    }

    // ── 2. Persistence ────────────────────────────────────────────────────────

    @Test
    @Order(9)
    @DisplayName("Data survives WAL recovery after close")
    void testWALRecovery() throws IOException {
        // Write data and close — simulates crash before memtable flush
        lsm.put("survivor", "yes".getBytes());
        lsm.close();

        // Reopen with WAL recovery
        LSMTree recovered = LSMTree.open(TEST_DIR, LARGE_MEMTABLE_SIZE, true);
        try {
            byte[] result = recovered.get("survivor");
            assertNotNull(result);
            assertEquals("yes", new String(result));
        } finally {
            recovered.close();
        }
    }

    @Test
    @Order(10)
    @DisplayName("Tombstone survives WAL recovery")
    void testTombstoneRecovery() throws IOException {
        lsm.put("key", "value".getBytes());
        lsm.delete("key");
        lsm.close();

        LSMTree recovered = LSMTree.open(TEST_DIR, LARGE_MEMTABLE_SIZE, true);
        try {
            assertNull(recovered.get("key"));
        } finally {
            recovered.close();
        }
    }

    @Test
    @Order(11)
    @DisplayName("Data survives SSTable reload after close")
    void testSSTableReload() throws IOException, InterruptedException {
        // Use small memtable to force flush to SSTable
        lsm.close();
        lsm = LSMTree.open(TEST_DIR, SMALL_MEMTABLE_SIZE, false);

        // Write enough data to trigger flush
        writeNEntries(lsm, 100);

        // Wait for background flush to complete
        waitForFlush();

        lsm.close();

        // Reopen without WAL recovery — data must come from SSTables
        LSMTree reloaded = LSMTree.open(TEST_DIR, LARGE_MEMTABLE_SIZE, false);
        try {
            for (int i = 0; i < 100; i++) {
                byte[] result = reloaded.get("key-" + i);
                assertNotNull(result, "key-" + i + " should exist after reload");
                assertEquals("value-" + i, new String(result));
            }
        } finally {
            reloaded.close();
        }
    }

    // ── 3. Memtable flushing ──────────────────────────────────────────────────

    @Test
    @Order(12)
    @DisplayName("Data readable immediately after flush trigger")
    void testReadAfterFlush() throws IOException, InterruptedException {
        lsm.close();
        lsm = LSMTree.open(TEST_DIR, SMALL_MEMTABLE_SIZE, false);

        writeNEntries(lsm, 200);
        waitForFlush();

        // All entries should still be readable after flush
        for (int i = 0; i < 200; i++) {
            byte[] result = lsm.get("key-" + i);
            assertNotNull(result, "key-" + i + " missing after flush");
            assertEquals("value-" + i, new String(result));
        }
    }

    @Test
    @Order(13)
    @DisplayName("Flushing queue drains before shutdown")
    void testFlushingQueueDrainsOnClose() throws IOException, InterruptedException {
        lsm.close();
        lsm = LSMTree.open(TEST_DIR, SMALL_MEMTABLE_SIZE, false);

        // Write enough to queue multiple flushes
        writeNEntries(lsm, 500);

        // Close immediately — should drain flushing queue before exiting
        lsm.close();

        // Reopen and verify all data present on disk
        LSMTree reloaded = LSMTree.open(TEST_DIR, LARGE_MEMTABLE_SIZE, false);
        try {
            for (int i = 0; i < 500; i++) {
                assertNotNull(reloaded.get("key-" + i), "key-" + i + " lost on shutdown");
            }
        } finally {
            reloaded.close();
            lsm = null;
        }
    }

    // ── 4. Compaction ─────────────────────────────────────────────────────────

    @Test
    @Order(14)
    @DisplayName("Data correct after Level 0 compaction")
    void testDataCorrectAfterCompaction() throws IOException, InterruptedException {
        lsm.close();
        // Very small memtable to trigger many flushes and compaction
        lsm = LSMTree.open(TEST_DIR, SMALL_MEMTABLE_SIZE, false);

        // Write enough to trigger Level 0 → Level 1 compaction (needs > 4 SSTables)
        writeNEntries(lsm, 1000);
        waitForCompaction();

        // All data must be correct after compaction
        for (int i = 0; i < 1000; i++) {
            byte[] result = lsm.get("key-" + i);
            assertNotNull(result, "key-" + i + " missing after compaction");
            assertEquals("value-" + i, new String(result));
        }
    }

    @Test
    @Order(15)
    @DisplayName("Compaction merges overwrites correctly — latest value wins")
    void testCompactionKeepsLatestValue() throws IOException, InterruptedException {
        lsm.close();
        lsm = LSMTree.open(TEST_DIR, SMALL_MEMTABLE_SIZE, false);

        // Write v1, force flush, write v2, force flush, trigger compaction
        lsm.put("key", "v1".getBytes());
        writeNEntries(lsm, 100); // pad to trigger flush
        waitForFlush();

        lsm.put("key", "v2".getBytes());
        writeNEntries(lsm, 100);
        waitForFlush();

        waitForCompaction();

        byte[] result = lsm.get("key");
        assertNotNull(result);
        assertEquals("v2", new String(result));
    }

    @Test
    @Order(16)
    @DisplayName("Compaction removes tombstones — deleted key stays deleted")
    void testCompactionRemovesTombstones() throws IOException, InterruptedException {
        lsm.close();
        lsm = LSMTree.open(TEST_DIR, SMALL_MEMTABLE_SIZE, false);

        lsm.put("ghost", "haunt".getBytes());
        writeNEntries(lsm, 100);
        waitForFlush();

        lsm.delete("ghost");
        writeNEntries(lsm, 100);
        waitForFlush();

        waitForCompaction();

        assertNull(lsm.get("ghost"), "Deleted key should remain null after compaction");
    }

    // ── 5. Concurrent access ──────────────────────────────────────────────────

    @Test
    @Order(17)
    @DisplayName("Concurrent writes do not corrupt data")
    void testConcurrentWrites() throws InterruptedException, IOException {
        int threadCount  = 10;
        int writesPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch  latch    = new CountDownLatch(1);
        List<Throwable> errors   = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    latch.await(); // all threads start simultaneously
                    for (int i = 0; i < writesPerThread; i++) {
                        String key   = "thread-" + threadId + "-key-" + i;
                        String value = "thread-" + threadId + "-value-" + i;
                        lsm.put(key, value.getBytes());
                    }
                } catch (Throwable e) {
                    synchronized (errors) { errors.add(e); }
                }
            });
        }

        latch.countDown(); // release all threads at once
        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue(errors.isEmpty(), "Errors during concurrent writes: " + errors);

        // Verify all written keys are readable
        for (int t = 0; t < threadCount; t++) {
            for (int i = 0; i < writesPerThread; i++) {
                String key      = "thread-" + t + "-key-" + i;
                String expected = "thread-" + t + "-value-" + i;
                byte[] result   = lsm.get(key);
                assertNotNull(result, key + " missing after concurrent writes");
                assertEquals(expected, new String(result));
            }
        }
    }

    @Test
    @Order(18)
    @DisplayName("Concurrent reads and writes do not deadlock")
    void testConcurrentReadsAndWrites() throws InterruptedException {
        int threadCount = 10;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch  latch    = new CountDownLatch(1);
        List<Throwable> errors   = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    latch.await();
                    if (threadId % 2 == 0) {
                        // Writer thread
                        for (int i = 0; i < 100; i++) {
                            lsm.put("key-" + i, ("value-" + i).getBytes());
                        }
                    } else {
                        // Reader thread — reads may return null (written concurrently)
                        for (int i = 0; i < 100; i++) {
                            lsm.get("key-" + i); // just must not throw or deadlock
                        }
                    }
                } catch (Throwable e) {
                    synchronized (errors) { errors.add(e); }
                }
            });
        }

        latch.countDown();
        executor.shutdown();
        assertTrue(
                executor.awaitTermination(30, TimeUnit.SECONDS),
                "Deadlock detected — executor did not terminate"
        );
        assertTrue(errors.isEmpty(), "Errors during concurrent reads/writes: " + errors);
    }

    @Test
    @Order(19)
    @DisplayName("Concurrent deletes do not corrupt surviving keys")
    void testConcurrentDeletes() throws InterruptedException, IOException {
        // Pre-populate
        for (int i = 0; i < 200; i++) {
            lsm.put("key-" + i, ("value-" + i).getBytes());
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch  latch    = new CountDownLatch(1);

        // Delete even keys concurrently
        for (int t = 0; t < 4; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    latch.await();
                    for (int i = threadId; i < 200; i += 4) {
                        if (i % 2 == 0) lsm.delete("key-" + i);
                    }
                } catch (Throwable ignored) {}
            });
        }

        latch.countDown();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Odd keys must survive, even keys must be gone
        for (int i = 0; i < 200; i++) {
            if (i % 2 == 0) {
                assertNull(lsm.get("key-" + i), "Even key-" + i + " should be deleted");
            } else {
                assertNotNull(lsm.get("key-" + i), "Odd key-" + i + " should survive");
            }
        }
    }

    // ── 6. Edge cases ─────────────────────────────────────────────────────────

    @Test
    @Order(20)
    @DisplayName("Large value is stored and retrieved correctly")
    void testLargeValue() throws IOException {
        byte[] largeValue = new byte[1024 * 1024]; // 1MB
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte)(i % 256);
        }

        lsm.put("large", largeValue);

        byte[] result = lsm.get("large");
        assertNotNull(result);
        assertArrayEquals(largeValue, result);
    }

    @Test
    @Order(21)
    @DisplayName("Large number of unique keys all retrievable")
    void testLargeNumberOfKeys() throws IOException {
        int keyCount = 10_000;
        for (int i = 0; i < keyCount; i++) {
            lsm.put("key-" + i, ("value-" + i).getBytes());
        }

        for (int i = 0; i < keyCount; i++) {
            byte[] result = lsm.get("key-" + i);
            assertNotNull(result, "key-" + i + " missing");
            assertEquals("value-" + i, new String(result));
        }
    }

    @Test
    @Order(22)
    @DisplayName("Keys with similar prefixes do not collide")
    void testSimilarPrefixKeys() throws IOException {
        lsm.put("key",    "v1".getBytes());
        lsm.put("key1",   "v2".getBytes());
        lsm.put("key12",  "v3".getBytes());
        lsm.put("key123", "v4".getBytes());

        assertEquals("v1", new String(lsm.get("key")));
        assertEquals("v2", new String(lsm.get("key1")));
        assertEquals("v3", new String(lsm.get("key12")));
        assertEquals("v4", new String(lsm.get("key123")));
    }

    @Test
    @Order(23)
    @DisplayName("Binary values with null bytes stored correctly")
    void testBinaryValues() throws IOException {
        byte[] binary = { 0x00, 0x01, 0x02, (byte)0xFF, 0x00 };
        lsm.put("binary", binary);

        byte[] result = lsm.get("binary");
        assertNotNull(result);
        assertArrayEquals(binary, result);
    }

    @Test
    @Order(24)
    @DisplayName("Reopen without recovery does not see unflushed memtable data")
    void testReopenWithoutRecoveryLosesMemtableData() throws IOException {
        // Write but do not flush (large memtable threshold)
        lsm.put("unflushed", "lost".getBytes());
        lsm.close();

        // Reopen WITHOUT WAL recovery
        LSMTree reloaded = LSMTree.open(TEST_DIR, LARGE_MEMTABLE_SIZE, false);
        try {
            // Data was only in memtable, no WAL recovery — should be gone
            assertNull(reloaded.get("unflushed"),
                    "Data should not appear without WAL recovery");
        } finally {
            reloaded.close();
            lsm = null;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void writeNEntries(LSMTree tree, int n) throws IOException {
        for (int i = 0; i < n; i++) {
            tree.put("key-" + i, ("value-" + i).getBytes());
        }
    }

    /**
     * Waits for background flush to complete.
     * Polls until the flushing queue is empty or timeout is reached.
     */
    private void waitForFlush() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (lsm.getFlushingChan().isEmpty()) return;
            Thread.sleep(50);
        }
        fail("Timed out waiting for memtable flush");
    }

    /**
     * Waits for background compaction to complete.
     * Polls until the compaction channel is empty or timeout is reached.
     */
    private void waitForCompaction() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (lsm.getCompactionChan().isEmpty()) return;
            Thread.sleep(100);
        }
        fail("Timed out waiting for compaction");
    }
}