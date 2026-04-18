package org.db.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public final class ChannelIO {

    private ChannelIO() {
    }

    // ── Sequential reads (single-threaded only) ───────────────────────────────

    public static long readInt64LE(FileChannel channel) throws IOException {
        ByteBuffer buf = ByteBuffer
                .allocate(Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("Unexpected EOF reading int64");
            }
        }
        buf.flip();
        return buf.getLong();
    }

    public static byte[] readBytes(FileChannel channel, long size) throws IOException {
        if (size > Integer.MAX_VALUE || size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size);
        }
        byte[] data = new byte[(int) size];
        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("Unexpected EOF, expected: " + size + " bytes");
            }
        }
        return data;
    }

    // ── Positional reads (concurrent-safe) ────────────────────────────────────

    /**
     * Reads 8 bytes at `position` without moving the channel cursor.
     * Returns the position after the read (position + 8).
     * Thread-safe — uses FileChannel.read(buf, position).
     */
    public static long readInt64LEAt(FileChannel channel, long position,
                                     long[] result) throws IOException {
        ByteBuffer buf = ByteBuffer
                .allocate(Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n == -1) {
                throw new IOException(
                        "Unexpected EOF at position: " + pos
                );
            }
            pos += n;
        }
        buf.flip();
        result[0] = buf.getLong();
        return pos;
    }

    /**
     * Reads `size` bytes at `position` without moving the channel cursor.
     * Returns the position after the read (position + size).
     * Thread-safe — uses FileChannel.read(buf, position).
     */
    public static long readBytesAt(FileChannel channel, long position,
                                   long size, byte[][] result) throws IOException {
        if (size > Integer.MAX_VALUE || size < 0) {
            throw new IllegalArgumentException(
                    "Invalid size: " + size + " at position: " + position
            );
        }
        byte[] data = new byte[(int) size];
        ByteBuffer buf = ByteBuffer.wrap(data);
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n == -1) {
                throw new IOException(
                        "Unexpected EOF — expected %d bytes at %d"
                                .formatted(size, position)
                );
            }
            pos += n;
        }
        result[0] = data;
        return pos;
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    public static long writeInt64LE(FileChannel fc, long v) throws IOException {
        ByteBuffer buf = ByteBuffer
                .allocate(Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(v);
        buf.flip();
        while (buf.hasRemaining()) {
            fc.write(buf);
        }
        return Long.BYTES;
    }

    public static long writeBytes(FileChannel fc, byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            fc.write(buf);
        }
        return data.length;
    }
}