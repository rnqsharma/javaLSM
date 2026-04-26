package org.db.io;

import java.io.IOException;

/**
 * Contract for binary serialisation of domain objects.
 *
 * Type parameter T — the domain object being serialised.
 *
 * Dependency Inversion: callers depend on this abstraction
 * rather than concrete serializer implementations.
 * Open/Closed: new serializers (e.g. ProtobufSerializer) can be
 * added without modifying existing code.
 *
 * marshall() declares IOException because some implementations
 * (LSMEntrySerializer) use DataOutputStream which throws it.
 * WALEntrySerializer uses ByteBuffer which does not throw —
 * the IOException is simply never thrown in that implementation.
 */
public interface EntrySerializer<T> {

    /**
     * Serialises a domain object to raw bytes.
     * These bytes are written to disk — SSTable entries or WAL data field.
     */
    byte[] marshall(T entry) throws IOException;

    /**
     * Deserialises raw bytes back into a domain object.
     * Called during SSTable reads or WAL recovery.
     */
    T unmarshall(byte[] data);
}