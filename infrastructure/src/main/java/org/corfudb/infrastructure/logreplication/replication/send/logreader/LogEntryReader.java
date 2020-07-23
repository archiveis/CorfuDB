package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.UUID;

/**
 * An Interface for Log Entry Reader
 *
 * A log entry logreader provides the functionality for reading incremental updates from Corfu.
 */
public interface LogEntryReader {

    /**
     * Read a Log Entry.
     *
     * @param logEntryRequestId unique identifier of log entry sync request.
     *
     * @return a log replication entry.
     */
    LogReplicationEntry read(UUID logEntryRequestId);

    void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp);

    void setTopologyConfigId(long topologyConfigId);

    boolean hasNoiseData();
}