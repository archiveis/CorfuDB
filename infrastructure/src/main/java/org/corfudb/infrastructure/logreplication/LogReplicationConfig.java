package org.corfudb.infrastructure.logreplication;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
@Slf4j
@Data
public class LogReplicationConfig {

    // Log Replication message timeout time in milliseconds.
    public static final int DEFAULT_TIMEOUT = 5000;

    // Log Replication default max number of message generated at the active cluster for each run cycle.
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Log Replication default max data message size is 64MB.
    public static final int DEFAULT_LOG_REPLICATION_DATA_MSG_SIZE = (64 << 20);

    /**
     * percentage of log data per log replication message
     */
    public static final int DATA_FRACTION_PER_MSG = 90;

    /*
     * Unique identifiers for all streams to be replicated across sites.
     */
    private Set<String> streamsToReplicate;

    /*
     * Snapshot Sync Batch Size Per Cycle(number of messages)
     */
    private int maxNumSnapshotMsgPerCycle;

    /*
     * The Max Size of Log Replication Data Message.
     */
    private int maxMsgSize;


    /**
     * The max size of data payload for the log replication message.
     */
    private int maxDataSizePerMsg;

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     */
    public LogReplicationConfig(Set<String> streamsToReplicate) {
        this.streamsToReplicate = streamsToReplicate;
        this.maxNumSnapshotMsgPerCycle = DEFAULT_MAX_NUM_MSG_PER_BATCH;
        this.maxMsgSize = DEFAULT_LOG_REPLICATION_DATA_MSG_SIZE;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param maxNumSnapshotMsgPerCycle snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, int maxNumSnapshotMsgPerCycle, int maxMsgSize) {
        this(streamsToReplicate);
        this.maxNumSnapshotMsgPerCycle = maxNumSnapshotMsgPerCycle;
        this.maxMsgSize = maxMsgSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
    }
}