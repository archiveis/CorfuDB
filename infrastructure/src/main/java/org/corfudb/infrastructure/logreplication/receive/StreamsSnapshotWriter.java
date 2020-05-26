package org.corfudb.infrastructure.logreplication.receive;

import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Writing a snapshot fullsync data
 * Open streams interested and append all entries
 */

@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter implements SnapshotWriter {
    final static String SHADOW_STREAM_NAME_SUFFIX = "_shadow";
    HashMap<UUID, String> streamViewMap; // It contains all the streams registered for write to.
    HashMap<UUID, String> shadowMap;
    CorfuRuntime rt;

    long siteConfigID;
    private long srcGlobalSnapshot; // The source snapshot timestamp
    private long recvSeq;
    private long shadowStreamStartAddress;
    @Getter
    private PersistedWriterMetadata persistedWriterMetadata;
    HashMap<UUID, UUID> uuidMap;
    Phase phase;


    // The sequence number of the message, it has received.
    // It is expecting the message in order of the sequence.

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config, PersistedWriterMetadata persistedWriterMetadata) {
        this.rt = rt;
        this.persistedWriterMetadata = persistedWriterMetadata;
        streamViewMap = new HashMap<>();
        uuidMap = new HashMap<>();
        shadowMap = new HashMap<>();
        phase = Phase.TransferPhase;

        for (String stream : config.getStreamsToReplicate()) {
            String shadowStream = stream + SHADOW_STREAM_NAME_SUFFIX;
            UUID streamID = CorfuRuntime.getStreamID(stream);
            UUID shadowID = CorfuRuntime.getStreamID(shadowStream);
            uuidMap.put(streamID, shadowID);
            uuidMap.put(shadowID, streamID);
            streamViewMap.put(streamID, stream);
            shadowMap.put(shadowID, shadowStream);
        }
    }

    /**
     * clear all tables registered
     * TODO: replace with stream API
     */
    void clearTables() {
        CorfuStoreMetadata.Timestamp timestamp = persistedWriterMetadata.getTimestamp();
        long persistSiteConfigID = persistedWriterMetadata.query(timestamp, PersistedWriterMetadata.PersistedWriterMetadataType.SiteConfigID);
        long persistSnapStart = persistedWriterMetadata.query(timestamp, PersistedWriterMetadata.PersistedWriterMetadataType.LastSnapStart);
        long persitSeqNum = persistedWriterMetadata.query(timestamp, PersistedWriterMetadata.PersistedWriterMetadataType.LastSnapSeqNum);

        //for transfer phase start
        if (siteConfigID != persistSiteConfigID || srcGlobalSnapshot != persistSnapStart ||
                (persitSeqNum + 1)!= recvSeq) {
            log.warn("Skip current siteConfigID " + siteConfigID + " srcGlobalSnapshot " + srcGlobalSnapshot + " currentSeqNum " + recvSeq +
                    " persistedMetadata " + persistedWriterMetadata.getSiteConfigID() + " startSnapshot " + persistedWriterMetadata.getLastSnapStartTimestamp() +
                    " lastSnapSeqNum " + persistedWriterMetadata.getLastSnapSeqNum());
            return;
        }


        TxBuilder txBuilder = persistedWriterMetadata.getTxBuilder();
        persistedWriterMetadata.appendUpdate(txBuilder, PersistedWriterMetadata.PersistedWriterMetadataType.SiteConfigID, siteConfigID);


        for (UUID streamID : streamViewMap.keySet()) {
            UUID usedStreamID = streamID;
            if (phase == Phase.TransferPhase) {
                usedStreamID = uuidMap.get(streamID);
            }

            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            txBuilder.logUpdate(usedStreamID, entry);
        }

        txBuilder.commit(timestamp);
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     * @return
     */
    void verifyMetadata(LogReplicationEntryMetadata metadata) throws ReplicationWriterException {
        if (metadata.getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE ||
                metadata.getSnapshotTimestamp() != srcGlobalSnapshot) {
            log.error("snapshot expected {} != recv snapshot {}, metadata {}",
                    srcGlobalSnapshot, metadata.getSnapshotTimestamp(), metadata);
            throw new ReplicationWriterException("Message is out of order");
        }
    }

    /**
     * Reset snapshot writer state.
     * @param snapshot
     */
    public void reset(long siteConfigID, long snapshot) {
        this.siteConfigID = siteConfigID;
        srcGlobalSnapshot = snapshot;
        recvSeq = 0;

        //clear shadow streams and remember the start address
        clearTables();
        shadowStreamStartAddress = rt.getAddressSpaceView().getLogTail();
    }


    /**
     * Write a list of SMR entries to the specified stream log.
     * @param smrEntries
     * @param currentSeqNum
     * @param dstUUID
     */
    void processOpaqueEntry(List<SMREntry> smrEntries, Long currentSeqNum, UUID dstUUID) {
        CorfuStoreMetadata.Timestamp timestamp = persistedWriterMetadata.getTimestamp();
        long persistConfigID = persistedWriterMetadata.query(timestamp, PersistedWriterMetadata.PersistedWriterMetadataType.SiteConfigID);
        long persistSnapStart = persistedWriterMetadata.query(timestamp, PersistedWriterMetadata.PersistedWriterMetadataType.LastSnapStart);
        long persitSeqNum = persistedWriterMetadata.query(timestamp, PersistedWriterMetadata.PersistedWriterMetadataType.LastSnapSeqNum);

        if (siteConfigID != persistConfigID || srcGlobalSnapshot != persistSnapStart || currentSeqNum != (persitSeqNum + 1)) {
            log.warn("Skip current siteConfigID " + siteConfigID + " srcGlobalSnapshot " + srcGlobalSnapshot + " currentSeqNum " + currentSeqNum +
                    " persistedMetadata " + persistedWriterMetadata.getSiteConfigID() + " startSnapshot " + persistedWriterMetadata.getLastSnapStartTimestamp() +
                    " lastSnapSeqNum " + persistedWriterMetadata.getLastSnapSeqNum());
            return;
        }

        TxBuilder txBuilder = persistedWriterMetadata.getTxBuilder();
        persistedWriterMetadata.appendUpdate(txBuilder, PersistedWriterMetadata.PersistedWriterMetadataType.SiteConfigID, siteConfigID);
        persistedWriterMetadata.appendUpdate(txBuilder, PersistedWriterMetadata.PersistedWriterMetadataType.LastSnapStart, srcGlobalSnapshot);
        persistedWriterMetadata.appendUpdate(txBuilder, PersistedWriterMetadata.PersistedWriterMetadataType.LastSnapSeqNum, currentSeqNum);
        for (SMREntry smrEntry : smrEntries) {
            txBuilder.logUpdate(dstUUID, smrEntry);
        }

        try {
            txBuilder.commit(timestamp);
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
        }
        log.debug("Process the entries {}  and set sequence number {} ", smrEntries, currentSeqNum);
    }

    @Override
    public void apply(LogReplicationEntry message) {
        verifyMetadata(message.getMetadata());

        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq ||
                message.getMetadata().getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE) {
            log.error("Expecting sequencer {} != recvSeq {} or wrong message type {} expecting {}",
                    message.getMetadata().getSnapshotSyncSeqNum(), recvSeq,
                    message.getMetadata().getMessageMetadataType(), MessageType.SNAPSHOT_MESSAGE);
            throw new ReplicationWriterException("Message is out of order or wrong type");
        }

        byte[] payload = message.getPayload();
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(payload));

        if (opaqueEntry.getEntries().keySet().size() != 1) {
            log.error("The opaqueEntry has more than one entry {}", opaqueEntry);
            return;
        }

        UUID uuid = opaqueEntry.getEntries().keySet().stream().findFirst().get();
        processOpaqueEntry(opaqueEntry.getEntries().get(uuid), message.getMetadata().getSnapshotSyncSeqNum(), uuidMap.get(uuid));
        recvSeq++;
    }

    @Override
    public void apply(List<LogReplicationEntry> messages) throws Exception {
        for (LogReplicationEntry msg : messages) {
            apply(msg);
        }
    }

    /**
     * Read from the shadow table and write to the real table
     * @param uuid: the real table uuid
     */
    public long applyShadowStream(UUID uuid, Long seqNum, long snapshot) {
        UUID shadowUUID = uuidMap.get(uuid);
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        //Can we do a seek after open to ignore all entries that are earlier
        Stream shadowStream = (new OpaqueStream(rt, rt.getStreamsView().get(shadowUUID, options))).streamUpTo(snapshot);

        Iterator<OpaqueEntry> iterator = shadowStream.iterator();
        while (iterator.hasNext()) {
            OpaqueEntry opaqueEntry = iterator.next();
            if (opaqueEntry.getVersion() > shadowStreamStartAddress) {
                processOpaqueEntry(opaqueEntry.getEntries().get(shadowUUID), seqNum, uuid);
                seqNum = seqNum + 1;
            }
        }

        return seqNum;
    }


    /**
     * read from shadowStream and append to the
     */
    public void applyShadowStreams(Long seqNum) {
        phase = Phase.ApplyPhase;
        long snapshot = rt.getAddressSpaceView().getLogTail();
        clearTables();
        for (UUID uuid : streamViewMap.keySet()) {
            seqNum = applyShadowStream(uuid, seqNum, snapshot);
        }
    }

    /**
     * Snapshot data has been transferred from primary node to the standby node
     * @param entry
     */
    public  void snapshotTransferDone(LogReplicationEntry entry) {
        phase = Phase.ApplyPhase;
        //verify that the snapshot Apply hasn't started yet and set it as started and set the seqNumber
        long ts = entry.getMetadata().getSnapshotTimestamp();
        long seqNum = 0;
        siteConfigID = entry.getMetadata().getSiteConfigID();

        //update the metadata
        persistedWriterMetadata.setLastSnapTransferDoneTimestamp(siteConfigID, ts);

        //get the number of entries to apply
        seqNum = 1 + persistedWriterMetadata.query(null, PersistedWriterMetadata.PersistedWriterMetadataType.LastSnapSeqNum);

        // There is no snapshot data to apply
        if (seqNum == 0)
            return;

        applyShadowStreams(seqNum);
    }
    
    enum Phase {
        TransferPhase,
        ApplyPhase
    };
}
