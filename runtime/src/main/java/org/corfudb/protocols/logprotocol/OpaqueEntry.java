package org.corfudb.protocols.logprotocol;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.Address;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.corfudb.util.serializer.CorfuSerializer.corfuPayloadMagic;

/**
 *
 *
 * Created by Maithem on 2/3/20.
 */
public class OpaqueEntry {

    private static OpaqueEntry empty = new OpaqueEntry(Address.NON_EXIST, Collections.emptyMap());

    @Getter
    final Map<UUID, List<SMREntry>> entries;

    @Getter
    // TODO(Maithem): Inconsistent behavior when full-sync vs delta (for full sync the versions will change)
    final long version;

    public OpaqueEntry(long version, Map<UUID, List<SMREntry>> updates) {
        this.entries = updates;
        this.version = version;
    }

    public static void serialize(ByteBuf buf, OpaqueEntry entry) {
        buf.writeLong(entry.getVersion());
        buf.writeInt(entry.getEntries().size());
        for (Map.Entry<UUID, List<SMREntry>> streamUpdates : entry.getEntries().entrySet()) {
            UUID streamId = streamUpdates.getKey();
            List<SMREntry> streamEntries = streamUpdates.getValue();
            buf.writeLong(streamId.getMostSignificantBits());
            buf.writeLong(streamId.getLeastSignificantBits());
            buf.writeInt(streamEntries.size());
            for (SMREntry smrEntry : streamEntries) {
                smrEntry.serialize(buf);
            }
        }
    }

    public static OpaqueEntry deserialize(ByteBuf buf) {
        long version = buf.readLong();
        int numStreams = buf.readInt();
        Map<UUID, List<SMREntry>> updates = new HashMap<>(numStreams);

        for (int x = 0; x < numStreams; x++) {
            UUID streamId = new UUID(buf.readLong(), buf.readLong());
            int numStreamUpdates = buf.readInt();
            List<SMREntry> streamUpdates = new ArrayList<>(numStreamUpdates);
            for (int i = 0; i < numStreamUpdates; i++) {
                streamUpdates.add((SMREntry) SMREntry.deserialize(buf, null, true));
            }
            updates.put(streamId, streamUpdates);
        }

        return new OpaqueEntry(version, updates);
    }

    public static OpaqueEntry unpack(ILogData logData) {
        byte[] payload = ((LogData) logData).getData();
        if (payload == null) return empty;
        // what if payload is null ?
        ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);

        if (logData.hasPayloadCodec()) {
            // if the payload has a codec we need to decode it before deserialization
            ByteBuf compressedBuf = ICorfuPayload.fromBuffer(payload, ByteBuf.class);
            byte[] compressedArrayBuf= new byte[compressedBuf.readableBytes()];
            compressedBuf.readBytes(compressedArrayBuf);
            payloadBuf = Unpooled.wrappedBuffer(logData.getPayloadCodecType()
                    .getInstance().decompress(ByteBuffer.wrap(compressedArrayBuf)));
        }

        if (payloadBuf.readByte() != corfuPayloadMagic) {
            throw new IllegalStateException("Must be ICorfuSerializable");
        }

        long version = logData.getGlobalAddress();

        LogEntry entry = (LogEntry) LogEntry.deserialize(payloadBuf, null, true);
        Map<UUID, List<SMREntry>> res = new HashMap<>();

        switch (entry.getType()) {
            // TODO(Maithem) : what if there's always a transaction stream
            case SMR:
                UUID id =  Iterables.getOnlyElement(logData.getStreams());
                ArrayList<SMREntry> smrEntry = Lists.newArrayList((SMREntry) entry);
                res.put(id, smrEntry);
                break;
            case MULTISMR:
                id =  Iterables.getOnlyElement(logData.getStreams());
                MultiSMREntry multiSMREntry = (MultiSMREntry) entry;
                res.put(id, multiSMREntry.getUpdates());
                break;
            case MULTIOBJSMR:
                MultiObjectSMREntry multiObjectSMREntry = (MultiObjectSMREntry) entry;
                for (Map.Entry<UUID, MultiSMREntry> mapEntry : multiObjectSMREntry.getEntryMap().entrySet()) {
                    res.put(mapEntry.getKey(), mapEntry.getValue().getUpdates());
                }
                break;
            case CHECKPOINT:
                CheckpointEntry cpEntries = (CheckpointEntry) entry;
                if (cpEntries.getCpType() != CheckpointEntry.CheckpointEntryType.CONTINUATION) return empty;

                version = Long.decode(cpEntries.getDict().get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
                res.put(logData.getCheckpointedStreamId(), cpEntries.getSmrEntries(true).getUpdates());
                break;
            default:
                throw new IllegalStateException("Unknown type " + entry.getType());
        }
        return new OpaqueEntry(version, res);
    }
}
