package org.corfudb.integration;


import lombok.Getter;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.StreamingManager;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema.SampleTableAMsg;
import org.corfudb.test.SampleSchema.SampleTableBMsg;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * Simple test that inserts data into CorfuStore and tests Streaming.
 */
public class StreamingIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private static int corfuStringNodePort;
    private static String singleNodeEndpoint;

    /**
     * A helper method that takes host and port specification, start a single server and
     * returns a process.
     */
    private Process runSinglePersistentServer(String host, int port) throws IOException {
        return new AbstractIT.CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();
    }

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * A StreamListener implementation to be used in the tests.
     * This listener accumulates all updates streamed to it into a linked list that
     * can then be used to verify.
     */
    private class StreamListenerImpl implements StreamListener {

        @Getter
        private final String name;

        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();

        StreamListenerImpl(String name) {
            this.name = name;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            updates.add(results);
        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public boolean equals(Object o) {
            return this.hashCode() == o.hashCode();
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * A StreamListener implementation to be used in the tests.
     * This listener blocks on a latch when executing onNext()/
     */
    private class BlockingStreamListener extends StreamListenerImpl {

        private final CountDownLatch latch;

        BlockingStreamListener(String name, CountDownLatch latch) {
            super(name);
            this.latch = latch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail();
            }
            super.updates.add(results);
        }
    }

    /**
     * Basic Streaming Test with a single table.
     * <p>
     * The test updates a single table a few times and ensures that the listeners subscribed
     * to the streaming updates from the table get the updates.
     * <p>
     * The test verifies the following:
     * 1. Streaming subscriptions with different timestamps get the right set of updates.
     * 2. The operation type, key, payload and metadata values are as expected.
     * 3. A listener does not get stream updates after it has been unsubscribed.
     */
    @Test
    public void testStreamingSingleTable() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        runtime = createRuntime(singleNodeEndpoint);
        runtime.setTransactionLogging(true);

        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = store.getTimestamp();

        // Create a table.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                "test_namespace", "tableA",
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.builder().build()
        );

        // Make some updates to the table, more than the buffer size.
        final int bufferSize = 5;
        final int numUpdates = bufferSize * 2 + 1;
        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            SampleTableAMsg msg = SampleTableAMsg.newBuilder().setPayload(String.valueOf(i)).build();
            TxnContext tx = store.txn("test_namespace");
            tx.putRecord(tableA, uuid, msg, uuid);
            tx.commit();
        }

        // Subscribe to streaming updates from the table using customized buffer size.
        StreamListenerImpl listener1 = new StreamListenerImpl("stream_listener_1");
        store.subscribe(listener1, "test_namespace", "sample_streamer_1",
                Collections.singletonList("tableA"), ts1, bufferSize);

        // After a brief wait verify that the listener gets all the updates.
        TimeUnit.SECONDS.sleep(2);

        LinkedList<CorfuStreamEntries> updates = listener1.getUpdates();
        assertThat(updates.size() == numUpdates).isTrue();

        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            SampleTableAMsg msg = SampleTableAMsg.newBuilder().setPayload(String.valueOf(i)).build();
            CorfuStreamEntries update = updates.get(i);
            assertThat(update.getEntries()).hasSize(1);
            List<CorfuStreamEntry> entry = update.getEntries().values().stream().findFirst().get();
            assertThat(entry).hasSize(1);
            assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
            assertThat(entry.get(0).getKey()).isEqualTo(uuid);
            assertThat(entry.get(0).getPayload()).isEqualTo(msg);
            assertThat(entry.get(0).getMetadata()).isEqualTo(uuid);
        }

        // Add another subscriber to the same table starting now.
        StreamListenerImpl listener2 = new StreamListenerImpl("stream_listener_2");
        store.subscribe(listener2, "test_namespace", "sample_streamer_1", Collections.singletonList("tableA"), null);

        TxnContext tx = store.txn("test_namespace");
        Uuid uuid0 = Uuid.newBuilder().setMsb(0).setLsb(0).build();
        tx.delete(tableA, uuid0).commit();

        TimeUnit.SECONDS.sleep(2);

        // Both the listener should see the deletion.
        updates = listener1.getUpdates();
        CorfuStreamEntries update = updates.getLast();
        assertThat(update.getEntries()).hasSize(1);
        List<CorfuStreamEntry> entry = update.getEntries().values().stream().findFirst().get();
        assertThat(entry.get(0).getAddress()).isGreaterThan(0L);
        assertThat(entry.get(0).getEpoch()).isEqualTo(0L);
        assertThat(entry).hasSize(1);
        assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.DELETE);
        assertThat(entry.get(0).getKey()).isEqualTo(uuid0);
        assertThat(entry.get(0).getPayload()).isNull();
        assertThat(entry.get(0).getMetadata()).isNull();

        // Ensure that the listener2 sees only one update, i.e the last delete.
        updates = listener2.getUpdates();
        assertThat(updates).hasSize(1);
        update = updates.getLast();
        assertThat(update.getEntries()).hasSize(1);
        entry = update.getEntries().values().stream().findFirst().get();
        assertThat(entry).hasSize(1);
        assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.DELETE);
        assertThat(entry.get(0).getKey()).isEqualTo(uuid0);
        assertThat(entry.get(0).getPayload()).isNull();
        assertThat(entry.get(0).getMetadata()).isNull();

        // Unsubscribe listener1 and ensure that it no longer gets any updates.
        store.unsubscribe(listener1);
        TimeUnit.SECONDS.sleep(2);

        tx = store.txn("test_namespace");
        Uuid uuid1 = Uuid.newBuilder().setMsb(1).setLsb(1).build();
        tx.clear(tableA);
        tx.commit();

        // Listener1 should see no new updates, where as listener2 should see the latest delete.
        TimeUnit.SECONDS.sleep(2);
        updates = listener1.getUpdates();
        assertThat(updates).hasSize(numUpdates + 1);
        updates = listener2.getUpdates();
        assertThat(updates).hasSize(2);
        update = updates.getLast();
        assertThat(update.getEntries()).hasSize(1);
        entry = update.getEntries().values().stream().findFirst().get();
        assertThat(entry).hasSize(1);
        assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.CLEAR);
        assertThat(entry.get(0).getKey()).isNull();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Streaming Test with two different tables with different stream tags.
     * Table A has tag1 and tag2, table B has tag2 and tag3.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testStreamingMultiTableStreams() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        runtime = createRuntime(singleNodeEndpoint);

        runtime.setTransactionLogging(true);
        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = store.getTimestamp();

        // Create 2 tables.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                "test_namespace", "tableA",
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.builder().build()
        );

        Table<Uuid, SampleTableBMsg, Uuid> tableB = store.openTable(
                "test_namespace", "tableB",
                Uuid.class, SampleTableBMsg.class, Uuid.class,
                TableOptions.builder().build()
        );

        // Make some updates to the tables.
        int index = 0;
        Uuid uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
        SampleTableAMsg msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
        TxnContext tx = store.txn("test_namespace");
        tx.putRecord(tableA, uuid, msgA, uuid);
        tx.commit();

        index++;
        uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
        SampleTableBMsg msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
        tx = store.txn("test_namespace");
        tx.putRecord(tableB, uuid, msgB, uuid);
        tx.commit();

        index++;
        uuid = Uuid.newBuilder().setMsb(index).setLsb(index).build();
        msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
        msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
        tx = store.txn("test_namespace");
        tx.putRecord(tableA, uuid, msgA, uuid);
        tx.putRecord(tableB, uuid, msgB, uuid);
        tx.commit();

        // Subscribe to streaming updates.
        StreamListenerImpl listener1 = new StreamListenerImpl("stream_listener_1");
        StreamListenerImpl listener2 = new StreamListenerImpl("stream_listener_2");
        StreamListenerImpl listener3 = new StreamListenerImpl("stream_listener_3");
        store.subscribe(listener1, "test_namespace", "sample_streamer_1", Collections.singletonList("tableA"), ts1);
        store.subscribe(listener2, "test_namespace", "sample_streamer_2", Arrays.asList("tableA", "tableB"), ts1);
        store.subscribe(listener3, "test_namespace", "sample_streamer_3", Collections.emptyList(), ts1);

        // After a brief wait verify that the listener gets all the updates.
        TimeUnit.SECONDS.sleep(2);

        LinkedList<CorfuStreamEntries> updates1 = listener1.getUpdates();
        assertThat(updates1).hasSize(2);

        LinkedList<CorfuStreamEntries> updates2 = listener2.getUpdates();
        assertThat(updates2).hasSize(3);

        LinkedList<CorfuStreamEntries> updates3 = listener3.getUpdates();
        assertThat(updates3).isEmpty();

        index = 0;
        assertThat(updates1.get(0).getEntries()).hasSize(1);
        List<CorfuStreamEntry> entries = updates1.get(0).getEntries().values().stream().findFirst().get();
        assertThat(entries).hasSize(1);
        uuid = Uuid.newBuilder().setLsb(index).setMsb(index).build();
        msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
        assertThat(entries.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
        assertThat(entries.get(0).getKey()).isEqualTo(uuid);
        assertThat(entries.get(0).getPayload()).isEqualTo(msgA);
        assertThat(entries.get(0).getMetadata()).isEqualTo(uuid);

        assertThat(updates2.get(0).getEntries()).isEqualTo(updates1.get(0).getEntries());

        index++;
        assertThat(updates2.get(1).getEntries()).hasSize(1);
        entries = updates2.get(1).getEntries().values().stream().findFirst().get();
        assertThat(entries).hasSize(1);
        uuid = Uuid.newBuilder().setLsb(index).setMsb(index).build();
        msgB = SampleTableBMsg.newBuilder().setPayload(String.valueOf(index)).build();
        assertThat(entries.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
        assertThat(entries.get(0).getKey()).isEqualTo(uuid);
        assertThat(entries.get(0).getPayload()).isEqualTo(msgB);
        assertThat(entries.get(0).getMetadata()).isEqualTo(uuid);

        index++;
        assertThat(updates1.get(1).getEntries()).hasSize(1);
        entries = updates1.get(1).getEntries().values().stream().findFirst().get();
        assertThat(entries).hasSize(1);
        uuid = Uuid.newBuilder().setLsb(index).setMsb(index).build();
        msgA = SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
        assertThat(entries.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
        assertThat(entries.get(0).getKey()).isEqualTo(uuid);
        assertThat(entries.get(0).getPayload()).isEqualTo(msgA);
        assertThat(entries.get(0).getMetadata()).isEqualTo(uuid);

        assertThat(updates2.get(2).getEntries()).hasSize(2);
        assertThat(updates2.get(2).getEntries().keySet()).containsExactlyInAnyOrder(
                new TableSchema<>("tableA", Uuid.class, SampleTableAMsg.class, Uuid.class),
                new TableSchema<>("tableB", Uuid.class, SampleTableBMsg.class, Uuid.class)
        );

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test that streaming can work well when number of subscribers exceeds number of
     * threads in the thread pools and some subscribers are slow.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testMoreSubscribersAndSlowSubscribers() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        runtime = createRuntime(singleNodeEndpoint);

        runtime.setTransactionLogging(true);
        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = store.getTimestamp();

        // Create a table.
        Table<Uuid, SampleTableAMsg, Uuid> tableA = store.openTable(
                "test_namespace", "tableA",
                Uuid.class, SampleTableAMsg.class, Uuid.class,
                TableOptions.builder().build()
        );

        final int numThread = StreamingManager.getNumThreadPerPool();
        final int numListener = numThread + 2;
        final int bufferSize = 3;
        final int numUpdates = bufferSize + 1;

        // Make some updates to the tables.
        for (int i = 0; i < numUpdates; i++) {
            SampleTableAMsg msg = SampleTableAMsg.newBuilder().setPayload(String.valueOf(i)).build();
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            TxnContext tx = store.txn("test_namespace");
            tx.putRecord(tableA, uuid, msg, uuid);
            tx.commit();
        }

        CountDownLatch latch = new CountDownLatch(1);
        StreamListenerImpl[] listeners = new StreamListenerImpl[numListener];

        // Some subscribers are blocked until the latch blocks them.
        // One thread in the pool will never blocked.
        for (int i = 0; i < numListener; i++) {
            if (i < numThread - 1) {
                listeners[i] = new BlockingStreamListener("listener" + i, latch);
            } else {
                listeners[i] = new StreamListenerImpl("listener" + i);
            }
            store.subscribe(listeners[i], "test_namespace", "sample_streamer_1",
                    Collections.singletonList("tableA"), ts1, bufferSize);
        }

        TimeUnit.SECONDS.sleep(6);

        for (int i = 0; i < numListener; i++) {
            if (i < numThread - 1) {
                assertThat(listeners[i].getUpdates().size()).isEqualTo(0);
            } else {
                assertThat(listeners[i].getUpdates().size()).isEqualTo(numUpdates);
            }
        }

        latch.countDown();
        TimeUnit.SECONDS.sleep(4);

        for (int i = 0; i < numListener; i++) {
            assertThat(listeners[i].getUpdates().size()).isEqualTo(numUpdates);
        }

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Backward compatibility test.
     * <p>
     * Streaming Test with 2 different tables and a single streamer
     * <p>
     * The test creates two tables in the same namespace and makes updates to both in the same transaction.
     * A single streamer subscribes to updates from both.
     * <p>
     * The test verifies that the streamer receives updates from both tables and since the updates were made in
     * the same transaction, they are received in one CorfuStreamEntry.
     */
    @Test
    public void testStreamingMultiTableSingleListener() throws Exception {
        // Run a corfu server.
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime.
        runtime = createRuntime(singleNodeEndpoint);

        runtime.setTransactionLogging(true);
        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = store.getTimestamp();

        // Create 2 tables in the same namespace.
        Table<Uuid, Uuid, Uuid> n1t1 = store.openTable(
                "n1", "t1", Uuid.class,
                Uuid.class, Uuid.class,
                TableOptions.builder().build()
        );

        Table<Uuid, Uuid, Uuid> n1t2 = store.openTable(
                "n1", "t2", Uuid.class,
                Uuid.class, Uuid.class,
                TableOptions.builder().build()
        );

        // Make an update to the tables in a transaction.
        final int t1_uuid = 5;
        final int t2_uuid = 10;
        Uuid t1Uuid = Uuid.newBuilder().setMsb(t1_uuid).setLsb(t1_uuid).build();
        Uuid t2Uuid = Uuid.newBuilder().setMsb(t2_uuid).setLsb(t2_uuid).build();
        TxnContext txnContext = store.txn("n1");
        txnContext.putRecord(n1t1, t1Uuid, t1Uuid, t1Uuid);
        txnContext.putRecord(n1t2, t2Uuid, t2Uuid, t2Uuid);
        txnContext.commit();

        // Subscribe to both tables.
        List<TableSchema<Uuid, Uuid, Uuid>> tablesSubscribed = new ArrayList<>();
        TableSchema<Uuid, Uuid, Uuid> schema1 = new TableSchema<>("t1", Uuid.class, Uuid.class, Uuid.class);
        TableSchema<Uuid, Uuid, Uuid> schema2 = new TableSchema<>("t2", Uuid.class, Uuid.class, Uuid.class);
        tablesSubscribed.add(schema1);
        tablesSubscribed.add(schema2);
        StreamListenerImpl listener = new StreamListenerImpl("n1_listener");
        store.subscribe(listener, "n1", tablesSubscribed, ts1);

        // Verify that both updates come to the subscriber in the same StreamEntry.
        TimeUnit.SECONDS.sleep(2);
        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates.size()).isEqualTo(1);
        assertThat(updates.getFirst().getEntries().entrySet().size()).isEqualTo(2);

        // Check the entries and operations in each.
        assertThat(updates.getFirst().getEntries().get(schema1).get(0).getKey()).isEqualTo(t1Uuid);
        assertThat(updates.getFirst().getEntries().get(schema1).get(0).getPayload()).isEqualTo(t1Uuid);
        assertThat(updates.getFirst().getEntries().get(schema1).get(0).getMetadata()).isEqualTo(t1Uuid);
        assertThat(updates.getFirst().getEntries().get(schema1).get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);

        assertThat(updates.getFirst().getEntries().get(schema2).get(0).getKey()).isEqualTo(t2Uuid);
        assertThat(updates.getFirst().getEntries().get(schema2).get(0).getPayload()).isEqualTo(t2Uuid);
        assertThat(updates.getFirst().getEntries().get(schema2).get(0).getMetadata()).isEqualTo(t2Uuid);
        assertThat(updates.getFirst().getEntries().get(schema2).get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }
}
