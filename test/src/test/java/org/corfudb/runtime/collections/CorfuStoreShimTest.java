package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.Messages;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.Messages.Uuid;
import org.corfudb.runtime.view.Address;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * To ensure that feature changes in CorfuStore do not break verticals,
 * we simulate their usage pattern with implementation and tests.
 * <p>
 * Created by hisundar on 2020-09-16
 */
@Slf4j
public class CorfuStoreShimTest extends AbstractViewTest {
    private CorfuRuntime getTestRuntime() {
        return getDefaultRuntime();
    }
    /**
     * CorfuStoreShim supports read your transactional writes implicitly when reads
     * happen in a write transaction or vice versa
     * This test demonstrates how that would work
     *
     * @throws Exception exception
     */
    @Test
    public void checkDirtyReads() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        ManagedTxnContext txn = shimStore.txn(someNamespace);
        txn.putRecord(tableName, key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                user_1);
        txn.commit();
        long tail1 = shimStore.getHighestSequence(someNamespace, tableName);

        // Take a snapshot to test snapshot isolation transaction
        final CorfuStoreMetadata.Timestamp timestamp = shimStore.getTimestamp();
        CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
        // Start a dirty read transaction
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            readWriteTxn.putRecord(table, key1,
                    ManagedMetadata.newBuilder().setCreateUser("xyz").build(),
                    ManagedMetadata.newBuilder().build());

            // Now within the same txn query the object and validate that it shows the local update.
            entry = readWriteTxn.getRecord(table, key1);
            assertThat(entry.getPayload().getCreateUser()).isEqualTo("xyz");
            readWriteTxn.commit();
        }

        long tail2 = shimStore.getHighestSequence(someNamespace, tableName);
        assertThat(tail2).isGreaterThan(tail1);

        // Try a read followed by write in same txn
        // Start a dirty read transaction
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            entry = readWriteTxn.getRecord(table, key1);
            readWriteTxn.putRecord(table, key1,
                    ManagedMetadata.newBuilder()
                            .setCreateUser("abc" + entry.getPayload().getCreateUser())
                            .build(),
                    ManagedMetadata.newBuilder().build());
            readWriteTxn.commit();
        }

        // Try a read on an older timestamp
        try (ManagedTxnContext readTxn = shimStore.txn(someNamespace, IsolationLevel.snapshot(timestamp))) {
            entry = readTxn.getRecord(table, key1);
            assertThat(entry.getPayload().getCreateUser()).isEqualTo("abc");
        }
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            Uuid key2 = null;
            assertThatThrownBy( () -> readWriteTxn.putRecord(tableName, key2, null, null))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }
        log.debug(table.getMetrics().toString());
    }

    /**
     * Simple example to see how secondary indexes work. Please see sample_schema.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testSecondaryIndexes() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, ExampleValue, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        final long eventTime = 123L;

        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ExampleValue.newBuilder()
                            .setPayload("abc")
                            .setAnotherKey(eventTime)
                            .build(),
                    user_1);
            txn.commit();
        }

        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "anotherKey", eventTime);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getPayload()).isEqualTo("abc");
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
    }

    /**
     * CorfuStoreShim stores 3 pieces of information - key, value and metadata
     * This test demonstrates how metadata field options esp "version" can be used and verified.
     *
     * @throws Exception exception
     */
    @Test
    public void checkRevisionValidation() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                    user_1);
            txn.commit();
        }

        // Validate that touch() does not change the revision
        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.touch(tableName, key1);
            txn.commit();
        }

        CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
        try (ManagedTxnContext queryTxn = shimStore.txn(someNamespace)) {
            entry = queryTxn.getRecord(table, key1);
        }
        assertNotNull(entry);
        assertThat(entry.getMetadata().getRevision()).isEqualTo(0L);
        assertThat(entry.getMetadata().getCreateTime()).isLessThan(System.currentTimeMillis());

        // Ensure that if metadata's revision field is set, it is validated and exception thrown if stale
        final ManagedTxnContext txn1 = shimStore.txn(someNamespace);
        txn1.putRecord(tableName, key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                ManagedMetadata.newBuilder().setRevision(1L).build());
        assertThatThrownBy(txn1::commit).isExactlyInstanceOf(StaleRevisionUpdateException.class);

        // Correct revision field set should NOT throw an exception
        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ManagedMetadata.newBuilder().setCreateUser("xyz").build(),
                    ManagedMetadata.newBuilder().setRevision(0L).build());
            txn.commit();
        }

        // Revision field not set should also not throw an exception, just internally bump up revision
        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ManagedMetadata.newBuilder().setCreateUser("xyz").build(),
                    ManagedMetadata.newBuilder().build());
            txn.commit();
        }

        try (ManagedTxnContext queryTxn = shimStore.txn(someNamespace)) {
            entry = queryTxn.getRecord(table, key1);
        }
        assertThat(entry.getMetadata().getRevision()).isEqualTo(2L);
        assertThat(entry.getMetadata().getCreateUser()).isEqualTo("user_1");
        assertThat(entry.getMetadata().getLastModifiedTime()).isLessThan(System.currentTimeMillis() + 1);
        assertThat(entry.getMetadata().getCreateTime()).isLessThan(entry.getMetadata().getLastModifiedTime());

        log.debug(table.getMetrics().toString());
    }

    /**
     * Demonstrates the checks that the metadata passed into CorfuStoreShim is validated against.
     *
     * @throws Exception exception
     */
    @Test
    public void checkNullMetadataTransactions() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, ManagedMetadata, ManagedMetadata> table =
                shimStore.openTable(
                        someNamespace,
                        tableName,
                        Uuid.class,
                        ManagedMetadata.class,
                        null,
                        // TableOptions includes option to choose - Memory/Disk based corfu table.
                        TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder().setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits()).build();
        ManagedTxnContext txn = shimStore.txn(someNamespace);
        txn.putRecord(tableName,
                key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                null);
               txn.commit();
        txn = shimStore.txn(someNamespace);
        txn.putRecord(table,
                key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                null);
        txn.commit();
        Message metadata = shimStore.getTable(someNamespace, tableName).get(key1).getMetadata();
        assertThat(metadata).isNull();

        // TODO: Finalize behavior of the following api with consumers..
        // Setting metadata into a schema that did not have metadata specified?
        txn = shimStore.txn(someNamespace);
        txn.putRecord(tableName,
                key1,
                ManagedMetadata.newBuilder().setCreateUser("bcd").build(),
                ManagedMetadata.newBuilder().setCreateUser("testUser").setRevision(1L).build());
        txn.commit();
        assertThat(shimStore.getTable(someNamespace, tableName).get(key1).getMetadata())
                .isNotNull();

        // Now setting back null into the metadata which had non-null value
        txn = shimStore.txn(someNamespace);
        txn.putRecord(tableName,
                key1,
                ManagedMetadata.newBuilder().setCreateUser("cde").build(),
                null);
        txn.commit();
        assertThat(shimStore.getTable(someNamespace, tableName).get(key1).getMetadata())
                .isNull();

        log.debug(table.getMetrics().toString());
    }

    /**
     * Validate that fields of metadata that are not set explicitly retain their prior values.
     *
     * @throws Exception exception
     */
    @Test
    public void checkMetadataMergesOldFieldsTest() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, ManagedMetadata, Messages.LogReplicationEntryMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                Messages.LogReplicationEntryMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();
        final String something = "double_nested_metadata_field";
        final int one = 1; // Frankly stupid but i could not figure out how to selectively disable checkstyle
        final long twelve = 12L; // please help figure out how to disable checkstyle selectively

        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    Messages.LogReplicationEntryMetadata.newBuilder()
                            .setSiteConfigID(twelve)
                            .setSyncRequestId(Uuid.newBuilder().setMsb(one).build())
                            .build());
            txn.commit();
        }

        // Update the record, validate that metadata fields not set, get merged with existing
        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    Messages.LogReplicationEntryMetadata.newBuilder()
                            .setTimestamp(one+twelve)
                            .build());
            txn.commit();
        }
        CorfuStoreEntry<Uuid, ManagedMetadata, Messages.LogReplicationEntryMetadata> entry = null;
        try (ManagedTxnContext queryTxn = shimStore.txn(someNamespace)) {
            entry = queryTxn.getRecord(table, key);
        }

        assertThat(entry.getMetadata().getSiteConfigID()).isEqualTo(twelve);
        assertThat(entry.getMetadata().getSyncRequestId().getMsb()).isEqualTo(one);
        assertThat(entry.getMetadata().getTimestamp()).isEqualTo(twelve+one);

        // Rolling Upgrade compatibility test: It should be ok to set a different metadata schema message
        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    ManagedMetadata.newBuilder()
                            .build(), true);
            txn.commit();
        }

        log.debug(table.getMetrics().toString());
    }

    /**
     * Validate that fields of metadata that are not set explicitly retain their prior values.
     *
     * @throws Exception
     */
    @Test
    public void checkMetadataWorksWithoutSupervision() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();

        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key, value); // Look no metadata specified!
            txn.commit();
        }

        CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
        try (ManagedTxnContext query = shimStore.txn(someNamespace)) {
            entry = query.getRecord(tableName, key);
        }
        assertThat(entry.getMetadata().getRevision()).isEqualTo(0);
        assertThat(entry.getMetadata().getCreateTime()).isGreaterThan(0);
        assertThat(entry.getMetadata().getCreateTime()).isEqualTo(entry.getMetadata().getLastModifiedTime());

        class CommitCallbackImpl implements TxnContext.CommitCallback {
            public void onCommit(Map<String, List<CorfuStreamEntry>> mutations) {
                assertThat(mutations.size()).isEqualTo(1);
                assertThat(mutations.get(table.getFullyQualifiedTableName()).size()).isEqualTo(1);
                // This one way to selectively extract the metadata out
                ManagedMetadata metadata = (ManagedMetadata) mutations.get(table.getFullyQualifiedTableName()).get(0).getMetadata();
                assertThat(metadata.getRevision()).isGreaterThan(0);

                // This is another way to extract the metadata out..
                mutations.forEach((tblName, entries) -> {
                    entries.forEach(mutation -> {
                        // This is how we can extract the metadata out
                        ManagedMetadata metaData = (ManagedMetadata) mutations.get(tblName).get(0).getMetadata();
                        assertThat(metaData.getRevision()).isGreaterThan(0);
                    });
                });
            }
        }

        CommitCallbackImpl commitCallback = new CommitCallbackImpl();
        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key, value); // Look no metadata specified!
            txn.addCommitCallback((mutations) -> {
                mutations.values().forEach(mutation -> {
                    CorfuStreamEntry.OperationType op = mutation.get(0).getOperation();
                    assertThat(op).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
                });
            });
            txn.commit();
        }

        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.deleteRecord(tableName, key, ManagedMetadata.newBuilder().build());
            txn.addCommitCallback((mutations) -> {
                mutations.values().forEach(mutation -> {
                    CorfuStreamEntry.OperationType op = mutation.get(0).getOperation();
                    assertThat(op).isEqualTo(CorfuStreamEntry.OperationType.DELETE);
                });
            });
            txn.commit();
        }

        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.clear(tableName);
            txn.addCommitCallback((mutations) -> {
                mutations.values().forEach(mutation -> {
                    CorfuStreamEntry.OperationType op = mutation.get(0).getOperation();
                    assertThat(op).isEqualTo(CorfuStreamEntry.OperationType.CLEAR);
                });
            });
            txn.commit();
        }
        log.debug(table.getMetrics().toString());
    }

    /**
     * Validate that nested transactions do not throw exception if txnWithNesting is used.
     *
     * @throws Exception
     */
    @Test
    public void checkNestedTransaction() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();

        class NestedTxnTester {
            public void nestedQuery() {
                CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
                try (ManagedTxnContext rwTxn = shimStore.txnWithNesting(someNamespace)) {
                    entry = rwTxn.getRecord(tableName, key);
                    // Nested transactions can also supply commitCallbacks that will be invoked when
                    // the transaction actually commits.
                    rwTxn.addCommitCallback((mutations) -> {
                        assertThat(mutations).containsKey(table.getFullyQualifiedTableName());
                        assertThat(TransactionalContext.isInTransaction()).isFalse();
                    });
                    rwTxn.commit();
                }
                assertThat(TransactionalContext.isInTransaction()).isTrue();
                assertThat(entry.getMetadata().getRevision()).isEqualTo(0);
                assertThat(entry.getMetadata().getCreateTime()).isGreaterThan(0);
                assertThat(entry.getMetadata().getCreateTime()).isEqualTo(entry.getMetadata().getLastModifiedTime());
            }
        }
        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key, value); // Look no metadata specified!
            NestedTxnTester nestedTxnTester = new NestedTxnTester();
            nestedTxnTester.nestedQuery();
            txn.commit();
        }

        assertThat(TransactionalContext.isInTransaction()).isFalse();

        // ----- check nested transactions NOT started by CorfuStore isn't messed up by CorfuStore txn -----
        CorfuTable<String, String>
                corfuTable = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuRuntime.getObjectsView()
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE).build().begin();
        corfuTable.put("k1", "a"); // Load non-CorfuStore data
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");
        CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
        try (ManagedTxnContext nestedTxn = shimStore.txnWithNesting(someNamespace)) {
            nestedTxn.putRecord(tableName, key, ManagedMetadata.newBuilder().setLastModifiedUser("secondUser").build());
            entry = nestedTxn.getRecord(tableName, key);
            nestedTxn.commit(); // should not commit the parent transaction!
        }
        assertThat(entry.getMetadata().getRevision()).isGreaterThan(0);

        assertThat(TransactionalContext.isInTransaction()).isTrue();
        long commitAddress = corfuRuntime.getObjectsView().TXEnd();
        assertThat(commitAddress).isNotEqualTo(Address.NON_ADDRESS);
    }
}
