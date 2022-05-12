package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.connector.pulsar.CircularDepTest;
import org.apache.flink.connector.pulsar.OneOfTest;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigBuilder;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.table.PulsarTableFactory;
import org.apache.flink.connector.pulsar.table.PulsarTableOptions;
import org.apache.flink.connector.pulsar.table.PulsarTableTestBase;
import org.apache.flink.connector.pulsar.table.testutils.TestingUser;
import org.apache.flink.formats.raw.RawFormatFactory;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.shade.org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalog.DEFAULT_DB;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactory.CATALOG_CONFIG_VALIDATOR;
import static org.apache.flink.connector.pulsar.table.testutils.PulsarTableTestUtils.collectRows;
import static org.apache.flink.connector.pulsar.table.testutils.SchemaData.INTEGER_LIST;
import static org.apache.flink.connector.pulsar.table.testutils.TestingUser.createRandomUser;
import static org.apache.flink.connector.pulsar.table.testutils.TestingUser.createUser;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

/** Unit test for {@link PulsarCatalog}. */
@ExtendWith(MiniClusterExtension.class)
public class PulsarCatalogITCase extends PulsarTableTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarCatalogITCase.class);

    private static final String INMEMORY_CATALOG = "inmemorycatalog";
    private static final String PULSAR_CATALOG1 = "pulsarcatalog1";
    private static final String PULSAR_CATALOG2 = "pulsarcatalog2";

    private static final String INMEMORY_DB = "mydatabase";
    private static final String PULSAR1_DB = "public/default";
    private static final String PULSAR2_DB = "tn/ns";

    private static final String FLINK_TENANT = "__flink_catalog";

    @BeforeAll
    void before() {
        registerCatalogs(tableEnv);
    }

    @Nested
    @DisplayName("Catalog operations Test")
    class CatalogOperationTest {
        // catalog operations
        @Test
        void createCatalogWithAllConfig() throws PulsarAdminException {
            String catalogName = RandomStringUtils.randomAlphabetic(10);
            String customTenantPath = "__flink_custom_tenant";
            String defaultDatabase = "my_db";
            tableEnv.executeSql(
                    String.format(
                            "CREATE CATALOG %s WITH ("
                                    + "'type' = 'pulsar-catalog',\n"
                                    + "'catalog-admin-url' = '%s',\n"
                                    + "'catalog-service-url' = '%s',\n"
                                    + "'catalog-tenant' = '%s',\n"
                                    + "'default-database' = '%s'\n"
                                    + ")",
                            catalogName,
                            pulsar.operator().adminUrl(),
                            pulsar.operator().serviceUrl(),
                            customTenantPath,
                            defaultDatabase));
            Optional<Catalog> catalogOptional = tableEnv.getCatalog(catalogName);
            assertThat(catalogOptional).isPresent();
            tableEnv.useCatalog(catalogName);
            assertThat(tableEnv.getCurrentDatabase()).isEqualTo(defaultDatabase);
            assertThat(pulsar.operator().admin().tenants().getTenants()).contains(customTenantPath);
        }

        @Test
        void createMultipleCatalog() {
            tableEnv.useCatalog(INMEMORY_CATALOG);
            assertThat(tableEnv.getCurrentCatalog()).isEqualTo(INMEMORY_CATALOG);
            assertThat(tableEnv.getCurrentDatabase()).isEqualTo(INMEMORY_DB);

            Catalog catalog = tableEnv.getCatalog(PULSAR_CATALOG1).orElse(null);
            assertThat(catalog).isNotNull();
            assertThat(catalog).isInstanceOf(PulsarCatalog.class);

            tableEnv.useCatalog(PULSAR_CATALOG1);
            assertThat(tableEnv.getCurrentDatabase()).isEqualTo(PULSAR1_DB);

            catalog = tableEnv.getCatalog(PULSAR_CATALOG2).orElse(null);
            assertThat(catalog).isNotNull();
            assertThat(catalog).isInstanceOf(PulsarCatalog.class);

            tableEnv.useCatalog(PULSAR_CATALOG2);
            assertThat(tableEnv.getCurrentDatabase()).isEqualTo(PULSAR2_DB);
        }

        // database operations
        @Test
        void listPulsarNativeDatabase() throws Exception {
            List<String> namespaces = Arrays.asList("tn1/ns1", "tn1/ns2");
            List<String> topics = Arrays.asList("tp1", "tp2");
            List<String> topicsFullName =
                    topics.stream().map(a -> "tn1/ns1/" + a).collect(Collectors.toList());
            List<String> partitionedTopics = Arrays.asList("ptp1", "ptp2");
            List<String> partitionedTopicsFullName =
                    partitionedTopics.stream()
                            .map(a -> "tn1/ns1/" + a)
                            .collect(Collectors.toList());

            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);
            assertThat(tableEnv.getCurrentDatabase()).isEqualTo(PULSAR1_DB);
            pulsar.operator()
                    .admin()
                    .tenants()
                    .createTenant(
                            "tn1",
                            TenantInfo.builder()
                                    .adminRoles(Sets.newHashSet())
                                    .allowedClusters(Sets.newHashSet("standalone"))
                                    .build());

            for (String ns : namespaces) {
                pulsar.operator().admin().namespaces().createNamespace(ns);
            }

            for (String tp : topicsFullName) {
                pulsar.operator().admin().topics().createNonPartitionedTopic(tp);
            }

            for (String tp : partitionedTopicsFullName) {
                pulsar.operator().admin().topics().createPartitionedTopic(tp, 5);
            }

            assertThat(Sets.newHashSet(tableEnv.listDatabases())).containsAll(namespaces);
            tableEnv.useDatabase("tn1/ns1");

            Set<String> tableSet = Sets.newHashSet(tableEnv.listTables());

            assertThat(tableSet)
                    .containsExactlyInAnyOrderElementsOf(
                            Iterables.concat(topics, partitionedTopics));
        }

        @Test
        void createExplicitDatabase() {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            String explictDatabaseName = newDatabaseName();
            assertThatNoException()
                    .isThrownBy(
                            () ->
                                    tableEnv.executeSql(
                                            String.format(
                                                    "CREATE DATABASE %s", explictDatabaseName)));
        }

        @Test
        void createCatalogAndExpectDefaultDatabase()
                throws ExecutionException, InterruptedException, TimeoutException {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            assertThatNoException()
                    .isThrownBy(() -> tableEnv.executeSql(String.format("USE %s", DEFAULT_DB)));

            assertThatNoException()
                    .isThrownBy(
                            () -> tableEnv.executeSql(String.format("SHOW TABLES", DEFAULT_DB)));

            String tableName = newTopicName();
            String tableDDL =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ")",
                            tableName);
            tableEnv.executeSql(tableDDL).await(10, TimeUnit.SECONDS);
            assertThat(tableEnv.listTables()).contains(tableName);
        }

        @Test
        void createNativeDatabaseShouldFail() {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            String nativeDatabaseName = "tn1/ns1";
            assertThatExceptionOfType(TableException.class)
                    .isThrownBy(
                            () ->
                                    tableEnv.executeSql(
                                            String.format(
                                                    "CREATE DATABASE `%s`", nativeDatabaseName)))
                    .withMessageStartingWith("Could not execute CREATE DATABASE");
        }

        @Test
        void dropNativeDatabaseShouldFail() {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            assertThatExceptionOfType(TableException.class)
                    .isThrownBy(
                            () ->
                                    tableEnv.executeSql(
                                            String.format("DROP DATABASE `%s`", PULSAR1_DB)))
                    .withMessageStartingWith("Could not execute DROP DATABASE");
        }

        @Test
        void dropExplicitDatabaseWithTablesShouldFail() {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            String explictDatabaseName = newDatabaseName();
            String topicName = newTopicName();

            String dbDDL = "CREATE DATABASE " + explictDatabaseName;
            tableEnv.executeSql(dbDDL).print();
            tableEnv.useDatabase(explictDatabaseName);

            String createTableSql =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ")",
                            topicName);
            tableEnv.executeSql(createTableSql);

            assertThatExceptionOfType(ValidationException.class)
                    .isThrownBy(
                            () ->
                                    tableEnv.executeSql(
                                            String.format("DROP DATABASE %s", explictDatabaseName)))
                    .withMessageStartingWith("Could not execute DROP DATABASE")
                    .withCauseInstanceOf(DatabaseNotEmptyException.class);
        }

        @Test
        void dropExplicitDatabase() {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            String explictDatabaseName = newDatabaseName();
            tableEnv.executeSql(String.format("CREATE DATABASE %s", explictDatabaseName));

            assertThatNoException()
                    .isThrownBy(
                            () ->
                                    tableEnv.executeSql(
                                            String.format(
                                                    "DROP DATABASE %s", explictDatabaseName)));
        }

        @Test
        void createAndGetDetailedDatabase() throws DatabaseNotExistException {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            String explictDatabaseName = newDatabaseName();
            tableEnv.executeSql(
                    String.format(
                            "CREATE DATABASE %s \n"
                                    + "COMMENT 'this is a comment'\n"
                                    + "WITH ("
                                    + "'p1' = 'k1',\n"
                                    + "'p2' = 'k2' \n"
                                    + ")",
                            explictDatabaseName));
            tableEnv.useDatabase(explictDatabaseName);
            Catalog catalog = tableEnv.getCatalog(PULSAR_CATALOG1).get();
            CatalogDatabase database = catalog.getDatabase(explictDatabaseName);

            Map<String, String> expectedProperties = new HashMap<>();
            expectedProperties.put("p1", "k1");
            expectedProperties.put("p2", "k2");
            assertThat(database.getProperties()).containsAllEntriesOf(expectedProperties);
            assertThat(database.getComment()).isEqualTo("this is a comment");
        }

        // table operations
        @Test
        void createExplicitTable() throws Exception {
            String databaseName = newDatabaseName();
            String tableTopic = newTopicName();
            String tableName = TopicName.get(tableTopic).getLocalName();

            tableEnv.useCatalog(PULSAR_CATALOG1);

            String dbDDL = "CREATE DATABASE " + databaseName;
            tableEnv.executeSql(dbDDL).print();
            tableEnv.useDatabase(databaseName);

            String tableDDL =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ")",
                            tableName);
            tableEnv.executeSql(tableDDL).await(10, TimeUnit.SECONDS);
            assertThat(tableEnv.listTables()).contains(tableName);
        }

        @Test
        void createExplicitTableAndRunSourceSink() {
            tableEnv.useCatalog(PULSAR_CATALOG1);

            String databaseName = newDatabaseName();
            String dbDDL = "CREATE DATABASE " + databaseName;
            tableEnv.executeSql(dbDDL).print();
            tableEnv.useDatabase(databaseName);

            String topicName = newTopicName();
            String createSql =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ") with (\n"
                                    + "   'connector' = 'pulsar',\n"
                                    + "   'topics' = '%s',"
                                    + "   'format' = 'avro'\n"
                                    + ")",
                            topicName, topicName);
            assertThatNoException().isThrownBy(() -> tableEnv.executeSql(createSql));
        }

        @Test
        void createExplicitTableInNativePulsarDatabaseShouldFail() {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);
            String topicName = newTopicName();
            String createSql =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ") with (\n"
                                    + "   'connector' = 'pulsar',\n"
                                    + "   'topics' = '%s',"
                                    + "   'format' = 'avro'\n"
                                    + ")",
                            topicName, topicName);

            assertThatExceptionOfType(TableException.class)
                    .isThrownBy(() -> tableEnv.executeSql(createSql))
                    .withMessage(
                            String.format(
                                    "Could not execute CreateTable in path `%s`.`%s`.`%s`",
                                    PULSAR_CATALOG1, PULSAR1_DB, topicName));
        }

        @Test
        void dropNativeTableShouldFail() {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);
            String topicName = newTopicName();
            pulsar.operator().createTopic(topicName, 1);
            assertThatExceptionOfType(TableException.class)
                    .isThrownBy(
                            () -> tableEnv.executeSql(String.format("DROP TABLE %s", topicName)))
                    .withMessage(
                            String.format(
                                    "Could not execute DropTable in path `%s`.`%s`.`%s`",
                                    PULSAR_CATALOG1, PULSAR1_DB, topicName));
        }

        @Test
        void dropExplicitTable() {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            String explictDatabaseName = newDatabaseName();
            tableEnv.executeSql(String.format("CREATE DATABASE %s", explictDatabaseName));
            tableEnv.useDatabase(explictDatabaseName);

            String topicName = newTopicName();
            String createTableSql =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ")",
                            topicName);
            tableEnv.executeSql(createTableSql);

            assertThatNoException()
                    .isThrownBy(
                            () -> tableEnv.executeSql(String.format("DROP TABLE %s", topicName)));
        }

        @Test
        void tableExists() throws ExecutionException, InterruptedException {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            String explictDatabaseName = newDatabaseName();
            tableEnv.executeSql(String.format("CREATE DATABASE %s", explictDatabaseName));
            tableEnv.useDatabase(explictDatabaseName);

            String topicName = newTopicName();
            String createTableSql =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ")",
                            topicName);
            tableEnv.executeSql(createTableSql).await();

            Catalog catalog = tableEnv.getCatalog(PULSAR_CATALOG1).get();
            assertThat(catalog.tableExists(new ObjectPath(explictDatabaseName, topicName)))
                    .isTrue();
        }

        @Test
        void getExplicitTable() throws TableNotExistException {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            String explictDatabaseName = newDatabaseName();
            tableEnv.executeSql(String.format("CREATE DATABASE %s", explictDatabaseName));
            tableEnv.useDatabase(explictDatabaseName);

            String topicName = newTopicName();
            String createTableSql =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ")",
                            topicName);
            tableEnv.executeSql(createTableSql);

            Catalog catalog = tableEnv.getCatalog(PULSAR_CATALOG1).get();
            CatalogBaseTable table =
                    catalog.getTable(new ObjectPath(explictDatabaseName, topicName));

            Map<String, String> expectedOptions = new HashMap<>();
            expectedOptions.put(PulsarTableOptions.EXPLICIT.key(), "true");
            expectedOptions.put(PulsarTableOptions.ADMIN_URL.key(), pulsar.operator().adminUrl());
            expectedOptions.put(
                    PulsarTableOptions.SERVICE_URL.key(), pulsar.operator().serviceUrl());
            expectedOptions.put(FactoryUtil.FORMAT.key(), RawFormatFactory.IDENTIFIER);
            expectedOptions.put(FactoryUtil.CONNECTOR.key(), PulsarTableFactory.IDENTIFIER);

            assertThat(table.getOptions()).containsExactlyEntriesOf(expectedOptions);
        }

        @Test
        void getNativeTable() throws Exception {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);

            String nativeTopicName = newTopicName();
            pulsar.operator().createTopic(nativeTopicName, 1);

            Catalog catalog = tableEnv.getCatalog(PULSAR_CATALOG1).get();
            CatalogBaseTable table = catalog.getTable(new ObjectPath(PULSAR1_DB, nativeTopicName));

            Map<String, String> expectedOptions = new HashMap<>();
            expectedOptions.put(
                    PulsarTableOptions.TOPICS.key(), TopicNameUtils.topicName(nativeTopicName));
            expectedOptions.put(PulsarTableOptions.ADMIN_URL.key(), pulsar.operator().adminUrl());
            expectedOptions.put(
                    PulsarTableOptions.SERVICE_URL.key(), pulsar.operator().serviceUrl());
            expectedOptions.put(FactoryUtil.FORMAT.key(), RawFormatFactory.IDENTIFIER);
            expectedOptions.put(FactoryUtil.CONNECTOR.key(), PulsarTableFactory.IDENTIFIER);

            assertThat(table.getOptions()).containsExactlyEntriesOf(expectedOptions);
        }
    }

    // runtime behaviour
    @Nested
    @DisplayName("Catalog operations Test")
    class ReadWriteTests {
        @Test
        void readFromNativeTable() throws Exception {
            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);
            String topicName = newTopicName();

            pulsar.operator().createTopic(topicName, 1);
            pulsar.operator().sendMessages(topicName, Schema.INT32, INTEGER_LIST);

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(String.format("SELECT * FROM %s", topicName)),
                            INTEGER_LIST.size());
            assertThat(result)
                    .containsExactlyElementsOf(
                            INTEGER_LIST.stream().map(Row::of).collect(Collectors.toList()));
        }

        @Test
        void readFromNativeTableWithMetadata() {
            // TODO this test will be implemented after useMetadata is supported;
        }

        @Test
        void readFromNativeTableFromEarliest() throws Exception {
            String topicName = newTopicName();
            pulsar.operator().sendMessages(topicName, Schema.INT32, INTEGER_LIST);

            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.getConfig()
                    .getConfiguration()
                    .setString("table.dynamic-table-options.enabled", "true");
            tableEnv.useDatabase(PULSAR1_DB);

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(
                                    "select `value` from "
                                            + TopicName.get(topicName).getLocalName()
                                            + " /*+ OPTIONS('source.start.message-id'='earliest') */"),
                            INTEGER_LIST.size());
            assertThat(result)
                    .containsExactlyElementsOf(
                            INTEGER_LIST.stream().map(Row::of).collect(Collectors.toList()));
        }

        // TODO we didn't create the topic, how can we send to it ?
        @ParameterizedTest
        @MethodSource("provideAvroBasedSchemaData")
        void readFromNativeTableWithAvroBasedSchema(String format, Schema<TestingUser> schema)
                throws Exception {
            String topicName = newTopicName();
            TestingUser expectedUser = createRandomUser();
            pulsar.operator().sendMessage(topicName, schema, expectedUser);

            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(
                                    String.format(
                                            "select * from %s ",
                                            TopicName.get(topicName).getLocalName())),
                            1);
            assertThat(result)
                    .containsExactlyElementsOf(
                            Collections.singletonList(
                                    Row.of(expectedUser.getAge(), expectedUser.getName())));
        }

        @ParameterizedTest
        @MethodSource("provideAvroBasedSchemaData")
        void readFromExplicitTableWithAvroBasedSchema(String format, Schema<TestingUser> schema)
                throws Exception {
            // TODO add this test
            String databaseName = newDatabaseName();
            String topicName = newTopicName();
            TestingUser expectedUser = createRandomUser();
            pulsar.operator().sendMessage(topicName, schema, expectedUser);

            tableEnv.useCatalog(PULSAR_CATALOG1);

            String dbDDL = "CREATE DATABASE " + databaseName;
            tableEnv.executeSql(dbDDL).print();
            tableEnv.useDatabase(databaseName);

            String sourceTableDDL =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  age INT,\n"
                                    + "  name STRING\n"
                                    + ") with (\n"
                                    + "   'connector' = 'pulsar',\n"
                                    + "   'topics' = '%s',\n"
                                    + "   'format' = '%s'\n"
                                    + ")",
                            topicName, topicName, format);
            tableEnv.executeSql(sourceTableDDL).await();

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(
                                    String.format(
                                            "select * from %s ",
                                            TopicName.get(topicName).getLocalName())),
                            1);
            assertThat(result)
                    .containsExactlyElementsOf(
                            Collections.singletonList(
                                    Row.of(expectedUser.getAge(), expectedUser.getName())));
        }

        @Test
        void readFromNativeTableWithStringSchemaUsingRawFormat() throws Exception {
            String topicName = newTopicName();
            String expectedString = "expected_string";
            pulsar.operator().sendMessage(topicName, Schema.STRING, expectedString);

            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(
                                    String.format(
                                            "select * from %s",
                                            TopicName.get(topicName).getLocalName())),
                            1);
            assertThat(result)
                    .containsExactlyElementsOf(Collections.singletonList(Row.of(expectedString)));
        }

        @Test
        void readFromNativeTableWithComplexSchemaUsingRawFormatShouldFail() {
            String topicName = newTopicName();
            TestingUser expectedUser = createRandomUser();
            pulsar.operator().sendMessage(topicName, Schema.AVRO(TestingUser.class), expectedUser);

            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);

            assertThatExceptionOfType(ValidationException.class)
                    .isThrownBy(
                            () ->
                                    collectRows(
                                            tableEnv.sqlQuery(
                                                    String.format(
                                                            "select * from %s /*+ OPTIONS('format'='raw') */",
                                                            TopicName.get(topicName)
                                                                    .getLocalName())),
                                            1))
                    .withMessageStartingWith(
                            "The 'raw' format only supports single physical column.");
        }

        @Test
        void copyDataFromNativeTableToNativeTable() throws Exception {
            String sourceTopic = newTopicName();
            String sourceTableName = TopicName.get(sourceTopic).getLocalName();
            pulsar.operator().sendMessages(sourceTopic, Schema.INT32, INTEGER_LIST);

            String sinkTopic = newTopicName();
            String sinkTableName = TopicName.get(sinkTopic).getLocalName();
            pulsar.operator().createTopic(sinkTopic, 1);
            pulsar.operator()
                    .admin()
                    .schemas()
                    .createSchema(sinkTopic, Schema.INT32.getSchemaInfo());

            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);
            String insertQ =
                    String.format(
                            "INSERT INTO %s SELECT * FROM %s", sinkTableName, sourceTableName);

            tableEnv.executeSql(insertQ);
            List<Integer> result =
                    pulsar.operator().receiveMessages(sinkTopic, Schema.INT32, INTEGER_LIST.size())
                            .stream()
                            .map(Message::getValue)
                            .collect(Collectors.toList());
            assertThat(result).containsExactlyElementsOf(INTEGER_LIST);
        }

        @ParameterizedTest
        @MethodSource("provideAvroBasedSchemaData")
        void writeToExplicitTableAndReadWithAvroBasedSchema(
                String format, Schema<TestingUser> schema) throws Exception {
            String databaseName = newDatabaseName();
            String tableSinkTopic = newTopicName();
            String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();

            pulsar.operator().createTopic(tableSinkTopic, 1);
            tableEnv.useCatalog(PULSAR_CATALOG1);

            String dbDDL = "CREATE DATABASE " + databaseName;
            tableEnv.executeSql(dbDDL).print();
            tableEnv.useDatabase(databaseName);

            String sinkDDL =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  oid STRING,\n"
                                    + "  totalprice INT,\n"
                                    + "  customerid STRING\n"
                                    + ") with (\n"
                                    + "   'connector' = 'pulsar',\n"
                                    + "   'topics' = '%s',\n"
                                    + "   'format' = '%s'\n"
                                    + ")",
                            tableSinkName, tableSinkTopic, format);
            tableEnv.executeSql(sinkDDL).await();

            String insertQ =
                    String.format(
                            "INSERT INTO %s"
                                    + " VALUES\n"
                                    + "  ('oid1', 10, 'cid1'),\n"
                                    + "  ('oid2', 20, 'cid2'),\n"
                                    + "  ('oid3', 30, 'cid3'),\n"
                                    + "  ('oid4', 10, 'cid4')",
                            tableSinkName);
            tableEnv.executeSql(insertQ).await();

            final List<Row> result =
                    collectRows(tableEnv.sqlQuery("select * from " + tableSinkName), 4);
            assertThat(result).hasSize(4);
        }

        @ParameterizedTest
        @MethodSource("provideAvroBasedSchemaData")
        @Disabled("flink-128")
        void writeToExplicitTableAndReadWithAvroBasedSchemaUsingPulsarConsumer(
                String format, Schema<TestingUser> schema) throws Exception {
            String databaseName = newDatabaseName();
            String tableSinkTopic = newTopicName();
            String tableSinkName = TopicName.get(tableSinkTopic).getLocalName();

            pulsar.operator().createTopic(tableSinkTopic, 1);
            tableEnv.useCatalog(PULSAR_CATALOG1);

            String dbDDL = "CREATE DATABASE " + databaseName;
            tableEnv.executeSql(dbDDL).print();
            tableEnv.executeSql("USE " + databaseName + "");

            String sinkDDL =
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  name STRING,\n"
                                    + "  age INT\n"
                                    + ") with (\n"
                                    + "   'connector' = 'pulsar',\n"
                                    + "   'topics' = '%s',\n"
                                    + "   'format' = '%s'\n"
                                    + ")",
                            tableSinkName, tableSinkTopic, format);
            tableEnv.executeSql(sinkDDL).await();

            String insertQ =
                    String.format(
                            "INSERT INTO %s"
                                    + " VALUES\n"
                                    + "  ('oid1', 10),\n"
                                    + "  ('oid2', 20),\n"
                                    + "  ('oid3', 30),\n"
                                    + "  ('oid4', 10)",
                            tableSinkName);
            tableEnv.executeSql(insertQ).await();

            List<TestingUser> sinkResult =
                    pulsar.operator().receiveMessages(tableSinkTopic, schema, 4).stream()
                            .map(Message::getValue)
                            .collect(Collectors.toList());
            assertThat(sinkResult)
                    .containsExactly(
                            createUser("oid1", 10),
                            createUser("oid2", 20),
                            createUser("oid3", 30),
                            createUser("oid4", 40));
        }

        @ParameterizedTest
        @MethodSource("provideAvroBasedSchemaData")
        void writeToNativeTableAndReadWithAvroBasedSchema(String format, Schema<TestingUser> schema)
                throws Exception {
            String tableSinkTopic = newTopicName();

            pulsar.operator().createTopic(tableSinkTopic, 1);
            pulsar.operator()
                    .admin()
                    .schemas()
                    .createSchema(tableSinkTopic, schema.getSchemaInfo());
            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);

            String insertQ =
                    String.format(
                            "INSERT INTO %s"
                                    + " VALUES\n"
                                    + "  (1, 'abc'),\n"
                                    + "  (2, 'bcd'),\n"
                                    + "  (3, 'cde'),\n"
                                    + "  (4, 'def')",
                            tableSinkTopic);
            tableEnv.executeSql(insertQ).await();

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(String.format("SELECT * FROM %s", tableSinkTopic)),
                            4);
            assertThat(result).hasSize(4);
        }

        @Test
        void writeToNativeTableAndReadWithStringSchema() throws Exception {
            String tableSinkTopic = newTopicName();

            pulsar.operator().createTopic(tableSinkTopic, 1);
            pulsar.operator()
                    .admin()
                    .schemas()
                    .createSchema(tableSinkTopic, Schema.STRING.getSchemaInfo());
            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);

            String insertQ =
                    String.format(
                            "INSERT INTO %s"
                                    + " VALUES\n"
                                    + "  ('abc'),\n"
                                    + "  ('bcd'),\n"
                                    + "  ('cde'),\n"
                                    + "  ('def')",
                            tableSinkTopic);
            tableEnv.executeSql(insertQ).await();

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(String.format("SELECT * FROM %s", tableSinkTopic)),
                            4);
            assertThat(result).hasSize(4);
        }

        @Test
        void writeToNativeTableAndReadWithIntegerSchema() throws Exception {
            String tableSinkTopic = newTopicName();

            pulsar.operator().createTopic(tableSinkTopic, 1);
            pulsar.operator()
                    .admin()
                    .schemas()
                    .createSchema(tableSinkTopic, Schema.INT32.getSchemaInfo());
            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);

            String insertQ =
                    String.format(
                            "INSERT INTO %s"
                                    + " VALUES\n"
                                    + "  (1),\n"
                                    + "  (2),\n"
                                    + "  (3),\n"
                                    + "  (4)",
                            tableSinkTopic);
            tableEnv.executeSql(insertQ).await();

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(String.format("SELECT * FROM %s", tableSinkTopic)),
                            4);
            assertThat(result).hasSize(4);
        }

        @Test
        void writeToNativeTableAndReadWithIntegerSchemaUsingValueField() throws Exception {
            String tableSinkTopic = newTopicName();

            pulsar.operator().createTopic(tableSinkTopic, 1);
            pulsar.operator()
                    .admin()
                    .schemas()
                    .createSchema(tableSinkTopic, Schema.INT32.getSchemaInfo());
            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.useDatabase(PULSAR1_DB);

            String insertQ =
                    String.format(
                            "INSERT INTO %s"
                                    + " VALUES\n"
                                    + "  (1),\n"
                                    + "  (2),\n"
                                    + "  (3),\n"
                                    + "  (4)",
                            tableSinkTopic);
            tableEnv.executeSql(insertQ).await();

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(
                                    String.format("SELECT `value` FROM %s", tableSinkTopic)),
                            4);
            assertThat(result).hasSize(4);
        }
    }

    @Nested
    @DisplayName("Protobuf native IT case")
    class ProtobufNativeTests {
        @Test
        void readFromNativeTableWithProtobufNativeSchema() throws Exception {
            String topicName = newTopicName();
            OneOfTest.Foo value =
                    OneOfTest.Foo.newBuilder()
                            .setBar1(OneOfTest.Bar1.newBuilder().setField1("bar1").build())
                            .setBar2(OneOfTest.Bar2.newBuilder().setField2("bar2").build())
                            .setMessageId("id")
                            .build();
            pulsar.operator()
                    .sendMessage(topicName, Schema.PROTOBUF_NATIVE(OneOfTest.Foo.class), value);

            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.getConfig()
                    .getConfiguration()
                    .setString("table.dynamic-table-options.enabled", "true");
            tableEnv.useDatabase(PULSAR1_DB);

            final List<Row> result =
                    collectRows(
                            tableEnv.sqlQuery(
                                    "select * from "
                                            + TopicName.get(topicName).getLocalName()
                                            + " /*+ OPTIONS('source.start.message-id'='earliest') */"),
                            1);

            Row expected = Row.of("id", Row.of(""), Row.of("bar2"));
            assertThat(result).containsExactly(expected);
        }

        @Test
        void readFromNativeTableWithProtobufNativeSchemaCircularNested() throws Exception {
            String topicName = newTopicName();
            CircularDepTest.Circle value = CircularDepTest.Circle.getDefaultInstance();
            pulsar.operator()
                    .sendMessage(
                            topicName, Schema.PROTOBUF_NATIVE(CircularDepTest.Circle.class), value);

            tableEnv.useCatalog(PULSAR_CATALOG1);
            tableEnv.getConfig()
                    .getConfiguration()
                    .setString("table.dynamic-table-options.enabled", "true");
            tableEnv.useDatabase(PULSAR1_DB);

            assertThatNoException()
                    .isThrownBy(
                            () ->
                                    tableEnv.executeSql(
                                            "select * from "
                                                    + TopicName.get(topicName).getLocalName()
                                                    + " /*+ OPTIONS('source.start.message-id'='earliest') */"));
        }
    }

    // utils
    private void registerCatalogs(TableEnvironment tableEnvironment) {
        tableEnvironment.registerCatalog(
                INMEMORY_CATALOG, new GenericInMemoryCatalog(INMEMORY_CATALOG, INMEMORY_DB));

        PulsarConfigBuilder configBuilder = new PulsarConfigBuilder();
        configBuilder.set(PulsarOptions.PULSAR_ADMIN_URL, pulsar.operator().adminUrl());
        configBuilder.set(PulsarOptions.PULSAR_SERVICE_URL, pulsar.operator().serviceUrl());
        tableEnvironment.registerCatalog(
                PULSAR_CATALOG1,
                new PulsarCatalog(
                        PULSAR_CATALOG1,
                        configBuilder.build(
                                CATALOG_CONFIG_VALIDATOR, PulsarCatalogConfiguration::new),
                        PULSAR1_DB,
                        FLINK_TENANT));

        tableEnvironment.registerCatalog(
                PULSAR_CATALOG2,
                new PulsarCatalog(
                        PULSAR_CATALOG2,
                        configBuilder.build(
                                CATALOG_CONFIG_VALIDATOR, PulsarCatalogConfiguration::new),
                        PULSAR2_DB,
                        FLINK_TENANT));

        tableEnvironment.useCatalog(INMEMORY_CATALOG);
    }

    private String newDatabaseName() {
        return "database" + RandomStringUtils.randomNumeric(8);
    }

    private String newTopicName() {
        return RandomStringUtils.randomAlphabetic(5);
    }
}
