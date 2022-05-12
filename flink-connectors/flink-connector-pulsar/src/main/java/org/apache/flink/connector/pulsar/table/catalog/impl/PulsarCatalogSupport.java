/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.table.catalog.impl;

import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.table.PulsarTableFactory;
import org.apache.flink.connector.pulsar.table.PulsarTableOptions;
import org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogConfiguration;
import org.apache.flink.connector.pulsar.table.catalog.utils.TableSchemaHelper;
import org.apache.flink.formats.raw.RawFormatFactory;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is the implementation layer of catalog operations. It uses {@link PulsarAdminTool} to
 * interact with Pulsar topics and manipulates metadata. {@link PulsarCatalogSupport} distinguish
 * between explicit and native tables.
 */
public class PulsarCatalogSupport {

    private static final String DATABASE_COMMENT_KEY = "__database_comment";
    private static final String DATABASE_DESCRIPTION_KEY = "__database_description";
    private static final String DATABASE_DETAILED_DESCRIPTION_KEY =
            "__database_detailed_description";

    private static final String TABLE_PREFIX = "table_";

    PulsarCatalogConfiguration catalogConfiguration;

    private final PulsarAdminTool pulsarAdminTool;

    private final String flinkCatalogTenant;

    private SchemaTranslator schemaTranslator;

    public PulsarCatalogSupport(
            PulsarCatalogConfiguration catalogConfiguration,
            String flinkTenant,
            SchemaTranslator schemaTranslator)
            throws PulsarAdminException {
        this.catalogConfiguration = catalogConfiguration;
        this.pulsarAdminTool = new PulsarAdminTool(catalogConfiguration);
        this.schemaTranslator = schemaTranslator;
        this.flinkCatalogTenant = flinkTenant;

        // Initialize the dedicated tenant if necessary
        if (!pulsarAdminTool.tenantExists(flinkCatalogTenant)) {
            pulsarAdminTool.createTenant(flinkCatalogTenant);
        }
    }

    /**
     * A generic database stored in pulsar catalog should consist of alphanumeric characters. A
     * pulsar tenant/namespace mapped database should contain the "/" in between tenant and
     * namespace
     *
     * @param name the database name
     * @return false if the name contains "/", which indicate it's a pulsar tenant/namespace mapped
     *     database
     */
    private boolean isExplicitDatabase(String name) {
        return !name.contains("/");
    }

    private String completeExplicitDatabasePath(String name) {
        return this.flinkCatalogTenant + "/" + name;
    }

    public List<String> listDatabases() throws PulsarAdminException {
        List<String> databases = new ArrayList<>();
        for (String ns : pulsarAdminTool.listNamespaces()) {
            if (ns.startsWith(flinkCatalogTenant)) {
                // explicit table database
                databases.add(ns.substring(flinkCatalogTenant.length() + 1));
            } else {
                // pulsar tenant/namespace mapped database
                databases.add(ns);
            }
        }
        return databases;
    }

    public boolean databaseExists(String name) throws PulsarAdminException {
        if (isExplicitDatabase(name)) {
            return pulsarAdminTool.namespaceExists(completeExplicitDatabasePath(name));
        } else {
            return pulsarAdminTool.namespaceExists(name);
        }
    }

    public void createDatabase(String name, CatalogDatabase database) throws PulsarAdminException {
        if (isExplicitDatabase(name)) {
            pulsarAdminTool.createNamespace(completeExplicitDatabasePath(name));
            Map<String, String> allProperties = database.getProperties();
            allProperties.put(DATABASE_COMMENT_KEY, database.getComment());
            allProperties.put(DATABASE_DESCRIPTION_KEY, database.getDescription().orElse(""));
            allProperties.put(
                    DATABASE_DETAILED_DESCRIPTION_KEY,
                    database.getDetailedDescription().orElse(""));
            pulsarAdminTool.updateNamespaceProperties(
                    completeExplicitDatabasePath(name), allProperties);
        } else {
            throw new CatalogException("Can't create pulsar tenant/namespace mapped database");
        }
    }

    public CatalogDatabase getDatabase(String name) throws PulsarAdminException {
        Map<String, String> allProperties =
                pulsarAdminTool.getNamespaceProperties(completeExplicitDatabasePath(name));
        String comment = allProperties.getOrDefault(DATABASE_COMMENT_KEY, "");
        allProperties.remove(DATABASE_COMMENT_KEY);
        return new CatalogDatabaseImpl(allProperties, comment);
    }

    public void dropDatabase(String name) throws PulsarAdminException {
        if (isExplicitDatabase(name)) {
            pulsarAdminTool.deleteNamespace(completeExplicitDatabasePath(name));
        } else {
            throw new CatalogException("Can't drop pulsar tenant/namespace mapped database");
        }
    }

    public List<String> listTables(String name) throws PulsarAdminException {
        if (isExplicitDatabase(name)) {
            List<String> tables = new ArrayList<>();
            List<String> topics = pulsarAdminTool.getTopics(completeExplicitDatabasePath(name));
            for (String topic : topics) {
                tables.add(topic.substring(TABLE_PREFIX.length()));
            }
            return tables;
        } else {
            return pulsarAdminTool.getTopics(name);
        }
    }

    public boolean tableExists(ObjectPath tablePath) throws PulsarAdminException {
        if (isExplicitDatabase(tablePath.getDatabaseName())) {
            return pulsarAdminTool.topicExists(findExplicitTablePlaceholderTopic(tablePath));
        } else {
            return pulsarAdminTool.topicExists(findTopicForNativeTable(tablePath));
        }
    }

    public CatalogTable getTable(ObjectPath tablePath) throws PulsarAdminException {
        if (isExplicitDatabase(tablePath.getDatabaseName())) {
            try {
                String mappedTopic = findExplicitTablePlaceholderTopic(tablePath);
                final SchemaInfo metadataSchema = pulsarAdminTool.getPulsarSchema(mappedTopic);
                Map<String, String> tableProperties =
                        TableSchemaHelper.generateTableProperties(metadataSchema);
                CatalogTable table = CatalogTable.fromProperties(tableProperties);
                table.getOptions().put(PulsarTableOptions.EXPLICIT.key(), Boolean.TRUE.toString());
                return CatalogTable.of(
                        table.getUnresolvedSchema(),
                        table.getComment(),
                        table.getPartitionKeys(),
                        fillDefaultOptionsFromCatalogOptions(table.getOptions()));
            } catch (Exception e) {
                e.printStackTrace();
                throw new CatalogException(
                        "Failed to fetch metadata for explict table: " + tablePath.getObjectName());
            }
        } else {
            String existingTopic = findTopicForNativeTable(tablePath);
            final SchemaInfo pulsarSchema = pulsarAdminTool.getPulsarSchema(existingTopic);
            return schemaToCatalogTable(pulsarSchema, existingTopic);
        }
    }

    public void dropTable(ObjectPath tablePath) throws PulsarAdminException {
        if (isExplicitDatabase(tablePath.getDatabaseName())) {
            String mappedTopic = findExplicitTablePlaceholderTopic(tablePath);
            // manually clean the schema to avoid affecting new table with same name use old schema
            pulsarAdminTool.deleteSchema(mappedTopic);
            pulsarAdminTool.deleteTopic(mappedTopic);
        } else {
            throw new CatalogException("Can't delete native topic");
        }
    }

    public void createTable(ObjectPath tablePath, ResolvedCatalogTable table)
            throws PulsarAdminException {
        // only allow creating table in explict database, the topic is used to save table
        // information
        if (!isExplicitDatabase(tablePath.getDatabaseName())) {
            throw new CatalogException(
                    String.format(
                            "Can't create explict table under pulsar tenant/namespace: %s because it's a native database",
                            tablePath.getDatabaseName()));
        }

        String mappedTopic = findExplicitTablePlaceholderTopic(tablePath);
        pulsarAdminTool.createTopic(mappedTopic, 1);

        // use pulsar schema to store explicit table information
        try {
            SchemaInfo schemaInfo = TableSchemaHelper.generateSchemaInfo(table.toProperties());
            pulsarAdminTool.uploadSchema(mappedTopic, schemaInfo);
        } catch (Exception e) {
            // delete topic if table info cannot be persisted
            try {
                pulsarAdminTool.deleteTopic(mappedTopic);
            } catch (PulsarAdminException ex) {
                // do nothing
            }
            e.printStackTrace();
            throw new CatalogException("Can't store table metadata");
        }
    }

    private CatalogTable schemaToCatalogTable(SchemaInfo pulsarSchema, String topicName) {
        final Schema schema = schemaTranslator.pulsarSchemaToFlinkSchema(pulsarSchema);

        Map<String, String> initialTableOptions = new HashMap<>();
        initialTableOptions.put(PulsarTableOptions.TOPICS.key(), topicName);
        initialTableOptions.put(
                FactoryUtil.FORMAT.key(), schemaTranslator.decideDefaultFlinkFormat(pulsarSchema));

        Map<String, String> enrichedTableOptions =
                fillDefaultOptionsFromCatalogOptions(initialTableOptions);

        return CatalogTable.of(schema, "", Collections.emptyList(), enrichedTableOptions);
    }

    // enrich table properties with proper catalog configs
    private Map<String, String> fillDefaultOptionsFromCatalogOptions(
            final Map<String, String> tableOptions) {
        Map<String, String> enrichedTableOptions = new HashMap<>();
        enrichedTableOptions.put(FactoryUtil.CONNECTOR.key(), PulsarTableFactory.IDENTIFIER);
        enrichedTableOptions.put(
                PulsarTableOptions.ADMIN_URL.key(),
                catalogConfiguration.get(PulsarOptions.PULSAR_ADMIN_URL));
        enrichedTableOptions.put(
                PulsarTableOptions.SERVICE_URL.key(),
                catalogConfiguration.get(PulsarOptions.PULSAR_SERVICE_URL));

        String authPlugin = catalogConfiguration.get(PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME);
        if (authPlugin != null && !authPlugin.isEmpty()) {
            enrichedTableOptions.put(PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME.key(), authPlugin);
        }

        String authParams = catalogConfiguration.get(PulsarOptions.PULSAR_AUTH_PARAMS);
        if (authParams != null && !authParams.isEmpty()) {
            enrichedTableOptions.put(PulsarOptions.PULSAR_AUTH_PARAMS.key(), authParams);
        }

        // we always provide RAW format as a default format
        if (!enrichedTableOptions.containsKey(FactoryUtil.FORMAT.key())) {
            enrichedTableOptions.put(FactoryUtil.FORMAT.key(), RawFormatFactory.IDENTIFIER);
        }

        if (tableOptions != null) {
            // table options could overwrite the default options provided above
            enrichedTableOptions.putAll(tableOptions);
        }
        return enrichedTableOptions;
    }

    private String findExplicitTablePlaceholderTopic(ObjectPath objectPath) {
        String database = flinkCatalogTenant + "/" + objectPath.getDatabaseName();
        String topic = TABLE_PREFIX + objectPath.getObjectName();

        NamespaceName ns = NamespaceName.get(database);
        TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, topic);
        return fullName.toString();
    }

    private String findTopicForNativeTable(ObjectPath objectPath) {
        String database = objectPath.getDatabaseName();
        String topic = objectPath.getObjectName();

        NamespaceName ns = NamespaceName.get(database);
        TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, topic);
        return fullName.toString();
    }

    public void close() {
        if (pulsarAdminTool != null) {
            pulsarAdminTool.close();
        }
    }
}
