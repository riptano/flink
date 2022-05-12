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

package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.connector.pulsar.table.PulsarTableFactory;
import org.apache.flink.connector.pulsar.table.catalog.impl.IncompatibleSchemaException;
import org.apache.flink.connector.pulsar.table.catalog.impl.PulsarCatalogSupport;
import org.apache.flink.connector.pulsar.table.catalog.impl.SchemaTranslator;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactoryOptions.DEFAULT_DATABASE;

/**
 * Catalog implementation to use Pulsar to store metadatas for Flink tables/databases.
 *
 * <p>A {@link PulsarCatalog} offers two modes when mapping a Pulsar topic to a Flink table.
 *
 * <p>explicit table: an explict table refers to a table created using CREATE statements. In this
 * mode, users are allowed to create a table that is bind to an existing Pulsar topic. Users can
 * specify watermarks, metatdata fields utilize the verbose configuration options to customize the
 * table connector.
 *
 * <p>native table: an native table refers to a table created by the Catalog and not by users using
 * a 1-to-1 mapping from Flink table to Pulsar topic. Each existing Pulsar topic will be mapped to a
 * table under a database using the topic's tenant and namespace named like 'tenant/namespace'. The
 * mapped table has the same name as the Pulsar topic. This mode allows users to easily query from
 * existing Pulsar topics without explicitly create the table. It automatically determines the Flink
 * format to use based on the stored Pulsar schema in the Pulsar topic. This mode has some
 * limitations, such as users can't designate an watermark and thus can't use window aggregation
 * functions.
 *
 * <p>Each topic(except Pulsar system topics) is mapped to a native table, and users can create
 * arbitrary number of explicit tables that binds to one Pulsar topic besides the native table.
 */
public class PulsarCatalog extends GenericInMemoryCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarCatalog.class);

    private final PulsarCatalogConfiguration catalogConfiguration;

    private PulsarCatalogSupport catalogSupport;

    private final String flinkTenant;

    public static final String DEFAULT_TENANT = "__flink_catalog";

    public static final String DEFAULT_DB = "default_database";

    public PulsarCatalog(
            String catalogName,
            PulsarCatalogConfiguration catalogConfiguration,
            String database,
            String flinkTenant) {
        super(catalogName, database);
        this.catalogConfiguration = catalogConfiguration;
        this.flinkTenant = flinkTenant;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new PulsarTableFactory());
    }

    @Override
    public void open() throws CatalogException {
        if (catalogSupport == null) {
            try {
                catalogSupport =
                        new PulsarCatalogSupport(
                                catalogConfiguration, flinkTenant, new SchemaTranslator(false));
            } catch (PulsarAdminException e) {
                throw new CatalogException(
                        "Failed to create Pulsar admin with configuration:"
                                + catalogConfiguration.toString(),
                        e);
            }
        }

        CatalogDatabaseImpl defaultDatabase =
                new CatalogDatabaseImpl(new HashMap<>(), "The default database for PulsarCatalog");
        try {
            createDatabase(catalogConfiguration.get(DEFAULT_DATABASE), defaultDatabase, true);
        } catch (DatabaseAlreadyExistException e) {
            throw new CatalogException(
                    "Error: should ignore default database if not exist instead of throwing exception");
        }
    }

    @Override
    public void close() throws CatalogException {
        if (catalogSupport != null) {
            catalogSupport.close();
            catalogSupport = null;
            LOG.info("Closed connection to Pulsar.");
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return catalogSupport.listDatabases();
        } catch (PulsarAdminException e) {
            throw new CatalogException(
                    String.format("Failed to list all databases in catalog: %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws CatalogException {
        try {
            return catalogSupport.getDatabase(databaseName);
        } catch (PulsarAdminException e) {
            throw new CatalogException(
                    String.format("Failed to get database info in catalog: %s", getName()), e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return catalogSupport.databaseExists(databaseName);
        } catch (PulsarAdminException e) {
            LOG.warn("Failed to check if database exists, encountered PulsarAdminError", e);
            return false;
        } catch (Exception e) {
            LOG.error("Failed to check if database exists", e);
            return false;
        }
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            catalogSupport.createDatabase(name, database);
        } catch (PulsarAdminException.ConflictException e) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), name, e);
            }
        } catch (PulsarAdminException e) {
            throw new CatalogException(String.format("Failed to create database %s", name), e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        try {
            if (!listTables(name).isEmpty() && !cascade) {
                throw new DatabaseNotEmptyException(getName(), name);
            }

            // the cascade deletion relies on the pulsar namespace deletion will clear all topics
            catalogSupport.dropDatabase(name);
        } catch (PulsarAdminException.NotFoundException e) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
        } catch (PulsarAdminException e) {
            throw new CatalogException(String.format("Failed to drop database %s", name), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        try {
            return catalogSupport.listTables(databaseName);
        } catch (PulsarAdminException.NotFoundException e) {
            throw new DatabaseNotExistException(getName(), databaseName, e);
        } catch (PulsarAdminException e) {
            throw new CatalogException(
                    String.format("Failed to list tables in database %s", databaseName), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (tablePath.getObjectName().startsWith("_tmp_table_")) {
            return super.getTable(tablePath);
        }
        try {
            return catalogSupport.getTable(tablePath);
        } catch (PulsarAdminException.NotFoundException e) {
            throw new TableNotExistException(getName(), tablePath, e);
        } catch (PulsarAdminException | IncompatibleSchemaException e) {
            throw new CatalogException(
                    String.format("Failed to get table %s schema", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        if (tablePath.getObjectName().startsWith("_tmp_table_")) {
            return super.tableExists(tablePath);
        }
        try {
            return catalogSupport.tableExists(tablePath);
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        } catch (PulsarAdminException e) {
            throw new CatalogException(
                    String.format("Failed to check table %s existence", tablePath.getFullName()),
                    e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (tablePath.getObjectName().startsWith("_tmp_table_")) {
            super.createTable(tablePath, table, ignoreIfExists);
        }

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }

        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            } else {
                return;
            }
        }

        if (table instanceof ResolvedCatalogTable) {
            try {
                catalogSupport.createTable(tablePath, (ResolvedCatalogTable) table);
            } catch (PulsarAdminException | IncompatibleSchemaException e) {
                throw new CatalogException(
                        String.format("Failed to create table %s", tablePath.getFullName()), e);
            }
        } else if (table instanceof ResolvedCatalogView) {
            throw new CatalogException(
                    String.format(
                            "Can't create view %s with catalog %s",
                            tablePath.getObjectName(), getName()));
        } else {
            throw new CatalogException(
                    String.format("Unknown Table Object kind: %s", table.getClass().getName()));
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            catalogSupport.dropTable(tablePath);
        } catch (PulsarAdminException.NotFoundException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath, e);
            } else {
                LOG.warn("The table {} does not exist. Drop table operation ignored", tablePath);
            }
        } catch (PulsarAdminException | RuntimeException e) {
            throw new CatalogException(
                    String.format("Failed to drop table %s", tablePath.getFullName()), e);
        }
    }

    // ------------------------------------------------------------------------
    // Unsupported catalog operations for Pulsar
    // There should not be such permission in the connector, it is very dangerous
    // ------------------------------------------------------------------------

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> expressions)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
