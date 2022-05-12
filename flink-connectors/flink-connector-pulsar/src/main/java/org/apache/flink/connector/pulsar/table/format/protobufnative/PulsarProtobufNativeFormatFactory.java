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

package org.apache.flink.connector.pulsar.table.format.protobufnative;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigBuilder;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.table.PulsarTableOptions;
import org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogConfiguration;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.protobuf.Descriptors;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getFirstTopic;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getPulsarPropertiesWithPrefix;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.ADMIN_URL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPICS;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactory.CATALOG_CONFIG_VALIDATOR;

/**
 * Support {@link org.apache.pulsar.client.impl.schema.ProtobufNativeSchema} based pulsar schema
 * SchemaRegistry.
 */
public class PulsarProtobufNativeFormatFactory implements DeserializationFormatFactory {

    public static final String IDENTIFIER = "pulsar-protobuf-native";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        ReadableConfig tableConf = Configuration.fromMap(context.getCatalogTable().getOptions());

        String topic = extractTopicName(context);

        Properties adminProperties =
                getPulsarPropertiesWithPrefix(tableConf, PulsarOptions.ADMIN_CONFIG_PREFIX);
        PulsarConfigBuilder configBuilder = new PulsarConfigBuilder();
        configBuilder.set(adminProperties);
        configBuilder.set(PulsarOptions.PULSAR_ADMIN_URL, tableConf.get(ADMIN_URL));
        configBuilder.set(PulsarOptions.PULSAR_SERVICE_URL, tableConf.get(SERVICE_URL));
        PulsarCatalogConfiguration catalogConfiguration =
                configBuilder.build(CATALOG_CONFIG_VALIDATOR, PulsarCatalogConfiguration::new);
        SerializableSupplier<Descriptors.Descriptor> loadDescriptor =
                () -> {
                    SchemaInfo schemaInfo = null;
                    try {
                        PulsarAdmin admin = createAdmin(catalogConfiguration);
                        schemaInfo = admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    return ((GenericProtobufNativeSchema)
                                    GenericProtobufNativeSchema.of(schemaInfo))
                            .getProtobufNativeSchema();
                };

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                return new PulsarProtobufNativeRowDataDeserializationSchema(
                        loadDescriptor, rowType);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    private String extractTopicName(DynamicTableFactory.Context context) {
        if (isExplicitTable(context)) {
            return getFirstTopic(context.getCatalogTable().getOptions().get(TOPICS.key()));
        } else {
            String database = context.getObjectIdentifier().getDatabaseName();
            String objectName = context.getObjectIdentifier().getObjectName();
            NamespaceName ns = NamespaceName.get(database);
            TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, objectName);
            return fullName.toString();
        }
    }

    /**
     * Check if the table is an explicit table. An explicit table is defined as created using
     * "CREATE TABLE". If the table is explicit, when getting the table from catalog there will be
     * an {@link PulsarTableOptions#EXPLICIT} option set to "true"
     *
     * @param context
     * @return true if the topic is created by "CREATE TABLE" , false if the table is directly
     *     mapped from pulsar topic
     */
    private boolean isExplicitTable(DynamicTableFactory.Context context) {
        final String isExplicit =
                context.getCatalogTable().getOptions().get(PulsarTableOptions.EXPLICIT.key());
        return !StringUtils.isNullOrWhitespaceOnly(isExplicit) && Boolean.parseBoolean(isExplicit);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
