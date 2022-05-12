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

package org.apache.flink.connector.pulsar.table.catalog.utils;

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.pulsar.shade.com.google.common.base.Preconditions.checkNotNull;

/** Util to convert between flink table map representation and pulsar SchemaInfo. */
public final class TableSchemaHelper {

    private TableSchemaHelper() {}

    public static SchemaInfo generateSchemaInfo(Map<String, String> properties)
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return SchemaInfoImpl.builder()
                .name("flink_table_schema")
                .type(SchemaType.BYTES)
                .schema(mapper.writeValueAsBytes(properties))
                .build();
    }

    public static Map<String, String> generateTableProperties(SchemaInfo schemaInfo)
            throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(
                schemaInfo.getSchema(), new TypeReference<HashMap<String, String>>() {});
    }

    public static void uploadPulsarSchema(PulsarAdmin admin, String topic, SchemaInfo schemaInfo) {
        checkNotNull(schemaInfo);

        SchemaInfo existingSchema;
        try {
            existingSchema = admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
        } catch (PulsarAdminException pae) {
            if (pae.getStatusCode() == 404) {
                existingSchema = null;
            } else {
                throw new RuntimeException(
                        String.format(
                                "Failed to get schema information for %s",
                                TopicName.get(topic).toString()),
                        pae);
            }
        } catch (Throwable e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to get schema information for %s",
                            TopicName.get(topic).toString()),
                    e);
        }

        if (existingSchema == null) {
            PostSchemaPayload pl = new PostSchemaPayload();
            try {
                pl.setType(schemaInfo.getType().name());
                pl.setSchema(getSchemaString(schemaInfo));
                pl.setProperties(schemaInfo.getProperties());
                admin.schemas().createSchema(TopicName.get(topic).toString(), pl);
            } catch (PulsarAdminException pae) {
                if (pae.getStatusCode() == 404) {
                    throw new RuntimeException(
                            String.format(
                                    "Create schema for %s get 404",
                                    TopicName.get(topic).toString()),
                            pae);
                } else {
                    throw new RuntimeException(
                            String.format(
                                    "Failed to create schema information for %s",
                                    TopicName.get(topic).toString()),
                            pae);
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to set schema information for %s",
                                TopicName.get(topic).toString()),
                        e);
            } catch (Throwable e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to create schema information for %s",
                                TopicName.get(topic).toString()),
                        e);
            }
        } else if (!schemaEqualsIgnoreProperties(schemaInfo, existingSchema)
                && !compatibleSchema(existingSchema, schemaInfo)) {
            throw new RuntimeException("Writing to a topic which have incompatible schema");
        }
    }

    public static void deletePulsarSchema(PulsarAdmin admin, String topic) {
        try {
            admin.schemas().deleteSchema(topic);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static boolean schemaEqualsIgnoreProperties(
            SchemaInfo schemaInfo, SchemaInfo existingSchema) {
        return existingSchema.getType().equals(schemaInfo.getType())
                && Arrays.equals(existingSchema.getSchema(), schemaInfo.getSchema());
    }

    // TODO handle the exception
    private static String getSchemaString(SchemaInfo schemaInfo) throws IOException {
        final byte[] schemaData = schemaInfo.getSchema();
        if (null == schemaData) {
            return null;
        }
        if (schemaInfo.getType() == SchemaType.KEY_VALUE) {
            return DefaultImplementation.getDefaultImplementation()
                    .convertKeyValueSchemaInfoDataToString(
                            DefaultImplementation.getDefaultImplementation()
                                    .decodeKeyValueSchemaInfo(schemaInfo));
        }
        return new String(schemaData, StandardCharsets.UTF_8);
    }

    public static boolean compatibleSchema(SchemaInfo s1, SchemaInfo s2) {
        if (s1.getType() == SchemaType.NONE && s2.getType() == SchemaType.BYTES) {
            return true;
        } else {
            return s1.getType() == SchemaType.BYTES && s2.getType() == SchemaType.NONE;
        }
    }

    public static SchemaInfoImpl getSchemaInfo(SchemaType type, DataType dataType) {
        byte[] schemaBytes = getAvroSchema(dataType).toString().getBytes(StandardCharsets.UTF_8);
        return SchemaInfoImpl.builder()
                .name("Record")
                .schema(schemaBytes)
                .type(type)
                .properties(Collections.emptyMap())
                .build();
    }

    public static org.apache.avro.Schema getAvroSchema(DataType dataType) {
        org.apache.avro.Schema schema =
                AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        if (schema.isNullable()) {
            schema =
                    schema.getTypes().stream()
                            .filter(s -> s.getType() == RECORD)
                            .findAny()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "not support DataType: "
                                                            + dataType.toString()));
        }
        return schema;
    }
}
