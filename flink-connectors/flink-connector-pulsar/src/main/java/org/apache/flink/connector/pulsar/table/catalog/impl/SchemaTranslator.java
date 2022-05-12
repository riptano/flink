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

import org.apache.flink.connector.pulsar.table.format.protobufnative.PulsarProtobufNativeFormatFactory;
import org.apache.flink.formats.avro.AvroFormatFactory;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.formats.raw.RawFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import com.google.protobuf.Descriptors;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Translate a Pulsar Schema to Flink Table Schema. */
public class SchemaTranslator {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaTranslator.class);

    public static final String SINGLE_FIELD_FIELD_NAME = "value";

    private final boolean useMetadataFields;

    public SchemaTranslator(boolean useMetadataFields) {
        this.useMetadataFields = useMetadataFields;
    }

    public org.apache.flink.table.api.Schema pulsarSchemaToFlinkSchema(SchemaInfo pulsarSchema)
            throws IncompatibleSchemaException {
        final DataType fieldsDataType = pulsarSchemaToPhysicalFields(pulsarSchema);
        org.apache.flink.table.api.Schema.Builder schemaBuilder =
                org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(fieldsDataType);

        if (useMetadataFields) {
            throw new UnsupportedOperationException(
                    "Querying Pulsar Metadata is not supported yet");
        }

        return schemaBuilder.build();
    }

    public DataType pulsarSchemaToPhysicalFields(SchemaInfo schemaInfo)
            throws IncompatibleSchemaException {
        List<DataTypes.Field> mainSchema = new ArrayList<>();
        DataType dataType = schemaInfo2SqlType(schemaInfo);
        // ROW and STRUCTURED are FieldsDataType
        if (dataType instanceof FieldsDataType) {
            FieldsDataType fieldsDataType = (FieldsDataType) dataType;
            RowType rowType = (RowType) fieldsDataType.getLogicalType();
            List<String> fieldNames = rowType.getFieldNames();
            for (int i = 0; i < fieldNames.size(); i++) {
                org.apache.flink.table.types.logical.LogicalType logicalType = rowType.getTypeAt(i);
                DataTypes.Field field =
                        DataTypes.FIELD(
                                fieldNames.get(i),
                                TypeConversions.fromLogicalToDataType(logicalType));
                mainSchema.add(field);
            }

        } else {
            mainSchema.add(DataTypes.FIELD(SINGLE_FIELD_FIELD_NAME, dataType));
        }

        return DataTypes.ROW(mainSchema.toArray(new DataTypes.Field[0]));
    }

    public DataType schemaInfo2SqlType(SchemaInfo si) throws IncompatibleSchemaException {
        switch (si.getType()) {
            case NONE:
            case BYTES:
                return DataTypes.BYTES();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case LOCAL_DATE:
                return DataTypes.DATE();
            case LOCAL_TIME:
                return DataTypes.TIME();
            case STRING:
                return DataTypes.STRING();
            case LOCAL_DATE_TIME:
                return DataTypes.TIMESTAMP(3);
            case INT8:
                return DataTypes.TINYINT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case FLOAT:
                return DataTypes.FLOAT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case INT16:
                return DataTypes.SMALLINT();
            case AVRO:
            case JSON:
                String avroSchemaString = new String(si.getSchema(), StandardCharsets.UTF_8);
                return AvroSchemaConverter.convertToDataType(avroSchemaString);
            case PROTOBUF_NATIVE:
                Descriptors.Descriptor descriptor =
                        ((GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(si))
                                .getProtobufNativeSchema();
                return protoDescriptorToSqlType(descriptor);

            default:
                throw new UnsupportedOperationException(
                        String.format("We do not support %s currently.", si.getType()));
        }
    }

    public static DataType protoDescriptorToSqlType(Descriptors.Descriptor descriptor)
            throws IncompatibleSchemaException {
        List<DataTypes.Field> fields = new ArrayList<>();
        List<Descriptors.FieldDescriptor> protoFields = descriptor.getFields();

        for (Descriptors.FieldDescriptor fieldDescriptor : protoFields) {
            DataType fieldType = protoFieldDescriptorToSqlType(fieldDescriptor);
            fields.add(DataTypes.FIELD(fieldDescriptor.getName(), fieldType));
        }

        if (fields.isEmpty()) {
            throw new IllegalArgumentException("No FieldDescriptors found");
        }
        return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
    }

    private static DataType protoFieldDescriptorToSqlType(Descriptors.FieldDescriptor field)
            throws IncompatibleSchemaException {
        Descriptors.FieldDescriptor.JavaType type = field.getJavaType();
        DataType dataType;
        switch (type) {
            case BOOLEAN:
                dataType = DataTypes.BOOLEAN();
                break;
            case BYTE_STRING:
                dataType = DataTypes.BYTES();
                break;
            case DOUBLE:
                dataType = DataTypes.DOUBLE();
                break;
            case ENUM:
                dataType = DataTypes.STRING();
                break;
            case FLOAT:
                dataType = DataTypes.FLOAT();
                break;
            case INT:
                dataType = DataTypes.INT();
                break;
            case LONG:
                dataType = DataTypes.BIGINT();
                break;
            case MESSAGE:
                Descriptors.Descriptor msg = field.getMessageType();
                if (field.isMapField()) {
                    // map
                    dataType =
                            DataTypes.MAP(
                                    protoFieldDescriptorToSqlType(msg.findFieldByName("key")),
                                    protoFieldDescriptorToSqlType(msg.findFieldByName("value")));
                } else {
                    // row
                    dataType = protoDescriptorToSqlType(field.getMessageType());
                }
                break;
            case STRING:
                dataType = DataTypes.STRING();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown type: "
                                + type.toString()
                                + " for FieldDescriptor: "
                                + field.toString());
        }
        // list
        if (field.isRepeated() && !field.isMapField()) {
            dataType = DataTypes.ARRAY(dataType);
        }

        return dataType;
    }

    /**
     * This method is used to determine the Flink format to use for a native table.
     *
     * @param pulsarSchemaInfo
     * @return
     */
    public String decideDefaultFlinkFormat(SchemaInfo pulsarSchemaInfo) {
        String formatIdentifier = RawFormatFactory.IDENTIFIER;
        switch (pulsarSchemaInfo.getType()) {
            case JSON:
                formatIdentifier = JsonFormatFactory.IDENTIFIER;
                break;
            case AVRO:
                formatIdentifier = AvroFormatFactory.IDENTIFIER;
                break;
            case PROTOBUF_NATIVE:
                formatIdentifier = PulsarProtobufNativeFormatFactory.IDENTIFIER;
                break;
            case PROTOBUF:
            case AUTO_CONSUME:
            case AUTO:
            case AUTO_PUBLISH:
                LOG.error(
                        "Can't decide format for {} schema", pulsarSchemaInfo.getType().toString());
                throw new UnsupportedOperationException(
                        String.format(
                                "Can't decide format for %s schema",
                                pulsarSchemaInfo.getType().toString()));
            default:
                break;
        }
        return formatIdentifier;
    }
}
