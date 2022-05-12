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

package org.apache.flink.connector.pulsar.sink.writer.topic.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.common.request.PulsarAdminRequest;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicExtractor.TopicMetadataProvider;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/**
 * Shared topic metadata query for {@code TopicRegister}. We would auto create the topics if you
 * enable this feature.
 */
@Internal
public class NotExistedTopicMetadataProvider implements TopicMetadataProvider {

    private final PulsarAdminRequest adminRequest;
    private final boolean enableTopicAutoCreation;
    private final int defaultTopicPartitions;

    public NotExistedTopicMetadataProvider(
            PulsarAdminRequest adminRequest, SinkConfiguration sinkConfiguration) {
        this.adminRequest = adminRequest;
        this.enableTopicAutoCreation = sinkConfiguration.isEnableTopicAutoCreation();
        this.defaultTopicPartitions = sinkConfiguration.getDefaultTopicPartitions();
    }

    @Override
    public TopicMetadata query(String topic) throws PulsarAdminException {
        try {
            return adminRequest.getTopicMetadata(topic);
        } catch (PulsarAdminException e) {
            if (findThrowable(e, NotFoundException.class).isPresent() && enableTopicAutoCreation) {
                createTopic(topic);
                return new TopicMetadata(topic, defaultTopicPartitions);
            } else {
                throw e;
            }
        }
    }

    private void createTopic(String topic) throws PulsarAdminException {
        if (defaultTopicPartitions == NON_PARTITIONED) {
            adminRequest.createNonPartitionedTopic(topic);
        } else {
            adminRequest.createPartitionedTopic(topic, defaultTopicPartitions);
        }
    }
}
