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

package org.apache.flink.connector.pulsar.sink.writer.topic.register;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.connector.pulsar.common.request.PulsarAdminRequest;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicRegister;
import org.apache.flink.connector.pulsar.sink.writer.topic.metadata.NotExistedTopicMetadataProvider;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;

import org.apache.flink.shaded.guava30.com.google.common.base.Objects;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isPartitioned;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/**
 * We need the latest topic metadata for making sure the Pulsar sink would use the newly created
 * topic partitions. This routing policy would be different compared with Pulsar Client built-in
 * logic. We use Flink's ProcessingTimer as the executor.
 */
@Internal
public class FixedTopicRegister<IN> implements TopicRegister<IN> {
    private static final long serialVersionUID = 6186948471557507522L;

    private static final Logger LOG = LoggerFactory.getLogger(FixedTopicRegister.class);

    private final ImmutableList<String> partitionedTopics;
    private final Map<String, Integer> topicMetadata;
    private volatile ImmutableList<String> availableTopics;

    // Dynamic fields.
    private transient PulsarAdminRequest adminRequest;
    private transient Long topicMetadataRefreshInterval;
    private transient ProcessingTimeService timeService;
    private transient NotExistedTopicMetadataProvider metadataProvider;

    public FixedTopicRegister(List<String> topics) {
        List<String> partitions = new ArrayList<>(topics.size());
        Map<String, Integer> metadata = new HashMap<>(topics.size());
        for (String topic : topics) {
            if (isPartitioned(topic)) {
                partitions.add(topic);
            } else {
                // This would be updated when open writing.
                metadata.put(topic, -1);
            }
        }

        this.partitionedTopics = ImmutableList.copyOf(partitions);
        this.topicMetadata = metadata;
        this.availableTopics = ImmutableList.of();
    }

    @Override
    public void open(SinkConfiguration sinkConfiguration, ProcessingTimeService timeService) {
        if (topicMetadata.isEmpty()) {
            LOG.info("No topics have been provided, skip listener initialize.");
            return;
        }

        // Initialize listener properties.
        this.adminRequest = new PulsarAdminRequest(sinkConfiguration);
        this.topicMetadataRefreshInterval = sinkConfiguration.getTopicMetadataRefreshInterval();
        this.timeService = timeService;
        this.metadataProvider =
                new NotExistedTopicMetadataProvider(adminRequest, sinkConfiguration);

        // Initialize the topic metadata. Quit if fail to connect to Pulsar.
        sneakyAdmin(this::updateTopicMetadata);

        // Register time service, if user enable the topic metadata update.
        if (topicMetadataRefreshInterval > 0) {
            triggerNextTopicMetadataUpdate(true);
        }
    }

    @Override
    public List<String> topics(IN in) {
        if (availableTopics.isEmpty()
                && (!partitionedTopics.isEmpty() || !topicMetadata.isEmpty())) {
            List<String> results = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : topicMetadata.entrySet()) {
                int partitionNums = entry.getValue();
                // Get all topics from partitioned and non-partitioned topic names
                if (partitionNums == NON_PARTITIONED) {
                    results.add(topicName(entry.getKey()));
                } else {
                    for (int i = 0; i < partitionNums; i++) {
                        results.add(topicNameWithPartition(entry.getKey(), i));
                    }
                }
            }

            results.addAll(partitionedTopics);
            this.availableTopics = ImmutableList.copyOf(results);
        }

        return availableTopics;
    }

    @Override
    public void close() throws IOException {
        if (adminRequest != null) {
            adminRequest.close();
        }
    }

    private void triggerNextTopicMetadataUpdate(boolean initial) {
        if (!initial) {
            // We should update the topic metadata, ignore the pulsar admin exception.
            try {
                updateTopicMetadata();
            } catch (PulsarAdminException e) {
                LOG.warn("", e);
            }
        }

        // Register next timer.
        long currentProcessingTime = timeService.getCurrentProcessingTime();
        long triggerTime = currentProcessingTime + topicMetadataRefreshInterval;
        timeService.registerTimer(triggerTime, time -> triggerNextTopicMetadataUpdate(false));
    }

    private void updateTopicMetadata() throws PulsarAdminException {
        boolean shouldUpdate = false;

        for (Map.Entry<String, Integer> entry : topicMetadata.entrySet()) {
            String topic = entry.getKey();
            TopicMetadata metadata = metadataProvider.query(topic);

            // Update topic metadata if it has been changed.
            if (!Objects.equal(entry.getValue(), metadata.getPartitionSize())) {
                entry.setValue(metadata.getPartitionSize());
                shouldUpdate = true;
            }
        }

        // Clear available topics if the topic metadata has been changed.
        if (shouldUpdate) {
            this.availableTopics = ImmutableList.of();
        }
    }
}
