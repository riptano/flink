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
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicExtractor;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicRegister;
import org.apache.flink.connector.pulsar.sink.writer.topic.metadata.CachedTopicMetadataProvider;
import org.apache.flink.connector.pulsar.sink.writer.topic.metadata.NotExistedTopicMetadataProvider;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import org.apache.pulsar.client.admin.PulsarAdminException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The register for returning dynamic topic partitions information. */
@Internal
public class DynamicTopicRegister<IN> implements TopicRegister<IN> {
    private static final long serialVersionUID = 4374769306761301456L;

    private final TopicExtractor<IN> topicExtractor;

    // Dynamic fields.
    private transient PulsarAdminRequest adminRequest;
    private transient CachedTopicMetadataProvider cachedMetadataProvider;
    private transient NotExistedTopicMetadataProvider notExistedMetadataProvider;
    private transient Cache<String, List<String>> partitionsCache;

    public DynamicTopicRegister(TopicExtractor<IN> topicExtractor) {
        this.topicExtractor = checkNotNull(topicExtractor);
    }

    @Override
    public List<String> topics(IN in) {
        TopicPartition partition = topicExtractor.extract(in, cachedMetadataProvider);
        String topicName = partition.getFullTopicName();

        if (partition.isPartition()) {
            return singletonList(topicName);
        } else {
            try {
                List<String> topics = partitionsCache.getIfPresent(topicName);
                if (topics == null) {
                    topics = queryTopics(topicName);
                    partitionsCache.put(topicName, topics);
                }

                return topics;
            } catch (PulsarAdminException e) {
                throw new FlinkRuntimeException(
                        "Failed to query Pulsar topic partitions.", e.getCause());
            }
        }
    }

    private List<String> queryTopics(String topic) throws PulsarAdminException {
        TopicMetadata metadata = notExistedMetadataProvider.query(topic);
        if (metadata.isPartitioned()) {
            int partitionSize = metadata.getPartitionSize();
            List<String> partitions = new ArrayList<>(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                partitions.add(topicNameWithPartition(topic, i));
            }
            return partitions;
        } else {
            return singletonList(topic);
        }
    }

    @Override
    public void open(SinkConfiguration sinkConfiguration, ProcessingTimeService timeService) {
        // Initialize Pulsar admin instance.
        this.adminRequest = new PulsarAdminRequest(sinkConfiguration);
        this.cachedMetadataProvider =
                new CachedTopicMetadataProvider(adminRequest, sinkConfiguration);
        this.notExistedMetadataProvider =
                new NotExistedTopicMetadataProvider(adminRequest, sinkConfiguration);

        long refreshInterval = sinkConfiguration.getTopicMetadataRefreshInterval();
        if (refreshInterval <= 0) {
            refreshInterval = Long.MAX_VALUE;
        }
        this.partitionsCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(refreshInterval, TimeUnit.MILLISECONDS)
                        .maximumSize(1000)
                        .build();

        // Open the topic extractor instance.
        topicExtractor.open(sinkConfiguration);
    }

    @Override
    public void close() throws IOException {
        if (adminRequest != null) {
            adminRequest.close();
        }
    }
}
