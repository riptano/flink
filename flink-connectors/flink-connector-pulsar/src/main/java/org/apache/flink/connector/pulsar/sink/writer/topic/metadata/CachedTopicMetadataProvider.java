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

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import org.apache.pulsar.client.admin.PulsarAdminException;

import java.util.concurrent.TimeUnit;

/**
 * The default implementation for querying topic metadata. The query result would be cached by
 * positive {@link SinkConfiguration#getTopicMetadataRefreshInterval()}.
 */
@Internal
public class CachedTopicMetadataProvider implements TopicMetadataProvider {

    private final PulsarAdminRequest adminRequest;
    private final Cache<String, TopicMetadata> metadataCache;

    public CachedTopicMetadataProvider(
            PulsarAdminRequest adminRequest, SinkConfiguration sinkConfiguration) {
        this.adminRequest = adminRequest;

        long refreshInterval = sinkConfiguration.getTopicMetadataRefreshInterval();
        if (refreshInterval <= 0) {
            // Disable cache expires, the query result will never be kept in the cache.
            this.metadataCache = null;
        } else {
            this.metadataCache =
                    CacheBuilder.newBuilder()
                            .expireAfterWrite(refreshInterval, TimeUnit.MILLISECONDS)
                            .maximumSize(1000)
                            .build();
        }
    }

    @Override
    public TopicMetadata query(String topic) throws PulsarAdminException {
        TopicMetadata metadata = metadataCache == null ? null : metadataCache.getIfPresent(topic);

        if (metadata == null) {
            metadata = adminRequest.getTopicMetadata(topic);
            if (metadataCache != null) {
                metadataCache.put(topic, metadata);
            }
        }

        return metadata;
    }
}
