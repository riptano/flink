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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.request.PulsarAdminRequest;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link CachedTopicMetadataProvider}. */
class CachedTopicMetadataProviderTest extends PulsarTestSuiteBase {

    @Test
    void queryTopicsWhichIsNotExisted() {
        SinkConfiguration sinkConfiguration = new SinkConfiguration(new Configuration());
        PulsarAdminRequest adminRequest =
                new PulsarAdminRequest(operator().admin(), sinkConfiguration);
        CachedTopicMetadataProvider provider =
                new CachedTopicMetadataProvider(adminRequest, sinkConfiguration);

        String notExistedTopic = "not-existed-topic-" + randomAlphanumeric(8);

        assertFalse(operator().topicExists(notExistedTopic));
        assertThrows(PulsarAdminException.class, () -> provider.query(notExistedTopic));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void queryTopicsWhichIsExisted(boolean enableCache) throws PulsarAdminException {
        String topicName = "existed-topic-" + randomAlphanumeric(8);
        operator().createTopic(topicName, 8);

        Configuration configuration = new Configuration();
        if (!enableCache) {
            configuration.set(PULSAR_TOPIC_METADATA_REFRESH_INTERVAL, -1L);
        }

        SinkConfiguration sinkConfiguration = new SinkConfiguration(configuration);
        PulsarAdminRequest adminRequest =
                new PulsarAdminRequest(operator().admin(), sinkConfiguration);
        CachedTopicMetadataProvider provider =
                new CachedTopicMetadataProvider(adminRequest, sinkConfiguration);

        TopicMetadata metadata1 = provider.query(topicName);
        assertThat(metadata1).hasFieldOrPropertyWithValue("partitionSize", 8);

        // Increase topic partition, but the query result didn't get changed immediately with cache.
        operator().increaseTopicPartitions(topicName, 16);

        TopicMetadata metadata2 = provider.query(topicName);
        if (enableCache) {
            assertThat(metadata2).hasFieldOrPropertyWithValue("partitionSize", 8);
        } else {
            assertThat(metadata2).hasFieldOrPropertyWithValue("partitionSize", 16);
        }
    }

    @Test
    void queryTopicsWhichIsExistedWithoutCache() {
        String topicName = "existed-topic-" + randomAlphanumeric(8);
        operator().createTopic(topicName, 8);

        Configuration configuration = new Configuration();
        configuration.set(PULSAR_TOPIC_METADATA_REFRESH_INTERVAL, -1L);

        SinkConfiguration sinkConfiguration = new SinkConfiguration(configuration);
        PulsarAdminRequest adminRequest =
                new PulsarAdminRequest(operator().admin(), sinkConfiguration);
        CachedTopicMetadataProvider provider =
                new CachedTopicMetadataProvider(adminRequest, sinkConfiguration);

        // TODO Finish this test?
    }
}
