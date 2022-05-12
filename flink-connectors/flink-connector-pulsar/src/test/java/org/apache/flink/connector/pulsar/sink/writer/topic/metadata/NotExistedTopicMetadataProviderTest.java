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

import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_SINK_DEFAULT_TOPIC_PARTITIONS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_SINK_TOPIC_AUTO_CREATION;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link NotExistedTopicMetadataProvider}. */
class NotExistedTopicMetadataProviderTest extends PulsarTestSuiteBase {

    @Test
    void autoTopicCreationForNotExisted() throws PulsarAdminException {
        // Create an existed topic
        operator().createTopic("existed-topic", 10);

        // This provider will create a topic with 5 partitions.
        SinkConfiguration configuration = configuration(5);
        PulsarAdminRequest adminRequest = new PulsarAdminRequest(operator().admin(), configuration);

        NotExistedTopicMetadataProvider provider1 =
                new NotExistedTopicMetadataProvider(adminRequest, configuration);

        TopicMetadata metadata1 = provider1.query("existed-topic");
        assertThat(metadata1).hasFieldOrPropertyWithValue("partitionSize", 10);

        TopicMetadata metadata2 = provider1.query("not-existed-topic-1");
        assertThat(metadata2).hasFieldOrPropertyWithValue("partitionSize", 5);

        // This provider will create a topic with 8 partitions.
        NotExistedTopicMetadataProvider provider2 =
                new NotExistedTopicMetadataProvider(adminRequest, configuration(8));

        TopicMetadata metadata3 = provider2.query("not-existed-topic-1");
        assertThat(metadata3).hasFieldOrPropertyWithValue("partitionSize", 5);

        TopicMetadata metadata4 = provider2.query("not-existed-topic-2");
        assertThat(metadata4).hasFieldOrPropertyWithValue("partitionSize", 8);
    }

    private SinkConfiguration configuration(int partitions) {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_SINK_TOPIC_AUTO_CREATION, true);
        configuration.set(PULSAR_SINK_DEFAULT_TOPIC_PARTITIONS, partitions);

        return new SinkConfiguration(configuration);
    }
}
