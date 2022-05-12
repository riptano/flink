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

package org.apache.flink.connector.pulsar.source.reader.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.common.request.PulsarAdminRequest;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.source.PulsarOrderedSourceReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.source.config.CursorVerification.FAIL_ON_MISMATCH;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.MessageIdUtils.nextMessageId;

/**
 * The split reader a given {@link PulsarPartitionSplit}, it would be closed once the {@link
 * PulsarOrderedSourceReader} is closed.
 */
@Internal
public class PulsarOrderedPartitionSplitReader extends PulsarPartitionSplitReaderBase {
    private static final Logger LOG =
            LoggerFactory.getLogger(PulsarOrderedPartitionSplitReader.class);

    public PulsarOrderedPartitionSplitReader(
            PulsarClient pulsarClient,
            PulsarAdminRequest adminRequest,
            SourceConfiguration sourceConfiguration,
            Schema<byte[]> schema,
            @Nullable CryptoKeyReader cryptoKeyReader) {
        super(pulsarClient, adminRequest, sourceConfiguration, schema, cryptoKeyReader);
    }

    @Override
    protected Message<byte[]> pollMessage(Duration timeout) throws PulsarClientException {
        return pulsarConsumer.receive(Math.toIntExact(timeout.toMillis()), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void finishedPollMessage(Message<?> message) {
        // Nothing to do here.
        LOG.debug("Finished polling message {}", message);
    }

    @Override
    protected void beforeCreatingConsumer(PulsarPartitionSplit split) {
        MessageId latestConsumedId = split.getLatestConsumedId();

        // Reset the start position for ordered pulsar consumer.
        if (latestConsumedId != null) {
            LOG.info("Reset subscription position by the checkpoint {}", latestConsumedId);
            try {
                MessageId initialPosition;
                if (latestConsumedId == MessageId.latest
                        || latestConsumedId == MessageId.earliest) {
                    // for compatibility
                    initialPosition = latestConsumedId;
                } else {
                    initialPosition = nextMessageId(latestConsumedId);
                }

                // Remove Consumer.seek() here for waiting for pulsar-client-all 2.12.0
                // See https://github.com/apache/pulsar/issues/16757 for more details.

                String topicName = split.getPartition().getFullTopicName();
                String subscriptionName = sourceConfiguration.getSubscriptionName();

                // If this subscription is not available. Just create it.
                if (!adminRequest.createSubscriptionIfNotExist(
                        topicName, subscriptionName, initialPosition)) {
                    // Reset the subscription if this is existed.
                    adminRequest.resetCursor(topicName, subscriptionName, initialPosition);
                }
            } catch (PulsarAdminException e) {
                if (sourceConfiguration.getVerifyInitialOffsets() == FAIL_ON_MISMATCH) {
                    throw new IllegalArgumentException(e);
                } else {
                    // WARN_ON_MISMATCH would just print this warning message.
                    // No need to print the stacktrace.
                    LOG.warn(
                            "Failed to reset cursor to {} on partition {}",
                            latestConsumedId,
                            split.getPartition(),
                            e);
                }
            }
        }
    }

    public void notifyCheckpointComplete(TopicPartition partition, MessageId offsetsToCommit) {
        if (pulsarConsumer == null) {
            this.pulsarConsumer = createPulsarConsumer(partition);
        }

        sneakyClient(() -> pulsarConsumer.acknowledgeCumulative(offsetsToCommit));
    }
}
