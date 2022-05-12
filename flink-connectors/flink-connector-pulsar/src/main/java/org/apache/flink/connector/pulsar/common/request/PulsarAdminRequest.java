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

package org.apache.flink.connector.pulsar.common.request;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.PulsarConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.RateLimiter;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_RATES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_RETRIES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_REQUEST_SLEEP_TIME;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.voidSupplier;
import static org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/**
 * A wrapper for PulsarAdmin, used to add ratelimit and retry logic for the query. All the {@link
 * PulsarAdminException} would be sneakily thrown. Remember to catch it if you needs.
 */
@Internal
public class PulsarAdminRequest implements Closeable {

    // Pulsar isn't allowed to convert a non-partitioned topic into a partitioned topic.
    // So we can just cache all the non-partitioned topics for speeding up the query time.
    private static final Set<String> NON_PARTITIONED_TOPICS = ConcurrentHashMap.newKeySet();

    private final PulsarAdmin pulsarAdmin;
    private final int retryTimes;
    private final long sleepTime;
    private final RateLimiter rateLimiter;

    public PulsarAdminRequest(PulsarConfiguration configuration) {
        this(createAdmin(configuration), configuration);
    }

    @VisibleForTesting
    public PulsarAdminRequest(PulsarAdmin pulsarAdmin, Configuration configuration) {
        this.pulsarAdmin = pulsarAdmin;
        this.retryTimes = configuration.get(PULSAR_ADMIN_REQUEST_RETRIES);
        this.sleepTime = configuration.get(PULSAR_ADMIN_REQUEST_SLEEP_TIME);
        this.rateLimiter = RateLimiter.create(configuration.get(PULSAR_ADMIN_REQUEST_RATES));
    }

    /**
     * Create a given subscription on the topic if it's not existed. We will return true if we
     * create subscription this time.
     */
    public boolean createSubscriptionIfNotExist(
            String topic, String subscription, MessageId initial) throws PulsarAdminException {
        List<String> subscriptions = request(() -> pulsarAdmin.topics().getSubscriptions(topic));
        if (!subscriptions.contains(subscription)) {
            request(() -> pulsarAdmin.topics().createSubscription(topic, subscription, initial));
            return true;
        }

        return false;
    }

    /** Reset the subscription's cursor to given position. */
    public void resetCursor(String topic, String subscription, MessageId position)
            throws PulsarAdminException {
        request(() -> pulsarAdmin.topics().resetCursor(topic, subscription, position));
    }

    /**
     * Query related message id by the publishing time. We will find the earliest message which its
     * publishing time is above the given timestamp.
     */
    public MessageId getMessageIdByPublishTime(String topic, long timestamp)
            throws PulsarAdminException {
        return request(() -> pulsarAdmin.topics().getMessageIdByTimestamp(topic, timestamp));
    }

    /**
     * Query the topic metadata with the non-partitioned cache. We will retry the query when meeting
     * exceptions.
     */
    public TopicMetadata getTopicMetadata(String topic) throws PulsarAdminException {
        if (NON_PARTITIONED_TOPICS.contains(topic)) {
            return new TopicMetadata(topic, NON_PARTITIONED);
        } else {
            PartitionedTopicMetadata metadata =
                    request(() -> pulsarAdmin.topics().getPartitionedTopicMetadata(topic));
            if (metadata.partitions == NON_PARTITIONED) {
                NON_PARTITIONED_TOPICS.add(topic);
            }
            return new TopicMetadata(topic, metadata.partitions);
        }
    }

    /** Get all the topic names (without partitions) under the namespace. */
    public List<String> getTopics(String namespace) throws PulsarAdminException {
        List<String> partitions = request(() -> pulsarAdmin.namespaces().getTopics(namespace));
        return partitions.stream().map(TopicNameUtils::topicName).distinct().collect(toList());
    }

    public void createNonPartitionedTopic(String topic) throws PulsarAdminException {
        request(() -> pulsarAdmin.topics().createNonPartitionedTopic(topic));
    }

    public void createPartitionedTopic(String topic, int partitionSize)
            throws PulsarAdminException {
        request(() -> pulsarAdmin.topics().createPartitionedTopic(topic, partitionSize));
    }

    public PulsarAdmin pulsarAdmin() {
        return pulsarAdmin;
    }

    private <R extends PulsarAdminException> void request(ThrowingRunnable<R> runnable)
            throws PulsarAdminException {
        doRequest(voidSupplier(runnable), retryTimes);
    }

    private <T, R extends PulsarAdminException> T request(SupplierWithException<T, R> supplier)
            throws PulsarAdminException {
        return doRequest(supplier, retryTimes);
    }

    private <T, R extends PulsarAdminException> T doRequest(
            SupplierWithException<T, R> supplier, int times) throws PulsarAdminException {
        while (true) {
            // Make sure the request in the given rate.
            rateLimiter.acquire();

            try {
                return supplier.get();
            } catch (PulsarAdminException e) {
                Optional<NotFoundException> notFound = findThrowable(e, NotFoundException.class);
                if (notFound.isPresent()) {
                    // This means the query topic is not existed on Pulsar.
                    // No need to retry the query.
                    throw notFound.get();
                } else if (times <= 1) {
                    throw e;
                } else {
                    sleepUninterruptibly(sleepTime, MILLISECONDS);
                    times = times - 1;
                }
            }
        }
    }

    @Override
    public void close() {
        pulsarAdmin.close();
    }
}
