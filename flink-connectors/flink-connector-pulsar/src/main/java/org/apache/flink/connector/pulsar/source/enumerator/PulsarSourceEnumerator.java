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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.pulsar.common.request.PulsarAdminRequest;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.assigner.SplitAssigner;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.CursorPosition;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState.initialState;
import static org.apache.flink.connector.pulsar.source.enumerator.assigner.SplitAssignerFactory.createAssigner;

/** The enumerator class for the pulsar source. */
@Internal
public class PulsarSourceEnumerator
        implements SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceEnumerator.class);

    private final PulsarAdminRequest adminRequest;
    private final PulsarSubscriber subscriber;
    private final StartCursor startCursor;
    private final RangeGenerator rangeGenerator;
    private final SourceConfiguration sourceConfiguration;
    private final SplitEnumeratorContext<PulsarPartitionSplit> context;
    private final SplitAssigner splitAssigner;

    public PulsarSourceEnumerator(
            PulsarSubscriber subscriber,
            StartCursor startCursor,
            StopCursor stopCursor,
            RangeGenerator rangeGenerator,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<PulsarPartitionSplit> context) {
        this(
                subscriber,
                startCursor,
                stopCursor,
                rangeGenerator,
                sourceConfiguration,
                context,
                initialState());
    }

    public PulsarSourceEnumerator(
            PulsarSubscriber subscriber,
            StartCursor startCursor,
            StopCursor stopCursor,
            RangeGenerator rangeGenerator,
            SourceConfiguration sourceConfiguration,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumState enumState) {
        this.adminRequest = new PulsarAdminRequest(sourceConfiguration);
        this.subscriber = subscriber;
        this.startCursor = startCursor;
        this.rangeGenerator = rangeGenerator;
        this.sourceConfiguration = sourceConfiguration;
        this.context = context;
        this.splitAssigner = createAssigner(stopCursor, sourceConfiguration, context, enumState);
    }

    @Override
    public void start() {
        rangeGenerator.open(sourceConfiguration);

        // Check the pulsar topic information and convert it into source split.
        if (sourceConfiguration.isEnablePartitionDiscovery()) {
            LOG.info(
                    "Starting the PulsarSourceEnumerator for subscription {} "
                            + "with partition discovery interval of {} ms.",
                    subscriptionDesc(),
                    sourceConfiguration.getPartitionDiscoveryIntervalMs());
            context.callAsync(
                    this::getSubscribedTopicPartitions,
                    this::checkPartitionChanges,
                    0,
                    sourceConfiguration.getPartitionDiscoveryIntervalMs());
        } else {
            LOG.info(
                    "Starting the PulsarSourceEnumerator for subscription {} "
                            + "without periodic partition discovery.",
                    subscriptionDesc());
            context.callAsync(this::getSubscribedTopicPartitions, this::checkPartitionChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the pulsar source pushes splits eagerly, rather than act upon split requests.
    }

    @Override
    public void addSplitsBack(List<PulsarPartitionSplit> splits, int subtaskId) {
        // Put the split back to current pending splits.
        splitAssigner.addSplitsBack(splits, subtaskId);

        // If the failed subtask has already restarted, we need to assign pending splits to it.
        if (context.registeredReaders().containsKey(subtaskId)) {
            LOG.debug(
                    "Reader {} has been restarted after crashing, we will put splits back to it.",
                    subtaskId);
            // Reassign for all readers in case of adding splits after scale up/down.
            List<Integer> readers = new ArrayList<>(context.registeredReaders().keySet());
            assignPendingPartitionSplits(readers);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to PulsarSourceEnumerator for subscription {}.",
                subtaskId,
                subscriptionDesc());
        assignPendingPartitionSplits(singletonList(subtaskId));
    }

    @Override
    public PulsarSourceEnumState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState();
    }

    @Override
    public void close() {
        if (adminRequest != null) {
            adminRequest.close();
        }
    }

    // ----------------- private methods -------------------

    /** Convert the subscription into a readable string. */
    private String subscriptionDesc() {
        return String.format(
                "%s(%s,%s)",
                sourceConfiguration.getSubscriptionName(),
                sourceConfiguration.getSubscriptionType(),
                sourceConfiguration.getSubscriptionMode());
    }

    /**
     * List subscribed topic partitions on Pulsar cluster.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * requires network I/O with Pulsar cluster.
     *
     * @return Set of subscribed {@link TopicPartition}s
     */
    private Set<TopicPartition> getSubscribedTopicPartitions() {
        int parallelism = context.currentParallelism();
        return subscriber.getSubscribedTopicPartitions(adminRequest, rangeGenerator, parallelism);
    }

    /**
     * Check if there are any partition changes within subscribed topic partitions fetched by worker
     * thread, and convert them to splits, then assign them to pulsar readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param fetchedPartitions Map from topic name to its description
     * @param throwable Exception in worker thread
     */
    private void checkPartitionChanges(Set<TopicPartition> fetchedPartitions, Throwable throwable) {
        if (throwable != null) {
            throw new FlinkRuntimeException(
                    "Failed to list subscribed topic partitions due to ", throwable);
        }

        // Append the partitions into current assignment state.
        List<TopicPartition> newPartitions =
                splitAssigner.registerTopicPartitions(fetchedPartitions);
        createSubscription(newPartitions);

        // Assign the new readers.
        List<Integer> registeredReaders = new ArrayList<>(context.registeredReaders().keySet());
        assignPendingPartitionSplits(registeredReaders);
    }

    /** Create subscription on topic partition if it doesn't exist. */
    private void createSubscription(List<TopicPartition> newPartitions) {
        for (TopicPartition partition : newPartitions) {
            String topicName = partition.getFullTopicName();
            String subscriptionName = sourceConfiguration.getSubscriptionName();
            CursorPosition position =
                    startCursor.position(partition.getTopic(), partition.getPartitionId());
            MessageId initialPosition = queryInitialPosition(topicName, position);

            sneakyAdmin(
                    () ->
                            adminRequest.createSubscriptionIfNotExist(
                                    topicName, subscriptionName, initialPosition));
        }
    }

    /** Query the available message id from Pulsar. */
    private MessageId queryInitialPosition(String topicName, CursorPosition position) {
        CursorPosition.Type type = position.getType();
        if (type == CursorPosition.Type.TIMESTAMP) {
            return sneakyAdmin(
                    () ->
                            adminRequest.getMessageIdByPublishTime(
                                    topicName, position.getTimestamp()));
        } else if (type == CursorPosition.Type.MESSAGE_ID) {
            return position.getMessageId();
        } else {
            throw new UnsupportedOperationException("We don't support this seek type " + type);
        }
    }

    /** Query the unassigned splits and assign them to the available readers. */
    private void assignPendingPartitionSplits(List<Integer> pendingReaders) {
        // Validate the reader.
        pendingReaders.forEach(
                reader -> {
                    if (!context.registeredReaders().containsKey(reader)) {
                        throw new IllegalStateException(
                                "Reader " + reader + " is not registered to source coordinator");
                    }
                });

        // Assign splits to downstream readers.
        splitAssigner
                .createAssignment(pendingReaders)
                .ifPresent(
                        assignments -> {
                            LOG.info(
                                    "The split assignment results are: {}",
                                    assignments.assignment());
                            context.assignSplits(assignments);
                        });

        // If periodically partition discovery is turned off and the initializing discovery has done
        // signal NoMoreSplitsEvent to pending readers.
        for (Integer reader : pendingReaders) {
            if (splitAssigner.noMoreSplits(reader)) {
                LOG.debug(
                        "No more PulsarPartitionSplits to assign."
                                + " Sending NoMoreSplitsEvent to reader {} in subscription {}.",
                        reader,
                        subscriptionDesc());
                context.signalNoMoreSplits(reader);
            }
        }
    }
}
