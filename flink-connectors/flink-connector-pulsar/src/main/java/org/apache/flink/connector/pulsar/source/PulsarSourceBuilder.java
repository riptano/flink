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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigBuilder;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSchemaWrapper;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.SplitRangeGenerator;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;

import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static java.lang.Boolean.FALSE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ENABLE_TRANSACTION;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONSUMER_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_READ_SCHEMA_EVOLUTION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_READ_TRANSACTION_TIMEOUT;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.config.PulsarSourceConfigUtils.SOURCE_CONFIG_VALIDATOR;
import static org.apache.flink.util.InstantiationUtil.isSerializable;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The builder class for {@link PulsarSource} to make it easier for the users to construct a {@link
 * PulsarSource}.
 *
 * <p>The following example shows the minimum setup to create a PulsarSource that reads the String
 * values from a Pulsar topic.
 *
 * <pre>{@code
 * PulsarSource<String> source = PulsarSource
 *     .builder()
 *     .setServiceUrl(PULSAR_BROKER_URL)
 *     .setAdminUrl(PULSAR_BROKER_HTTP_URL)
 *     .setSubscriptionName("flink-source-1")
 *     .setTopics(Arrays.asList(TOPIC1, TOPIC2))
 *     .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
 *     .build();
 * }</pre>
 *
 * <p>The service url, admin url, subscription name, topics to consume, and the record deserializer
 * are required fields that must be set.
 *
 * <p>To specify the starting position of PulsarSource, one can call {@link
 * #setStartCursor(StartCursor)}.
 *
 * <p>By default, the PulsarSource runs in an {@link Boundedness#CONTINUOUS_UNBOUNDED} mode and
 * never stop until the Flink job is canceled or fails. To let the PulsarSource run in {@link
 * Boundedness#CONTINUOUS_UNBOUNDED} but stops at some given offsets, one can call {@link
 * #setUnboundedStopCursor(StopCursor)} and disable auto partition discovery as described below. For
 * example the following PulsarSource stops after it consumes up to a event time when the Flink
 * started.
 *
 * <p>To stop the connector user has to disable the auto partition discovery. As auto partition
 * discovery always expected new splits to come and not exiting. To disable auto partition
 * discovery, use builder.setConfig({@link
 * PulsarSourceOptions#PULSAR_PARTITION_DISCOVERY_INTERVAL_MS}, -1).
 *
 * <pre>{@code
 * PulsarSource<String> source = PulsarSource
 *     .builder()
 *     .setServiceUrl(PULSAR_BROKER_URL)
 *     .setAdminUrl(PULSAR_BROKER_HTTP_URL)
 *     .setSubscriptionName("flink-source-1")
 *     .setTopics(Arrays.asList(TOPIC1, TOPIC2))
 *     .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
 *     .setUnboundedStopCursor(StopCursor.atEventTime(System.currentTimeMillis()))
 *     .build();
 * }</pre>
 *
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public final class PulsarSourceBuilder<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceBuilder.class);

    private final PulsarConfigBuilder configBuilder;

    private PulsarSubscriber subscriber;
    private RangeGenerator rangeGenerator;
    private StartCursor startCursor;
    private StopCursor stopCursor;
    private Boundedness boundedness;
    private PulsarDeserializationSchema<OUT> deserializationSchema;
    @Nullable private CryptoKeyReader cryptoKeyReader;

    // private builder constructor.
    PulsarSourceBuilder() {
        this.configBuilder = new PulsarConfigBuilder();
        this.startCursor = StartCursor.defaultStartCursor();
        this.stopCursor = StopCursor.defaultStopCursor();
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * Sets the admin endpoint for the PulsarAdmin of the PulsarSource.
     *
     * @param adminUrl the url for the PulsarAdmin.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setAdminUrl(String adminUrl) {
        return setConfig(PULSAR_ADMIN_URL, adminUrl);
    }

    /**
     * Sets the server's link for the PulsarConsumer of the PulsarSource.
     *
     * @param serviceUrl the server url of the Pulsar cluster.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setServiceUrl(String serviceUrl) {
        return setConfig(PULSAR_SERVICE_URL, serviceUrl);
    }

    /**
     * Sets the name for this pulsar subscription.
     *
     * @param subscriptionName the server url of the Pulsar cluster.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setSubscriptionName(String subscriptionName) {
        return setConfig(PULSAR_SUBSCRIPTION_NAME, subscriptionName);
    }

    /**
     * {@link SubscriptionType} is the consuming behavior for pulsar, we would generator different
     * split by the given subscription type. Please take some time to consider which subscription
     * type matches your application best. Default is {@link SubscriptionType#Shared}.
     *
     * @param subscriptionType The type of subscription.
     * @return this PulsarSourceBuilder.
     * @see <a href="https://pulsar.apache.org/docs/en/concepts-messaging/#subscriptions">Pulsar
     *     Subscriptions</a>
     */
    public PulsarSourceBuilder<OUT> setSubscriptionType(SubscriptionType subscriptionType) {
        return setConfig(PULSAR_SUBSCRIPTION_TYPE, subscriptionType);
    }

    /**
     * Set a pulsar topic list for flink source. Some topic may not exist currently, consuming this
     * non-existed topic wouldn't throw any exception. But the best solution is just consuming by
     * using a topic regex. You can set topics once either with {@link #setTopics} or {@link
     * #setTopicPattern} in this builder.
     *
     * @param topics The topic list you would like to consume message.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setTopics(String... topics) {
        return setTopics(Arrays.asList(topics));
    }

    /**
     * Set a pulsar topic list for flink source. Some topic may not exist currently, consuming this
     * non-existed topic wouldn't throw any exception. But the best solution is just consuming by
     * using a topic regex. You can set topics once either with {@link #setTopics} or {@link
     * #setTopicPattern} in this builder.
     *
     * @param topics The topic list you would like to consume message.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setTopics(List<String> topics) {
        ensureSubscriberIsNull("topics");
        List<String> distinctTopics = TopicNameUtils.distinctTopics(topics);
        this.subscriber = PulsarSubscriber.getTopicListSubscriber(distinctTopics);
        return this;
    }

    /**
     * Set a topic pattern to consume from the java regex str. You can set topics once either with
     * {@link #setTopics} or {@link #setTopicPattern} in this builder.
     *
     * <p>Remember that we will only subscribe to one tenant and one namespace by using regular
     * expression. If you didn't provide the tenant and namespace in the given topic pattern. We
     * will use default one instead.
     *
     * @param topicsPattern the pattern of the topic name to consume from.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setTopicPattern(String topicsPattern) {
        return setTopicPattern(Pattern.compile(topicsPattern));
    }

    /**
     * Set a topic pattern to consume from the java {@link Pattern}. You can set topics once either
     * with {@link #setTopics} or {@link #setTopicPattern} in this builder.
     *
     * <p>Remember that we will only subscribe to one tenant and one namespace by using regular
     * expression. If you didn't provide the tenant and namespace in the given topic pattern. We
     * will use default one instead.
     *
     * @param topicsPattern the pattern of the topic name to consume from.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setTopicPattern(Pattern topicsPattern) {
        return setTopicPattern(topicsPattern, RegexSubscriptionMode.AllTopics);
    }

    /**
     * Set a topic pattern to consume from the java regex str. You can set topics once either with
     * {@link #setTopics} or {@link #setTopicPattern} in this builder.
     *
     * <p>Remember that we will only subscribe to one tenant and one namespace by using regular
     * expression. If you didn't provide the tenant and namespace in the given topic pattern. We
     * will use default one instead.
     *
     * @param topicsPattern the pattern of the topic name to consume from.
     * @param regexSubscriptionMode When subscribing to a topic using a regular expression, you can
     *     pick a certain type of topics.
     *     <ul>
     *       <li>PersistentOnly: only subscribe to persistent topics.
     *       <li>NonPersistentOnly: only subscribe to non-persistent topics.
     *       <li>AllTopics: subscribe to both persistent and non-persistent topics.
     *     </ul>
     *
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setTopicPattern(
            String topicsPattern, RegexSubscriptionMode regexSubscriptionMode) {
        return setTopicPattern(Pattern.compile(topicsPattern), regexSubscriptionMode);
    }

    /**
     * Set a topic pattern to consume from the java {@link Pattern}. You can set topics once either
     * with {@link #setTopics} or {@link #setTopicPattern} in this builder.
     *
     * <p>Remember that we will only subscribe to one tenant and one namespace by using regular
     * expression. If you didn't provide the tenant and namespace in the given topic pattern. We
     * will use default one instead.
     *
     * @param topicsPattern the pattern of the topic name to consume from.
     * @param regexSubscriptionMode When subscribing to a topic using a regular expression, you can
     *     pick a certain type of topics.
     *     <ul>
     *       <li>PersistentOnly: only subscribe to persistent topics.
     *       <li>NonPersistentOnly: only subscribe to non-persistent topics.
     *       <li>AllTopics: subscribe to both persistent and non-persistent topics.
     *     </ul>
     *
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setTopicPattern(
            Pattern topicsPattern, RegexSubscriptionMode regexSubscriptionMode) {
        ensureSubscriberIsNull("topic pattern");
        this.subscriber =
                PulsarSubscriber.getTopicPatternSubscriber(topicsPattern, regexSubscriptionMode);
        return this;
    }

    /**
     * The consumer name is informative, and it can be used to identify a particular consumer
     * instance from the topic stats.
     */
    public PulsarSourceBuilder<OUT> setConsumerName(String consumerName) {
        return setConfig(PULSAR_CONSUMER_NAME, consumerName);
    }

    /**
     * If you enable this option, we would consume and deserialize the message by using Pulsar
     * {@link Schema}.
     */
    public PulsarSourceBuilder<OUT> enableSchemaEvolution() {
        configBuilder.set(PULSAR_READ_SCHEMA_EVOLUTION, true);
        return this;
    }

    /**
     * Set a topic range generator for Key_Shared subscription.
     *
     * @param rangeGenerator A generator which would generate a set of {@link TopicRange} for given
     *     topic.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setRangeGenerator(RangeGenerator rangeGenerator) {
        if (configBuilder.contains(PULSAR_SUBSCRIPTION_TYPE)) {
            SubscriptionType subscriptionType = configBuilder.get(PULSAR_SUBSCRIPTION_TYPE);
            checkArgument(
                    subscriptionType == SubscriptionType.Key_Shared,
                    "Key_Shared subscription should be used for custom rangeGenerator instead of %s",
                    subscriptionType);
        } else {
            LOG.warn("No subscription type provided, set it to Key_Shared.");
            setSubscriptionType(SubscriptionType.Key_Shared);
        }
        this.rangeGenerator = checkNotNull(rangeGenerator);
        return this;
    }

    /**
     * Specify from which offsets the PulsarSource should start consume from by providing an {@link
     * StartCursor}.
     *
     * @param startCursor set the starting offsets for the Source.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setStartCursor(StartCursor startCursor) {
        this.startCursor = checkNotNull(startCursor);
        return this;
    }

    /**
     * By default, the PulsarSource runs in an {@link Boundedness#CONTINUOUS_UNBOUNDED} mode and
     * never stop until the Flink job is canceled or fails. To let the PulsarSource run in {@link
     * Boundedness#CONTINUOUS_UNBOUNDED} but stops at some given offsets, one can call {@link
     * #setUnboundedStopCursor(StopCursor)} and disable auto partition discovery as described below.
     *
     * <p>This method is different from {@link #setBoundedStopCursor(StopCursor)} that after setting
     * the stopping offsets with this method, {@link PulsarSource#getBoundedness()} will still
     * return {@link Boundedness#CONTINUOUS_UNBOUNDED} even though it will stop at the stopping
     * offsets specified by the stopping offsets {@link StopCursor}.
     *
     * <p>To stop the connector user has to disable the auto partition discovery. As auto partition
     * discovery always expected new splits to come and not exiting. To disable auto partition
     * discovery, use builder.setConfig({@link
     * PulsarSourceOptions#PULSAR_PARTITION_DISCOVERY_INTERVAL_MS}, -1).
     *
     * @param stopCursor The {@link StopCursor} to specify the stopping offset.
     * @return this PulsarSourceBuilder.
     * @see #setBoundedStopCursor(StopCursor)
     */
    public PulsarSourceBuilder<OUT> setUnboundedStopCursor(StopCursor stopCursor) {
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.stopCursor = checkNotNull(stopCursor);
        return this;
    }

    /**
     * By default, the PulsarSource is set to run in {@link Boundedness#CONTINUOUS_UNBOUNDED} manner
     * and thus never stops until the Flink job fails or is canceled. To let the PulsarSource run in
     * {@link Boundedness#BOUNDED} manner and stops at some point, one can set an {@link StopCursor}
     * to specify the stopping offsets for each partition. When all the partitions have reached
     * their stopping offsets, the PulsarSource will then exit.
     *
     * <p>This method is different from {@link #setUnboundedStopCursor(StopCursor)} that after
     * setting the stopping offsets with this method, {@link PulsarSource#getBoundedness()} will
     * return {@link Boundedness#BOUNDED} instead of {@link Boundedness#CONTINUOUS_UNBOUNDED}.
     *
     * @param stopCursor the {@link StopCursor} to specify the stopping offsets.
     * @return this PulsarSourceBuilder.
     * @see #setUnboundedStopCursor(StopCursor)
     */
    public PulsarSourceBuilder<OUT> setBoundedStopCursor(StopCursor stopCursor) {
        this.boundedness = Boundedness.BOUNDED;
        this.stopCursor = checkNotNull(stopCursor);
        return this;
    }

    /**
     * DeserializationSchema is required for getting the {@link Schema} for deserialize message from
     * pulsar and getting the {@link TypeInformation} for message serialization in flink.
     *
     * <p>We have defined a set of implementations, using {@code
     * PulsarDeserializationSchema#pulsarSchema} or {@code PulsarDeserializationSchema#flinkSchema}
     * for creating the desired schema.
     */
    public <T extends OUT> PulsarSourceBuilder<T> setDeserializationSchema(
            PulsarDeserializationSchema<T> deserializationSchema) {
        PulsarSourceBuilder<T> self = specialized();
        self.deserializationSchema = deserializationSchema;
        return self;
    }

    /**
     * Configure the authentication provider to use in the Pulsar client instance.
     *
     * @param authPluginClassName name of the Authentication-Plugin you want to use
     * @param authParamsString string which represents parameters for the Authentication-Plugin,
     *     e.g., "key1:val1,key2:val2"
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setAuthentication(
            String authPluginClassName, String authParamsString) {
        configBuilder.set(PULSAR_AUTH_PLUGIN_CLASS_NAME, authPluginClassName);
        configBuilder.set(PULSAR_AUTH_PARAMS, authParamsString);
        return this;
    }

    /**
     * Configure the authentication provider to use in the Pulsar client instance.
     *
     * @param authPluginClassName name of the Authentication-Plugin you want to use
     * @param authParams map which represents parameters for the Authentication-Plugin
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setAuthentication(
            String authPluginClassName, Map<String, String> authParams) {
        configBuilder.set(PULSAR_AUTH_PLUGIN_CLASS_NAME, authPluginClassName);
        configBuilder.set(PULSAR_AUTH_PARAM_MAP, authParams);
        return this;
    }

    /**
     * Sets a {@link CryptoKeyReader}. Configure the key reader to be used to decrypt the message
     * payloads.
     *
     * @param cryptoKeyReader CryptoKeyReader object
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setCryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        this.cryptoKeyReader = checkNotNull(cryptoKeyReader);
        return this;
    }

    /**
     * Set an arbitrary property for the PulsarSource and Pulsar Consumer. The valid keys can be
     * found in {@link PulsarSourceOptions} and {@link PulsarOptions}.
     *
     * <p>Make sure the option could be set only once or with same value.
     *
     * @param key the key of the property.
     * @param value the value of the property.
     * @return this PulsarSourceBuilder.
     */
    public <T> PulsarSourceBuilder<OUT> setConfig(ConfigOption<T> key, T value) {
        configBuilder.set(key, value);
        return this;
    }

    /**
     * Set arbitrary properties for the PulsarSource and Pulsar Consumer. The valid keys can be
     * found in {@link PulsarSourceOptions} and {@link PulsarOptions}.
     *
     * @param config the config to set for the PulsarSource.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setConfig(Configuration config) {
        configBuilder.set(config);
        return this;
    }

    /**
     * Set arbitrary properties for the PulsarSource and Pulsar Consumer. The valid keys can be
     * found in {@link PulsarSourceOptions} and {@link PulsarOptions}.
     *
     * <p>This method is mainly used for future flink SQL binding.
     *
     * @param properties the config properties to set for the PulsarSource.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<OUT> setProperties(Properties properties) {
        configBuilder.set(properties);
        return this;
    }

    /**
     * Build the {@link PulsarSource}.
     *
     * @return a PulsarSource with the settings made for this builder.
     */
    @SuppressWarnings("java:S3776")
    public PulsarSource<OUT> build() {

        // Ensure the topic subscriber for pulsar.
        checkNotNull(subscriber, "No topic names or topic pattern are provided.");

        SubscriptionType subscriptionType = configBuilder.get(PULSAR_SUBSCRIPTION_TYPE);
        if (subscriptionType == SubscriptionType.Key_Shared) {
            if (rangeGenerator == null) {
                LOG.warn(
                        "No range generator provided for key_shared subscription,"
                                + " we would use the SplitRangeGenerator as the default range generator.");
                this.rangeGenerator = new SplitRangeGenerator();
            }
        } else {
            // Override the range generator.
            this.rangeGenerator = new FullRangeGenerator();
        }

        if (boundedness == null) {
            LOG.warn("No boundedness was set, mark it as a endless stream.");
            this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        }
        if (boundedness == Boundedness.BOUNDED
                && configBuilder.get(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS) >= 0) {
            LOG.warn(
                    "{} property is overridden to -1 because the source is bounded.",
                    PULSAR_PARTITION_DISCOVERY_INTERVAL_MS);
            configBuilder.override(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, -1L);
        }

        checkNotNull(deserializationSchema, "deserializationSchema should be set.");

        // Enable transaction if the cursor auto commit is disabled for Key_Shared & Shared.
        if (FALSE.equals(configBuilder.get(PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE))
                && (subscriptionType == SubscriptionType.Key_Shared
                        || subscriptionType == SubscriptionType.Shared)) {
            LOG.info(
                    "Pulsar cursor auto commit is disabled, make sure checkpoint is enabled "
                            + "and your pulsar cluster is support the transaction.");
            configBuilder.override(PULSAR_ENABLE_TRANSACTION, true);

            if (!configBuilder.contains(PULSAR_READ_TRANSACTION_TIMEOUT)) {
                LOG.warn(
                        "The default pulsar transaction timeout is 3 hours, "
                                + "make sure it was greater than your checkpoint interval.");
            } else {
                Long timeout = configBuilder.get(PULSAR_READ_TRANSACTION_TIMEOUT);
                LOG.warn(
                        "The configured transaction timeout is {} mille seconds, "
                                + "make sure it was greater than your checkpoint interval.",
                        timeout);
            }
        }

        // Schema evolution validation.
        if (Boolean.TRUE.equals(configBuilder.get(PULSAR_READ_SCHEMA_EVOLUTION))) {
            checkState(
                    deserializationSchema instanceof PulsarSchemaWrapper,
                    "When enabling schema evolution, you must provide a Pulsar Schema in PulsarDeserializationSchema.");
        } else if (deserializationSchema instanceof PulsarSchemaWrapper) {
            LOG.info(
                    "It seems like you want to read message using Pulsar Schema."
                            + " You can enableSchemaEvolution for using this feature."
                            + " We would use Schema.BYTES as the default schema if you don't enable this option.");
        }

        if (!configBuilder.contains(PULSAR_CONSUMER_NAME)) {
            LOG.warn(
                    "We recommend set a readable consumer name through setConsumerName(String) in production mode.");
        } else {
            String consumerName = configBuilder.get(PULSAR_CONSUMER_NAME);
            if (!consumerName.contains("%s")) {
                configBuilder.override(PULSAR_CONSUMER_NAME, consumerName + " - %s");
            }
        }

        // Since these implementations could be a lambda, make sure they are serializable.
        checkState(isSerializable(startCursor), "StartCursor isn't serializable");
        checkState(isSerializable(stopCursor), "StopCursor isn't serializable");
        checkState(isSerializable(rangeGenerator), "RangeGenerator isn't serializable");

        // Check builder configuration.
        SourceConfiguration sourceConfiguration =
                configBuilder.build(SOURCE_CONFIG_VALIDATOR, SourceConfiguration::new);

        return new PulsarSource<>(
                sourceConfiguration,
                subscriber,
                rangeGenerator,
                startCursor,
                stopCursor,
                boundedness,
                deserializationSchema,
                cryptoKeyReader);
    }

    // ------------- private helpers  --------------

    /** Helper method for java compiler recognizes the generic type. */
    @SuppressWarnings("unchecked")
    private <T extends OUT> PulsarSourceBuilder<T> specialized() {
        return (PulsarSourceBuilder<T>) this;
    }

    /** Topic name and topic pattern is conflict, make sure they are set only once. */
    private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
        if (subscriber != null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot use %s for consumption because a %s is already set for consumption.",
                            attemptingSubscribeMode, subscriber.getClass().getSimpleName()));
        }
    }
}
