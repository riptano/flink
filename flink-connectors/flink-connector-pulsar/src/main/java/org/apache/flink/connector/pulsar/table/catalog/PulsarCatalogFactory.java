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

package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigBuilder;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigValidator;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactoryOptions.AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactoryOptions.AUTH_PLUGIN;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactoryOptions.CATALOG_ADMIN_URL;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactoryOptions.CATALOG_SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactoryOptions.CATALOG_TENANT;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogFactoryOptions.DEFAULT_DATABASE;

/** PulsarCatalogFactory implementing {@link CatalogFactory}. */
public class PulsarCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "pulsar-catalog";

    public static final PulsarConfigValidator CATALOG_CONFIG_VALIDATOR =
            PulsarConfigValidator.builder().build();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);

        helper.validate();

        PulsarConfigBuilder configBuilder = new PulsarConfigBuilder();
        ReadableConfig tableOptions = helper.getOptions();

        configBuilder.set(PulsarOptions.PULSAR_ADMIN_URL, tableOptions.get(CATALOG_ADMIN_URL));
        configBuilder.set(PulsarOptions.PULSAR_SERVICE_URL, tableOptions.get(CATALOG_SERVICE_URL));
        if (tableOptions.getOptional(AUTH_PLUGIN).isPresent()) {
            configBuilder.set(
                    PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME, tableOptions.get(AUTH_PLUGIN));
        }

        if (tableOptions.getOptional(AUTH_PARAMS).isPresent()) {
            configBuilder.set(PulsarOptions.PULSAR_AUTH_PARAMS, tableOptions.get(AUTH_PARAMS));
        }

        PulsarCatalogConfiguration catalogConfiguration =
                configBuilder.build(CATALOG_CONFIG_VALIDATOR, PulsarCatalogConfiguration::new);

        return new PulsarCatalog(
                context.getName(),
                catalogConfiguration,
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(CATALOG_TENANT));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // pulsar catalog options
        return Stream.of(
                        CATALOG_ADMIN_URL,
                        CATALOG_SERVICE_URL,
                        CATALOG_TENANT,
                        DEFAULT_DATABASE,
                        AUTH_PLUGIN,
                        AUTH_PARAMS)
                .collect(Collectors.toSet());
    }
}
