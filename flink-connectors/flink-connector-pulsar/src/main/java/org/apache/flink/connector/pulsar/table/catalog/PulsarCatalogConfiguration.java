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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.PulsarConfiguration;

/** The configuration class for {@link PulsarCatalog}. */
public class PulsarCatalogConfiguration extends PulsarConfiguration {
    private static final long serialVersionUID = 3139935676757015589L;

    /**
     * Creates a new PulsarConfiguration, which holds a copy of the given configuration that can't
     * be altered. PulsarCatalogConfiguration does not have extra configs besides {@link
     * PulsarConfiguration}
     *
     * @param config The configuration with the original contents.
     */
    public PulsarCatalogConfiguration(Configuration config) {
        super(config);
    }
}
