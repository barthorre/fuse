/**
 * Copyright (C) FuseSource, Inc.
 * http://fusesource.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.mq.fabric;

import java.util.Map;

public class BrokerConfig {

    public static final String BROKER_NAME = "broker-name";
    public static final String DATA = "data";
    public static final String CONFIG = "config";
    public static final String GROUP = "group";
    public static final String POOL = "standby.pool";
    public static final String CONNECTORS = "connectors";
    public static final String REPLICATING = "replicating";
    public static final String STANDALONE = "standalone";
    public static final String REGISTER_SERVICE = "registerService";
    public static final String CONFIG_CHECK = "config.check";

    private static final String CONNECTORS_SPLIT_REGEX = "\\s";

    public static final class Builder {

        private String name = System.getProperty("karaf.name");
        private String data =  "data" + System.getProperty("file.separator") + name;
        private String config;
        private String group = "default";
        private String pool = "default";
        private String connectors = "";
        private boolean replicating = false;
        private boolean standalone = false;
        private boolean serviceRegistrationEnabled = true;
        private boolean configCheckEnabled = true;

        public Builder fromConfiguration(Map<String, ?> properties) {
            name = getStringProperty(properties, BROKER_NAME, name);
            data = getStringProperty(properties, DATA, data);
            config = getStringProperty(properties, CONFIG, config);
            group = getStringProperty(properties, GROUP, group);
            pool = getStringProperty(properties, POOL, pool);
            connectors = getStringProperty(properties, CONNECTORS, connectors);
            replicating = getBooleanProperty(properties, REPLICATING, replicating);
            standalone = getBooleanProperty(properties, STANDALONE, standalone);
            serviceRegistrationEnabled = getBooleanProperty(properties, REGISTER_SERVICE, serviceRegistrationEnabled);
            configCheckEnabled = getBooleanProperty(properties, CONFIG_CHECK, configCheckEnabled);
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder data(final String data) {
            this.data = data;
            return this;
        }

        public Builder group(final String group) {
            this.group = group;
            return this;
        }

        public Builder pool(final String pool) {
            this.pool = pool;
            return this;
        }

        public Builder connectors(final String connectors) {
            this.connectors = connectors;
            return this;
        }

        public Builder replicating(final boolean replicating) {
            this.replicating = replicating;
            return this;
        }

        public Builder standalone(final boolean standalone) {
            this.standalone = standalone;
            return this;
        }

        public Builder serviceRegistrationEnabled(final boolean serviceRegistrationEnabled) {
            this.serviceRegistrationEnabled = serviceRegistrationEnabled;
            return this;
        }

        public Builder configCheckEnabled(final boolean configCheckEnabled) {
            this.configCheckEnabled = configCheckEnabled;
            return this;
        }

        private static String getStringProperty(Map<String, ?> properties, String name, String defaultValue) {
            if (properties.containsKey(name)) {
                return String.valueOf(properties.get(name));
            } else {
                return defaultValue;
            }
        }

        private static boolean getBooleanProperty(Map<String, ?> properties, String name, Boolean defaultValue) {
            if (properties.containsKey(name)) {
                return Boolean.parseBoolean(String.valueOf(properties.get(name)));
            } else {
                return defaultValue;
            }
        }

        public BrokerConfig build() {
            return new BrokerConfig(name, data, group, pool, connectors.split(CONNECTORS_SPLIT_REGEX), replicating, standalone, serviceRegistrationEnabled, configCheckEnabled);
        }
    }

    private final String name;
    private final String data;
    private final String group;
    private final String pool;
    private final String[] connectors;
    private final boolean replicating;
    private final boolean standalone;
    private final boolean serviceRegistrationEnabled;
    private final boolean configCheckEnabled;


    public BrokerConfig(String name, String data, String group, String pool, String[] connectors, boolean replicating, boolean standalone, boolean serviceRegistrationEnabled, boolean configCheckEnabled) {
        this.name = name;
        this.data = data;
        this.group = group;
        this.pool = pool;
        this.connectors = connectors;
        this.replicating = replicating;
        this.standalone = standalone;
        this.serviceRegistrationEnabled = serviceRegistrationEnabled;
        this.configCheckEnabled = configCheckEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }
    public String getName() {
        return name;
    }

    public String getData() {
        return data;
    }

    public String getGroup() {
        return group;
    }

    public String getPool() {
        return pool;
    }

    public String[] getConnectors() {
        return connectors;
    }

    public boolean isReplicating() {
        return replicating;
    }

    public boolean isStandalone() {
        return standalone;
    }

    public boolean isServiceRegistrationEnabled() {
        return serviceRegistrationEnabled;
    }

    public boolean isConfigCheckEnabled() {
        return configCheckEnabled;
    }
}
