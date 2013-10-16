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

import org.apache.activemq.broker.TransportConnector;
import org.apache.curator.framework.CuratorFramework;
import org.fusesource.fabric.api.FabricException;
import org.fusesource.fabric.groups.Group;
import org.fusesource.fabric.groups.GroupListener;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusteredBroker extends BasicBroker implements GroupListener<FabricDiscoveryAgent.ActiveMQNode> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusteredBroker.class);
    private static final String KARAF_NAME = System.getProperty("karaf.name");

    private final CuratorFramework curator;
    private final PoolManager poolManager;

    private final FabricDiscoveryAgent discoveryAgent = new FabricDiscoveryAgent();
    private final AtomicBoolean poolEnabled = new AtomicBoolean();

    public ClusteredBroker(CuratorFramework curator, PoolManager poolManager, BundleContext bundleContext, Map<String, Object> config) throws Exception {
        super(bundleContext, config);
        this.curator = curator;
        this.poolManager = poolManager;
        configureDiscoveryAgent();
    }

    @Override
    public void init() {
        if (brokerConfiguration.isReplicating()) {
            try {
                discoveryAgent.start();
            } catch (Exception e) {
                LOGGER.warn("Error starting mq fabric discovery agent.", e);
            }
            start();
        } else {
            discoveryAgent.getGroup().add(this);
            updatePoolState();
        }
    }

    @Override
    public void close() {
        try {
            if (poolEnabled.get() || (brokerConfiguration.isReplicating() && discoveryAgent != null)) {
                discoveryAgent.stop();
            }
            if (poolEnabled.get()) {
                poolManager.returnToPool(this);
            }
            if (started.compareAndSet(true, false)) {
                tryStop();
            }
        } catch (Exception e) {
            FabricException.launderThrowable(e);
        }
    }

    @Override
    synchronized boolean tryStart() {
        boolean success = super.tryStart();
        publishServices();
        return success;
    }

    @Override
    public void groupEvent(Group<FabricDiscoveryAgent.ActiveMQNode> group, GroupEvent event) {
        String name = brokerConfiguration.getName();
        switch (event) {
            case CONNECTED:
            case CHANGED:
                if (discoveryAgent.getGroup().isMaster(brokerConfiguration.getName())) {
                    if (!started.get()) {
                        LOGGER.info("Broker {} is now the master, starting the broker.", name);
                        start();
                    } else {
                        try {
                            updatePoolState();
                            started.set(false);
                        } catch (Exception e) {
                            FabricException.launderThrowable(e);
                        }
                    }
                } else if (started.get()) {
                    LOGGER.info("Broker {} is now a slave, stopping the broker.", name);
                    removeServices();
                    poolManager.returnToPool(ClusteredBroker.this);
                    stop();
                } else {
                    LOGGER.info("Broker {} is now a slave.", name);
                    removeServices();
                }
                break;
            case DISCONNECTED:
                removeServices();
        }
    }


    void updatePoolState()  {
        try {
            String pool = brokerConfiguration.getPool();
            boolean canAcquire = poolManager.canAcquire(this);

            if (poolEnabled.get() != canAcquire) {
                poolEnabled.set(canAcquire);

                if (poolEnabled.get()) {
                    if (pool != null) {
                        LOGGER.info("Broker {} added to pool {}.", name, pool);
                    }
                    discoveryAgent.start();
                } else {
                    if (pool != null) {
                        LOGGER.info("Broker {} removed to pool {}.", name, pool);
                    }
                    discoveryAgent.stop();
                }
            }
        } catch (Exception e) {
            FabricException.launderThrowable(e);
        }
    }

    private void configureDiscoveryAgent() {
        discoveryAgent.setAgent(KARAF_NAME);
        discoveryAgent.setId(brokerConfiguration.getName());
        discoveryAgent.setGroupName(brokerConfiguration.getGroup());
        discoveryAgent.setCurator(curator);
    }

    private void publishServices() {
        List<String> services = new ArrayList<String>();

        for (String connector : brokerConfiguration.getConnectors()) {
            TransportConnector tc = brokerInstance.getBrokerService().getConnectorByName(connector);
            if (tc != null) {
                try {
                    services.add(tc.getConnectUri().getScheme() + "://${zk:" + KARAF_NAME + "/ip}:" + tc.getPublishableConnectURI().getPort());
                } catch (Exception e) {
                    LOGGER.warn("Error while adding advertising connector. Ignoring");
                }
            }
        }
        discoveryAgent.setServices(services.toArray(new String[services.size()]));
    }

    private void removeServices() {
        discoveryAgent.setServices(new String[0]);
    }
}
