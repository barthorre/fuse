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

import org.apache.curator.framework.CuratorFramework;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.fusesource.fabric.api.Container;
import org.fusesource.fabric.api.FabricService;
import org.fusesource.fabric.api.scr.ValidatingReference;
import org.osgi.framework.BundleContext;

import java.util.HashMap;
import java.util.Map;

@Component(name = MQServiceFactory.ID, configurationPid = MQServiceFactory.ID,
        configurationFactory = true,
        policy = ConfigurationPolicy.REQUIRE)
public class MQServiceFactory {

    public static final String ID = "org.fusesource.mq.fabric.server";

    private final String DEFAULT_NAME = System.getProperty("karaf.name");
    private final String DEFAULT_DATA =  "data" + System.getProperty("file.separator") + DEFAULT_NAME;

    //Use dynamic policy to prevent unneeded re-activations.
    // We are guarded by the FabricService anyways => we only use curator when FabricService is available.
    @Reference(referenceInterface = CuratorFramework.class, cardinality = ReferenceCardinality.OPTIONAL_UNARY, policy = ReferencePolicy.DYNAMIC)
    private final ValidatingReference<CuratorFramework> curator = new ValidatingReference<CuratorFramework>();

    @Reference(referenceInterface = FabricService.class, cardinality = ReferenceCardinality.OPTIONAL_UNARY)
    private final ValidatingReference<FabricService> fabricService = new ValidatingReference<FabricService>();

    @Reference(referenceInterface = PoolManager.class)
    private final ValidatingReference<PoolManager> poolManager = new ValidatingReference<PoolManager>();

    private ManagedBroker broker;

   @Activate
   synchronized void activate(BundleContext bundleContext, Map<String, Object> configuration) throws Exception {
       create(bundleContext, configuration);
   }

    @Modified
    synchronized void modified(BundleContext bundleContext, Map<String, Object> configuration) throws Exception {
        if (broker != null) {
            broker.close();
        }
        create(bundleContext, configuration);
    }

    @Deactivate
    synchronized void deactivate() {
        if (broker != null) {
            broker.close();
        }
    }

    synchronized void create(BundleContext bundleContext, Map<String, Object> input) throws Exception {
        Map<String, Object> configuration = new HashMap<String, Object>();
        configuration.putAll(input);
        populateRequiredAttributes(configuration, DEFAULT_NAME, DEFAULT_DATA);

        if (isClusterAvailable()) {
            populateFabricAttributes(fabricService.get(), configuration);
            broker = new ClusteredBroker(curator.get(), poolManager.get(), bundleContext, configuration);
        } else {
            broker = new BasicBroker(bundleContext, configuration);
        }
        broker.init();
    }

    private boolean isClusterAvailable() {
        return fabricService.getOptional() != null && curator.getOptional() != null;
    }

    private void populateRequiredAttributes(Map<String, Object> config, String name, String data) {
        if (!config.containsKey("broker-name")) {
            config.put("broker-name", name);
        }
        if (!config.containsKey("data")) {
            config.put("data", data);
        }
    }

    private static void populateFabricAttributes(FabricService fabricService, Map<String, Object> config) {
        Container container = fabricService.getCurrentContainer();
        if (!config.containsKey("container.id")) {
            config.put("container.id", container.getId());
        }
        if (!config.containsKey("container.ip")) {
            config.put("container.ip", container.getId());
        }
        if (!config.containsKey("zookeeper.url")) {
            config.put("zookeeper.url", fabricService.getZookeeperUrl());
        }
        if (!config.containsKey("zookeeper.password")) {
            config.put("zookeeper.password", fabricService.getZookeeperPassword());
        }
    }


    public void bindCurator(CuratorFramework curatorFramework) {
        this.curator.bind(curatorFramework);
    }

    public void unbindCurator(CuratorFramework curatorFramework) {
        this.curator.unbind(curatorFramework);
    }

    public void bindFabricService(FabricService fabricService) {
        this.fabricService.bind(fabricService);
    }

    public void unbindFabricService(FabricService fabricService) {
        this.fabricService.unbind(fabricService);
    }

    public void bindPoolManager(PoolManager poolManager) {
        this.poolManager.bind(poolManager);
    }

    public void unbindPoolManager(PoolManager poolManager) {
        this.poolManager.unbind(poolManager);
    }
}
