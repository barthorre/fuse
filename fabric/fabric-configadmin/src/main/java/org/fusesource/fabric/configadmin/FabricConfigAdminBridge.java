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
package org.fusesource.fabric.configadmin;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.fusesource.fabric.api.ContainerRegistration;
import org.fusesource.fabric.api.FabricService;
import org.fusesource.fabric.api.Profile;
import org.fusesource.fabric.api.jcip.ThreadSafe;
import org.fusesource.fabric.api.scr.AbstractComponent;
import org.fusesource.fabric.api.scr.ValidatingReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
@Component(name = "org.fusesource.fabric.configadmin.bridge", description = "Fabric Config Admin Bridge")
public final class FabricConfigAdminBridge extends AbstractComponent implements Runnable {

    public static final String FABRIC_ZOOKEEPER_PID = "fabric.zookeeper.pid";
    public static final String AGENT_PID = "org.fusesource.fabric.agent";
    public static final String LAST_MODIFIED = "lastModified";

    private static final Logger LOGGER = LoggerFactory.getLogger(FabricConfigAdminBridge.class);

    @Reference(referenceInterface = ConfigurationAdmin.class)
    private final ValidatingReference<ConfigurationAdmin> configAdmin = new ValidatingReference<ConfigurationAdmin>();
    @Reference(referenceInterface = FabricService.class)
    private final ValidatingReference<FabricService> fabricService = new ValidatingReference<FabricService>();
    @Reference(referenceInterface = ContainerRegistration.class)
    private final ValidatingReference<ContainerRegistration> registration = new ValidatingReference<ContainerRegistration>();

    private final ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("fabric-configadmin"));

    @Activate
    synchronized void activate(ComponentContext context) {
        fabricService.get().trackConfiguration(this);
        activateComponent();
        submitUpdateJob();
    }

    @Deactivate
    synchronized void deactivate() {
        deactivateComponent();
        fabricService.get().unTrackConfiguration(this);
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            // Ignore
        }
        executor.shutdownNow();
    }

    @Override
    public void run() {
        if (isValid()) {
            submitUpdateJob();
        }
    }

    private void submitUpdateJob() {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                update();
            }
        });
    }

    private synchronized void update() {
        if (isValid()) {
            try {
                Profile profile = fabricService.get().getCurrentContainer().getOverlayProfile();
                final Map<String, Map<String, String>> pidProperties = profile.getConfigurations();
                List<Configuration> configs = asList(configAdmin.get().listConfigurations("(" + FABRIC_ZOOKEEPER_PID + "=*)"));
                for (String pid : pidProperties.keySet()) {
                    Hashtable<String, String> c = new Hashtable<String, String>();
                    c.putAll(pidProperties.get(pid));
                    String p[] = parsePid(pid);
                    //Get the configuration by fabric zookeeper pid, pid and factory pid.
                    Configuration config = getConfiguration(configAdmin.get(), pid, p[0], p[1]);
                    configs.remove(config);
                    Dictionary props = config.getProperties();
                    Hashtable old = props != null ? new Hashtable() : null;
                    if (pid.equals(AGENT_PID)) {
                        c.put(LAST_MODIFIED, String.valueOf(profile.getLastModified()));
                    }
                    if (old != null) {
                        for (Enumeration e = props.keys(); e.hasMoreElements(); ) {
                            Object key = e.nextElement();
                            Object val = props.get(key);
                            old.put(key, val);
                        }
                        old.remove(FABRIC_ZOOKEEPER_PID);
                        old.remove(org.osgi.framework.Constants.SERVICE_PID);
                        old.remove(ConfigurationAdmin.SERVICE_FACTORYPID);
                    }
                    if (!c.equals(old)) {
                        LOGGER.info("Updating configuration {}", config.getPid());
                        c.put(FABRIC_ZOOKEEPER_PID, pid);
                        if (config.getBundleLocation() != null) {
                            config.setBundleLocation(null);
                        }
                        config.update(c);
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Ignoring configuration {} (no changes)", config.getPid());
                        }
                    }
                }
                for (Configuration config : configs) {
                    LOGGER.info("Deleting configuration {}", config.getPid());
                    fabricService.get().getPortService().unregisterPort(fabricService.get().getCurrentContainer(), config.getPid());
                    config.delete();
                }
            } catch (Throwable e) {
                if (isValid()) {
                    LOGGER.warn("Exception when tracking configurations. This exception will be ignored.", e);
                } else {
                    LOGGER.debug("Exception when tracking configurations. This exception will be ignored because services have been unbound in the mean time.", e);
                }
            }
        }
    }

    private <T> List<T> asList(T... a) {
        List<T> l = new ArrayList<T>();
        if (a != null) {
            Collections.addAll(l, a);
        }
        return l;
    }

    /**
     * Splits a pid into service and factory pid.
     *
     * @param pid The pid to parse.
     * @return An arrays which contains the pid[0] the pid and pid[1] the factory pid if applicable.
     */
    private String[] parsePid(String pid) {
        int n = pid.indexOf('-');
        if (n > 0) {
            String factoryPid = pid.substring(n + 1);
            pid = pid.substring(0, n);
            return new String[]{pid, factoryPid};
        } else {
            return new String[]{pid, null};
        }
    }

    private Configuration getConfiguration(ConfigurationAdmin configAdmin, String zooKeeperPid, String pid, String factoryPid) throws Exception {
        String filter = "(" + FABRIC_ZOOKEEPER_PID + "=" + zooKeeperPid + ")";
        Configuration[] oldConfiguration = configAdmin.listConfigurations(filter);
        if (oldConfiguration != null && oldConfiguration.length > 0) {
            return oldConfiguration[0];
        } else {
            Configuration newConfiguration;
            if (factoryPid != null) {
                newConfiguration = configAdmin.createFactoryConfiguration(pid, null);
            } else {
                newConfiguration = configAdmin.getConfiguration(pid, null);
            }
            return newConfiguration;
        }
    }

    void bindConfigAdmin(ConfigurationAdmin service) {
        this.configAdmin.bind(service);
    }

    void unbindConfigAdmin(ConfigurationAdmin service) {
        this.configAdmin.unbind(service);
    }

    void bindRegistration(ContainerRegistration service) {
        this.registration.bind(service);
    }

    void unbindRegistration(ContainerRegistration service) {
        this.registration.unbind(service);
    }

    void bindFabricService(FabricService fabricService) {
        this.fabricService.bind(fabricService);
    }

    void unbindFabricService(FabricService fabricService) {
        this.fabricService.unbind(fabricService);
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NamedThreadFactory(String prefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = prefix + "-" +
                    poolNumber.getAndIncrement() +
                    "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }

    }

}
