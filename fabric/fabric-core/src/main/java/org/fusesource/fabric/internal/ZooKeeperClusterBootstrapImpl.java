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
package org.fusesource.fabric.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.felix.scr.annotations.*;
import org.apache.zookeeper.KeeperException;
import org.fusesource.fabric.api.*;
import org.fusesource.fabric.api.jcip.GuardedBy;
import org.fusesource.fabric.api.jcip.ThreadSafe;
import org.fusesource.fabric.api.scr.AbstractComponent;
import org.fusesource.fabric.api.scr.ValidatingReference;
import org.fusesource.fabric.utils.BundleUtils;
import org.fusesource.fabric.utils.HostUtils;
import org.fusesource.fabric.utils.OsgiUtils;
import org.fusesource.fabric.utils.Ports;
import org.fusesource.fabric.zookeeper.ZkDefs;
import org.fusesource.fabric.zookeeper.utils.ZooKeeperUtils;
import org.osgi.framework.*;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
@Component(name = "org.fusesource.fabric.zookeeper.cluster.bootstrap", description = "Fabric ZooKeeper Cluster Bootstrap", immediate = true)
@Service(ZooKeeperClusterBootstrap.class)
public final class ZooKeeperClusterBootstrapImpl extends AbstractComponent implements ZooKeeperClusterBootstrap {

    private static final Long FABRIC_SERVICE_TIMEOUT = 60000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperClusterBootstrapImpl.class);

    @Reference(referenceInterface = ConfigurationAdmin.class)
    private final ValidatingReference<ConfigurationAdmin> configAdmin = new ValidatingReference<ConfigurationAdmin>();
    @Reference(referenceInterface = DataStoreRegistrationHandler.class)
    private final ValidatingReference<DataStoreRegistrationHandler> registrationHandler = new ValidatingReference<DataStoreRegistrationHandler>();

    @GuardedBy("this") private Map<String, String> configuration;
    @GuardedBy("this") private BundleUtils bundleUtils;
    @GuardedBy("this") private BundleContext bundleContext;

    @Activate
    synchronized void activate(BundleContext bundleContext, Map<String,String> configuration) {
        this.bundleContext = bundleContext;
        this.configuration = Collections.unmodifiableMap(new HashMap<String,String>(configuration));
        this.bundleUtils = new BundleUtils(bundleContext);
        new Thread(new Runnable() {
            @Override
            public void run() {
                createOnActivate();
                activateComponent();
            }
        }).start();
    }

    @Deactivate
    synchronized void deactivate() {
        deactivateComponent();
    }

    private void createOnActivate() {
        org.apache.felix.utils.properties.Properties userProps = null;
        try {
            userProps = new org.apache.felix.utils.properties.Properties(new File(System.getProperty("karaf.home") + "/etc/users.properties"));
        } catch (IOException e) {
            LOGGER.warn("Failed to load users from etc/users.properties. No users will be imported.", e);
        }
        CreateEnsembleOptions createOpts = CreateEnsembleOptions.builder().fromSystemProperties().users(userProps).build();
        if (createOpts.isEnsembleStart()) {
            createInternal(createOpts);
        }
    }

    @Override
    public void create(CreateEnsembleOptions options) {
        assertValid();
        createInternal(options);
	}

    private void createInternal(CreateEnsembleOptions options) {
        try {
            int minimumPort = options.getMinimumPort();
            int maximumPort = options.getMaximumPort();
            String zooKeeperServerHost = options.getBindAddress();
            int zooKeeperServerPort = options.getZooKeeperServerPort();
            int zooKeeperServerConnectionPort = options.getZooKeeperServerConnectionPort();
            int mappedPort = Ports.mapPortToRange(zooKeeperServerPort, minimumPort, maximumPort);
			String connectionUrl = getConnectionAddress() + ":" + zooKeeperServerConnectionPort;

            // Create configuration
            updateDataStoreConfig(options.getDataStoreProperties());
            createZooKeeeperServerConfig(zooKeeperServerHost, mappedPort, options);
            registrationHandler.get().addRegistrationCallback(new DataStoreBootstrapTemplate(connectionUrl, configuration, options));

            // Create the client configuration
            createZooKeeeperConfig(connectionUrl, options);
            // Reset the autostart flag
            if (options.isEnsembleStart()) {
                System.setProperty(CreateEnsembleOptions.ENSEMBLE_AUTOSTART, Boolean.FALSE.toString());
                File file = new File(System.getProperty("karaf.base") + "/etc/system.properties");
                org.apache.felix.utils.properties.Properties props = new org.apache.felix.utils.properties.Properties(file);
                props.put(CreateEnsembleOptions.ENSEMBLE_AUTOSTART, Boolean.FALSE.toString());
                props.save();
            }
            startBundles(options);
            //Wait until Fabric Service becomes available.
            OsgiUtils.waitForSerice(FabricService.class, null, FABRIC_SERVICE_TIMEOUT );
            waitForSuccessfulDeploymentOf(System.getProperty("karaf.name"), "fabric-ensemble-0000-1");

		} catch (Exception e) {
			throw new FabricException("Unable to create zookeeper server configuration", e);
		}
    }

    private void waitForSuccessfulDeploymentOf(String containerName, String profileName) throws InterruptedException {
        while(true) {
            ServiceTracker tracker = null;
            try {
                Filter osgiFilter = FrameworkUtil.createFilter("(" + org.osgi.framework.Constants.OBJECTCLASS + "=" + FabricService.class.getName() + ")");
                tracker = new ServiceTracker(bundleContext, osgiFilter, null);
                tracker.open(true);
                tracker.waitForService(1000);
                FabricService fs = (FabricService) tracker.getService();
                if( fs!=null ) {
                    Container container = fs.getContainer(containerName);

                    if( container.isAlive() &&
                            contains(container.getProfiles(), profileName) &&
                            "success".equals(container.getProvisionStatus()) ) {
                        return;
                    }

                    System.out.println(String.format("Waiting for container %s to deploy profile %s", containerName, profileName));
                    Thread.sleep(1000);
                } else {
                    System.out.println(String.format("Waiting for fabric service to come online"));
                }
            } catch (Exception e) {
                System.out.println(String.format("Waiting for fabric service to come online"));
            } finally {
                tracker.close();
            }
        }
    }

    private boolean contains(Profile[] profiles, String profileName) {
        if( profiles==null )
            return false;
        for (Profile profile : profiles) {
            if( profileName.equals(profile.getId()) ) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void clean() {
        assertValid();
        try {
            Bundle bundleFabricZooKeeper = bundleUtils.findAndStopBundle("org.fusesource.fabric.fabric-zookeeper");

            for (; ; ) {
                Configuration[] configs = configAdmin.get().listConfigurations("(|(service.factoryPid=org.fusesource.fabric.zookeeper.server)(service.pid=org.fusesource.fabric.zookeeper))");
                if (configs != null && configs.length > 0) {
                    for (Configuration config : configs) {
                        config.delete();
                    }
                    Thread.sleep(100);
                } else {
                    break;
                }
            }

            File zkDir = new File("data/zookeeper");
            if (zkDir.isDirectory()) {
                File newZkDir = new File("data/zookeeper." + System.currentTimeMillis());
                if (!zkDir.renameTo(newZkDir)) {
                    newZkDir = zkDir;
                }
                delete(newZkDir);
            }

            bundleFabricZooKeeper.start();
        } catch (Exception e) {
            throw new FabricException("Unable to delete zookeeper configuration", e);
        }
    }


    private void updateDataStoreConfig(Map<String, String> dataStoreConfiguration) throws IOException {
        boolean updated = false;
        Configuration config = configAdmin.get().getConfiguration(DataStore.DATASTORE_TYPE_PID);
        Dictionary<String, Object> properties = config.getProperties();
        for (Map.Entry<String, String> entry : dataStoreConfiguration.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!value.equals(properties.put(key, value))) {
                updated = true;
            }
        }
        if (updated) {
            //We unbind, so that we know exactly when we get the re-configured instance.
            //dataStoreRegistrationHandler.unbind();
            config.setBundleLocation(null);
            config.update(properties);
        }
    }

    /**
     * Creates ZooKeeper server configuration
     */
    private void createZooKeeeperServerConfig(String serverHost, int serverPort, CreateEnsembleOptions options) throws IOException {
        Configuration config = configAdmin.get().createFactoryConfiguration("org.fusesource.fabric.zookeeper.server");
        Hashtable properties = new Hashtable<String, Object>();
        if (options.isAutoImportEnabled()) {
            loadPropertiesFrom(properties, options.getImportPath() + "/fabric/configs/versions/1.0/profiles/default/org.fusesource.fabric.zookeeper.server.properties");
        }
        properties.put("tickTime", "2000");
        properties.put("initLimit", "10");
        properties.put("syncLimit", "5");
        properties.put("dataDir", "data/zookeeper/0000");
        properties.put("clientPort", Integer.toString(serverPort));
        properties.put("clientPortAddress", serverHost);
        properties.put("fabric.zookeeper.pid", "org.fusesource.fabric.zookeeper.server-0000");
        config.setBundleLocation(null);
        config.update(properties);
    }

    /**
     * Creates ZooKeeper client configuration.
     * @param connectionUrl
     * @param options
     * @throws IOException
     */
    private void createZooKeeeperConfig(String connectionUrl, CreateEnsembleOptions options) throws IOException {
        Configuration config = configAdmin.get().getConfiguration("org.fusesource.fabric.zookeeper");
        Hashtable properties = new Hashtable<String, Object>();
        if (options.isAutoImportEnabled()) {
            loadPropertiesFrom(properties, options.getImportPath() + "/fabric/configs/versions/1.0/profiles/default/org.fusesource.fabric.zookeeper.properties");
        }
        properties.put("zookeeper.url", connectionUrl);
        properties.put("zookeeper.timeout", System.getProperties().containsKey("zookeeper.timeout") ? System.getProperties().getProperty("zookeeper.timeout") : "30000");
        properties.put("fabric.zookeeper.pid", "org.fusesource.fabric.zookeeper");
        properties.put("zookeeper.password", options.getZookeeperPassword());
        config.setBundleLocation(null);
        config.update(properties);
    }


    private void startBundles(CreateEnsembleOptions options) throws BundleException {

        // Install the required bundles
        Bundle bundleFabricAgent = bundleUtils.findAndStopBundle("org.fusesource.fabric.fabric-agent");
        //Bundle bundleFabricConfigAdmin = bundleUtils.installBundle("org.fusesource.fabric.fabric-configadmin", FABRIC_CONFIGADMIN_URL);
        //Bundle bundleFabricCommands = bundleUtils.installBundle("org.fusesource.fabric.fabric-commands", FABRIC_COMMANDS_URL);

        //bundleFabricCommands.start();
        //bundleFabricConfigAdmin.start();

        //Check if the agent is configured to auto start.
        if (options.isAgentEnabled()) {
            bundleFabricAgent.start();
        }
    }


    private void loadPropertiesFrom(Hashtable hashtable, String from) {
        InputStream is = null;
        Properties properties = new Properties();
        try {
            is = new FileInputStream(from);
            properties.load(is);
            for (String key : properties.stringPropertyNames()) {
                hashtable.put(key, properties.get(key));
            }
        } catch (Exception e) {
            // Ignore
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }


    private static void delete(File dir) {
        if (dir.isDirectory()) {
            for (File child : dir.listFiles()) {
                delete(child);
            }
        }
        if (dir.exists()) {
            dir.delete();
        }
    }

    private static String getConnectionAddress() throws UnknownHostException {
        String resolver = System.getProperty(ZkDefs.LOCAL_RESOLVER_PROPERTY, System.getProperty(ZkDefs.GLOBAL_RESOLVER_PROPERTY, ZkDefs.LOCAL_HOSTNAME));
        if (resolver.equals(ZkDefs.LOCAL_HOSTNAME)) {
            return HostUtils.getLocalHostName();
        } else if (resolver.equals(ZkDefs.LOCAL_IP)) {
            return HostUtils.getLocalIp();
        } else if (resolver.equals(ZkDefs.MANUAL_IP) && System.getProperty(ZkDefs.MANUAL_IP) != null) {
            return System.getProperty(ZkDefs.MANUAL_IP);
        }  else return HostUtils.getLocalHostName();
    }

    private static String toString(Properties source) throws IOException {
        StringWriter writer = new StringWriter();
        source.store(writer, null);
        return writer.toString();
    }

    private static Properties getProperties(CuratorFramework client, String file, Properties defaultValue) throws Exception {
        try {
            String v = ZooKeeperUtils.getStringData(client, file);
            if (v != null) {
                return DataStoreHelpers.toProperties(v);
            } else {
                return defaultValue;
            }
        } catch (KeeperException.NoNodeException e) {
            return defaultValue;
        }
    }

    void bindConfigAdmin(ConfigurationAdmin service) {
        this.configAdmin.bind(service);
    }

    void unbindConfigAdmin(ConfigurationAdmin service) {
        this.configAdmin.unbind(service);
    }

    void bindRegistrationHandler(DataStoreRegistrationHandler service) {
        this.registrationHandler.bind(service);
    }

    void unbindRegistrationHandler(DataStoreRegistrationHandler service) {
        this.registrationHandler.unbind(service);
    }
}
