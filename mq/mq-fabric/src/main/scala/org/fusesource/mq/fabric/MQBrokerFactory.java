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

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.spring.SpringBrokerContext;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.xbean.classloader.MultiParentClassLoader;
import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.apache.xbean.spring.context.impl.URIEditor;
import org.fusesource.fabric.api.FabricException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;

import java.beans.PropertyEditorManager;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class MQBrokerFactory {


    private static final Logger LOGGER = LoggerFactory.getLogger(MQBrokerFactory.class);
    private static final String CONFIG = "config";
    public static final ThreadLocal<Properties> CONFIG_PROPERTIES = new ThreadLocal<Properties>();

    static {
        PropertyEditorManager.registerEditor(URI.class, URIEditor.class);
    }


    public static BrokerInstance createBroker(Map<String, ?> configuration) throws Exception {
        try {
            CONFIG_PROPERTIES.set(toProperties(configuration));
            String uri = String.valueOf(configuration.get(CONFIG));
            Thread.currentThread().setContextClassLoader(createMultiParentClassLoader());

            Resource resource = Utils.resourceFromString(uri);
            ResourceXmlApplicationContext ctx = new ResourceXmlApplicationContext((resource)) {
                @Override
                protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
                    reader.setValidating(false);
                }
            };

            BrokerService brokerService = findBrokerService(ctx);
            String networks = configuration.containsKey("network") ? String.valueOf(configuration.get("network")) : "";
            for (String network : networks.trim().split(",")) {
                if (!network.isEmpty()) {
                    LOGGER.info("Adding network connector " + network);
                    brokerService.addNetworkConnector(createNetworkConnector(network, configuration));
                }
            }
            configurePorts(brokerService, configuration);
            SpringBrokerContext brokerContext = new SpringBrokerContext();
            brokerContext.setConfigurationUrl(resource.getURL().toExternalForm());
            brokerContext.setApplicationContext(ctx);
            brokerService.setBrokerContext(brokerContext);
            return new BrokerInstance(ctx, brokerService, resource);
        } finally {
            CONFIG_PROPERTIES.remove();
        }
    }

    private static final Properties toProperties(Map<String, ?> config) {
        Properties properties = new Properties();
        properties.putAll(config);
        return properties;
    }

    private static ClassLoader createMultiParentClassLoader() {
        ClassLoader currentClassLoader = MQBrokerFactory.class.getClassLoader();
        ClassLoader brokerServiceClassLoader = BrokerService.class.getClassLoader();
        ClassLoader[] parents = {currentClassLoader, brokerServiceClassLoader};

        return new MultiParentClassLoader("xbean", new URL[0], parents);
    }

    /**
     * Creates a {@link NetworkConnector}.
     *
     * @param network The network.
     * @param config  The broker configuration.
     * @return Returns a {@link NetworkConnector}.
     * @throws URISyntaxException
     * @throws IOException
     */
    private static NetworkConnector createNetworkConnector(String network, Map<String, ?> config) throws URISyntaxException, IOException {
        DiscoveryNetworkConnector connector = new DiscoveryNetworkConnector(new URI("fabric:" + network));
        connector.setName("fabric-" + network);
        Map<String, Object> networkProperties = new HashMap<String, Object>();
        networkProperties.putAll(config);
        IntrospectionSupport.setProperties(connector, networkProperties, "network.");
        return connector;
    }

    /**
     * Apply port related configuration to the {@link BrokerService}.
     *
     * @param brokerService The target {@link BrokerService}.
     * @param config        The broker configuration.
     * @throws URISyntaxException
     */
    private static void configurePorts(BrokerService brokerService, Map<String, ?> config) throws URISyntaxException {
        for (TransportConnector connector : brokerService.getTransportConnectors()) {
            String portKey = connector.getName() + "-port";
            if (config.containsKey(portKey)) {
                int port = Integer.parseInt(String.valueOf(config.get(portKey)));
                URI template = connector.getUri();
                connector.setUri(new URI(template.getScheme(),
                        template.getUserInfo(),
                        template.getHost(),
                        port,
                        template.getPath(),
                        template.getQuery(),
                        template.getFragment()));
            }
        }
    }

    /**
     * Retrieves a {@link BrokerService} from the specified {@link ApplicationContext}.
     *
     * @param context The application context.
     * @return The {@link BrokerService} or throws a FabricException.
     */
    private static BrokerService findBrokerService(ApplicationContext context) {
        BrokerService brokerService = null;
        try {
            brokerService = context.getBean(BrokerService.class);
        } catch (BeansException e) {
            FabricException.launderThrowable(e);
        }
        return brokerService;
    }
}
