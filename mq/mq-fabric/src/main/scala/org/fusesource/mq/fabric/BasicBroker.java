package org.fusesource.mq.fabric;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BasicBroker implements ManagedBroker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedBroker.class);

    final BundleContext bundleContext;
    final Map<String, Object> config;
    final BrokerConfig brokerConfiguration;
    final String name;
    final AtomicBoolean started = new AtomicBoolean();
    final AtomicLong lastModified = new AtomicLong(-1);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    volatile BrokerInstance brokerInstance;
    private ServiceRegistration registration;

    public BasicBroker(BundleContext bundleContext, Map<String, Object> config) throws Exception {
        this.bundleContext = bundleContext;
        this.config = config;
        this.brokerConfiguration = BrokerConfig.builder().fromConfiguration(config).build();
        this.name = brokerConfiguration.getName();
    }

    public void init() {
        start();
    }

    @Override
    public void close() {
        stop();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            LOGGER.info("Broker {} is starting.", name);
            executorService.submit(new BrokerBootstrap());
            executorService.submit( new BrokerCheckConfig());
        }
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            LOGGER.info("Broker {} is stopping.", name);
            executorService.submit(new BrokerShutdown());
        }
    }


    synchronized boolean tryStart() {
        boolean success = false;
        try {
            brokerInstance = MQBrokerFactory.createBroker(config);
            brokerInstance.getBrokerService().addShutdownHook(new BrokerShutdownHook());
            brokerInstance.getBrokerService().start();
            LOGGER.info("Broker {} has started.", name);
            if (brokerConfiguration.isServiceRegistrationEnabled()) {
                registerConnectionFactory(brokerInstance.getBrokerService());
            }
            success = true;
        } catch (InterruptedException e) {
            Thread.interrupted();
        } catch (Exception e) {
            success = false;
        } finally {
            updateLastModified();
            return success;
        }
    }

    synchronized void tryStop() {
        try {
            if (brokerInstance != null) {
                brokerInstance.close();
                brokerInstance.getBrokerService().waitUntilStopped();
                brokerInstance = null;
            }

            if (registration != null) {
                unregisterConnectionFactory();
            }

        } catch (Throwable t) {
            LOGGER.debug("Exception on close.", t);
        }
    }

    void registerConnectionFactory(BrokerService brokerService) {
        try {
            Hashtable<String, String> props = new Hashtable<String, String>();
            props.put("name", brokerService.getBrokerName());
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://" + brokerService.getBrokerName() + "?create=false");
            registration = bundleContext.registerService(ConnectionFactory.class.getName(), connectionFactory, props);
            LOGGER.debug("Registration of type " + ConnectionFactory.class.getName() + " as: " + connectionFactory + " with name: " + brokerService.getBrokerName() + "; " + registration);
        } catch (Exception ex) {
            LOGGER.debug("Error registering " + ConnectionFactory.class.getName() + ".", ex);
        }
    }

    void unregisterConnectionFactory() {
        if (registration != null) {
            registration.unregister();
        }
        LOGGER.debug("Un-register connection factory.");
    }

    private void updateLastModified() {
        try {
            lastModified.set(brokerInstance.getResource().lastModified());
        } catch (IOException e) {
           LOGGER.debug("Error getting broker resource last modified time.");
        }
    }

    public BrokerConfig getBrokerConfiguration() {
        return brokerConfiguration;
    }

    class BrokerBootstrap implements Runnable {
        boolean success = false;
        @Override
        public void run() {
            while (started.get() && !success && !Thread.currentThread().isInterrupted()) {
                success = tryStart();
            }
        }
    }

    class BrokerCheckConfig implements Runnable {
        @Override
        public void run() {
            while (started.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    if (brokerConfiguration.isConfigCheckEnabled() && lastModified.get() != brokerInstance.getResource().lastModified()) {
                        close();
                        init();
                        return;
                    }
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                } catch (Exception e) {
                    //Ignored
                }
            }
        }
    }

    class BrokerShutdown implements Runnable {
        @Override
        public void run() {
            tryStop();
        }
    }

    class BrokerShutdownHook implements Runnable {
        @Override
        public void run() {
            if (started.get()) {
                tryStart();
            }
        }
    }
}
