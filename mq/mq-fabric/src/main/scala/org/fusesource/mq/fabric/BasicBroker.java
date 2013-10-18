package org.fusesource.mq.fabric;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.fusesource.fabric.api.FabricException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BasicBroker implements ManagedBroker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedBroker.class);

    final BundleContext bundleContext;
    final Map<String, Object> config;
    final BrokerConfig brokerConfiguration;
    final String name;
    final AtomicBoolean active = new AtomicBoolean();
    final AtomicBoolean started = new AtomicBoolean();
    final AtomicLong lastModified = new AtomicLong(-1);
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    volatile BrokerInstance brokerInstance;
    private ServiceRegistration registration;

    public BasicBroker(BundleContext bundleContext, Map<String, Object> config) throws Exception {
        this.bundleContext = bundleContext;
        this.config = config;
        this.brokerConfiguration = BrokerConfig.builder().fromConfiguration(config).build();
        this.name = brokerConfiguration.getName();
    }

    public void init() {
        active.set(true);
        start(false);
    }

    @Override
    public void close() {
        active.set(false);
        stop(false);
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            FabricException.launderThrowable(e);
        }
    }

    public void start(boolean async) {
        if (started.compareAndSet(false, true)) {
            LOGGER.info("Broker {} is starting.", name);
            if (async) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        startLoop();
                    }
                });
            } else {
                startLoop();
            }
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    checkConfiguration();
                }
            });
        }
    }

    public void stop(boolean async) {
        if (started.compareAndSet(true, false)) {
            LOGGER.info("Broker {} is stopping.", name);
            if (async) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        stopBroker();
                    }
                });
            } else {
                stopBroker();
            }
            executorService.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
                brokerInstance = null;
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    synchronized boolean startBroker() {
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
            updateLastModified();
        } catch (InterruptedException e) {
            Thread.interrupted();
        } catch (Exception e) {
            LOGGER.warn("Broker {} failed to start.", e);
            success = false;
            lastModified.set(-1);
        } finally {
            return success;
        }
    }

    synchronized void stopBroker() {
        try {
            if (registration != null) {
                unregisterConnectionFactory();
            }

            if (brokerInstance != null) {
                brokerInstance.close();
            }
        } catch (Throwable t) {
            LOGGER.debug("Exception on close.", t);
        }
    }

    void startLoop() {
        boolean success = false;
        while (started.get() && !success && !Thread.currentThread().isInterrupted()) {
            success = startBroker();
        }
    }


    void checkConfiguration() {
        while (started.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (brokerConfiguration.isConfigCheckEnabled() &&
                        lastModified.get() != -1 &&
                        lastModified.get() != brokerInstance.getResource().lastModified()) {
                    //We have a shutdown hook that will restart the broker, if it's stopped without setting "started = false"
                    stopBroker();
                }
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                Thread.interrupted();
            } catch (Exception e) {
                //Ignored
            }
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

    class BrokerShutdownHook implements java.lang.Runnable {
        @Override
        public void run() {
            if (started.get()) {
                stopBroker();
                startBroker();
            }
        }
    }
}
