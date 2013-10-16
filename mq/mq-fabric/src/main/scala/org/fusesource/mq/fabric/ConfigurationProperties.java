package org.fusesource.mq.fabric;

import org.springframework.beans.factory.FactoryBean;

import java.util.Properties;

public class ConfigurationProperties implements FactoryBean<Properties> {

    @Override
    public Properties getObject() throws Exception {
        return MQBrokerFactory.CONFIG_PROPERTIES.get();
    }

    @Override
    public Class<?> getObjectType() {
        return Properties.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }
}

