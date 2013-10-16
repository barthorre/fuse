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
import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

import java.io.Closeable;
import java.io.IOException;

public class BrokerInstance implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerInstance.class);

    private final ResourceXmlApplicationContext brokerContext;
    private final BrokerService brokerService;
    private final Resource resource;

    public BrokerInstance(ResourceXmlApplicationContext brokerContext, BrokerService brokerService, Resource resource) {
        this.brokerContext = brokerContext;
        this.brokerService = brokerService;
        this.resource = resource;
    }


    public BrokerService getBrokerService() {
        return brokerService;
    }

    public Resource getResource() {
        return resource;
    }


    @Override
    public void close() throws IOException {
        try {
            brokerService.stop();
            brokerService.waitUntilStopped();
        } catch (Throwable t) {
            LOGGER.debug("Error stopping the BrokerService.", t);
        }

        try {
            brokerContext.close();
        } catch (Throwable t) {
            LOGGER.debug("Exception on close.", t);
        }
    }
}
