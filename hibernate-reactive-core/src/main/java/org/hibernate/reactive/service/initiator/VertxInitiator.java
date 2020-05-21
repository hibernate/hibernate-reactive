package org.hibernate.reactive.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.reactive.service.SelfmanagingVertxService;
import org.hibernate.reactive.service.VertxService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * Factory for the default implementation of a VertxService
 */
public final class VertxInitiator implements StandardServiceInitiator<VertxService> {

    public static final VertxInitiator INSTANCE = new VertxInitiator();

    @Override
    public VertxService initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
        return new SelfmanagingVertxService();
    }

    @Override
    public Class<VertxService> getServiceInitiated() {
        return VertxService.class;
    }

}
