package org.hibernate.rx.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.rx.service.RxSessionFactoryServiceRegistryFactoryImpl;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.SessionFactoryServiceRegistryFactory;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * integrates our implementation of {@link SessionFactoryServiceRegistryFactory}.
 *
 * @see RxSessionFactoryServiceRegistryFactoryImpl
 */
public class RxSessionFactoryServiceRegistryFactoryInitiator implements StandardServiceInitiator<SessionFactoryServiceRegistryFactory> {

	public static final RxSessionFactoryServiceRegistryFactoryInitiator INSTANCE = new RxSessionFactoryServiceRegistryFactoryInitiator();

	@Override
	public Class<SessionFactoryServiceRegistryFactory> getServiceInitiated() {
		return SessionFactoryServiceRegistryFactory.class;
	}

	@Override
	public SessionFactoryServiceRegistryFactory initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new RxSessionFactoryServiceRegistryFactoryImpl( registry );
	}
}
