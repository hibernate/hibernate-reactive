package org.hibernate.rx.service.initiator;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.rx.service.RxSessionFactoryServiceRegistryFactoryImpl;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.SessionFactoryServiceRegistryFactory;

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
