package org.hibernate.rx.boot.impl;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.rx.jpa.impl.RxPersisterClassResolverInitiator;
import org.hibernate.rx.service.initiator.RxConnectionProviderInitiator;
import org.hibernate.rx.service.initiator.RxSessionFactoryServiceRegistryFactoryInitiator;
import org.hibernate.rx.service.initiator.RxTransactionCoordinatorBuilderInitiator;
import org.hibernate.service.spi.ServiceContributor;

public class RxServiceRegistryInitializer implements ServiceContributor {

	@Override
	public void contribute(StandardServiceRegistryBuilder serviceRegistryBuilder) {
		Map<Object, Object> settings = serviceRegistryBuilder.getSettings();

//		serviceRegistryBuilder.addInitiator( RxJdbcServicesInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxSessionFactoryServiceRegistryFactoryInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxConnectionProviderInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxTransactionCoordinatorBuilderInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxPersisterClassResolverInitiator.INSTANCE );
	}
}
