package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.reactive.jpa.impl.RxPersisterClassResolverInitiator;
import org.hibernate.reactive.service.initiator.RxConnectionProviderInitiator;
import org.hibernate.reactive.service.initiator.RxDummyConnectionProviderInitiator;
import org.hibernate.reactive.service.initiator.RxJdbcEnvironmentInitiator;
import org.hibernate.reactive.service.initiator.RxQueryTranslatorFactoryInitiator;
import org.hibernate.reactive.service.initiator.RxTransactionCoordinatorBuilderInitiator;
import org.hibernate.service.spi.ServiceContributor;

/**
 * Contributes our
 * {@link org.hibernate.service.spi.ServiceInitiator service initiators}
 * to Hibernate.
 */
public class RxServiceRegistryInitializer implements ServiceContributor {

	@Override
	public void contribute(StandardServiceRegistryBuilder serviceRegistryBuilder) {
		serviceRegistryBuilder.addInitiator( RxDummyConnectionProviderInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxJdbcEnvironmentInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxConnectionProviderInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxTransactionCoordinatorBuilderInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxPersisterClassResolverInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( RxQueryTranslatorFactoryInitiator.INSTANCE );
	}
}
