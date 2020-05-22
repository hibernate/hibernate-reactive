package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.reactive.jpa.impl.ReactivePersisterClassResolverInitiator;
import org.hibernate.reactive.pool.impl.SqlClientPoolInitiator;
import org.hibernate.reactive.service.initiator.DummyConnectionProviderInitiator;
import org.hibernate.reactive.service.initiator.NoJtaPlatformInitiator;
import org.hibernate.reactive.service.initiator.ReactiveJdbcEnvironmentInitiator;
import org.hibernate.reactive.service.initiator.ReactiveQueryTranslatorFactoryInitiator;
import org.hibernate.reactive.service.initiator.ReactiveTransactionCoordinatorBuilderInitiator;
import org.hibernate.reactive.vertx.impl.VertxInstanceInitiator;
import org.hibernate.service.spi.ServiceContributor;

/**
 * Contributes our
 * {@link org.hibernate.service.spi.ServiceInitiator service initiators}
 * to Hibernate.
 */
public class ReactiveServiceRegistryInitializer implements ServiceContributor {

	@Override
	public void contribute(StandardServiceRegistryBuilder serviceRegistryBuilder) {
		serviceRegistryBuilder.addInitiator( VertxInstanceInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( NoJtaPlatformInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( DummyConnectionProviderInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( ReactiveJdbcEnvironmentInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( ReactiveTransactionCoordinatorBuilderInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( ReactivePersisterClassResolverInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( ReactiveQueryTranslatorFactoryInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( SqlClientPoolInitiator.INSTANCE );
	}
}
