package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.reactive.jpa.impl.ReactivePersisterClassResolverInitiator;
import org.hibernate.reactive.pool.impl.SqlClientPoolInitiator;
import org.hibernate.reactive.boot.service.NoJdbcConnectionProviderInitiator;
import org.hibernate.reactive.boot.service.NoJtaPlatformInitiator;
import org.hibernate.reactive.boot.service.NoJdbcEnvironmentInitiator;
import org.hibernate.reactive.boot.service.ReactiveQueryTranslatorFactoryInitiator;
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
		serviceRegistryBuilder.addInitiator( NoJdbcConnectionProviderInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( NoJdbcEnvironmentInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( ReactivePersisterClassResolverInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( ReactiveQueryTranslatorFactoryInitiator.INSTANCE );
		serviceRegistryBuilder.addInitiator( SqlClientPoolInitiator.INSTANCE );
	}
}
