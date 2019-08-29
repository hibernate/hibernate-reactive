package org.hibernate.rx.boot.impl;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
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

//		serviceRegistryBuilder.addInitiator( OgmQueryTranslatorFactoryInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OgmSessionFactoryServiceRegistryFactoryInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OgmPersisterClassResolverInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OgmConnectionProviderInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OgmDialectFactoryInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OgmTransactionCoordinatorBuilderInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OgmJtaPlatformInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( DatastoreProviderInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OptionsServiceInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OgmMutableIdentifierGeneratorFactoryInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( EventContextManagerInitiator.INSTANCE );
//
//		serviceRegistryBuilder.addInitiator( GridDialectInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( QueryableGridDialectInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( IdentityColumnAwareGridDialectInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( OptimisticLockingAwareGridDialectInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( MultigetGridDialectInitiator.INSTANCE );
//		serviceRegistryBuilder.addInitiator( StoredProcedureGridDialectInitiator.INSTANCE );

	}
}
