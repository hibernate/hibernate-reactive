package org.hibernate.rx.boot.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.boot.cfgxml.internal.CfgXmlAccessServiceInitiator;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.boot.registry.internal.StandardServiceRegistryImpl;
import org.hibernate.engine.config.internal.ConfigurationServiceInitiator;
import org.hibernate.engine.jdbc.batch.internal.BatchBuilderInitiator;
import org.hibernate.engine.jdbc.connections.internal.ConnectionProviderInitiator;
import org.hibernate.engine.jdbc.connections.internal.MultiTenantConnectionProviderInitiator;
import org.hibernate.engine.jdbc.cursor.internal.RefCursorSupportInitiator;
import org.hibernate.engine.jdbc.dialect.internal.DialectFactoryInitiator;
import org.hibernate.engine.jdbc.dialect.internal.DialectResolverInitiator;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentInitiator;
import org.hibernate.engine.jdbc.internal.JdbcServicesInitiator;
import org.hibernate.engine.transaction.jta.platform.internal.JtaPlatformInitiator;
import org.hibernate.engine.transaction.jta.platform.internal.JtaPlatformResolverInitiator;
import org.hibernate.event.internal.EntityCopyObserverFactoryInitiator;
import org.hibernate.id.factory.internal.MutableIdentifierGeneratorFactoryInitiator;
import org.hibernate.persister.internal.PersisterClassResolverInitiator;
import org.hibernate.property.access.internal.PropertyAccessStrategyResolverInitiator;
import org.hibernate.resource.beans.spi.ManagedBeanRegistryInitiator;
import org.hibernate.resource.transaction.internal.TransactionCoordinatorBuilderInitiator;
import org.hibernate.service.internal.ProvidedService;
import org.hibernate.service.internal.SessionFactoryServiceRegistryFactoryInitiator;
import org.hibernate.tool.hbm2ddl.ImportSqlCommandExtractorInitiator;
import org.hibernate.tool.schema.internal.SchemaManagementToolInitiator;

public class ServiceRegistryBootstrap {

	private final static List<StandardServiceInitiator> initiators = standardInitiatorList();

	private final static List<ProvidedService> providedServices = new ArrayList<ProvidedService>();

	private static BootstrapServiceRegistry buildBootstrapServiceRegistry() {

		final BootstrapServiceRegistryBuilder bsrBuilder = new BootstrapServiceRegistryBuilder();
		final ClassLoaderService providedClassLoaderService = FlatClassLoaderService.INSTANCE;
		bsrBuilder.applyClassLoaderService( providedClassLoaderService );

		return bsrBuilder.build();
	}

	public static StandardServiceRegistry build(Map configurationProperties) {
		return new StandardServiceRegistryImpl( false, buildBootstrapServiceRegistry(), initiators,
												providedServices, configurationProperties
		);
	}

	private static List<StandardServiceInitiator> standardInitiatorList() {
		final ArrayList<StandardServiceInitiator> serviceInitiators = new ArrayList<StandardServiceInitiator>();

		// Custom RX initiators:
//        serviceInitiators.add( RxRuntimeModelDescriptorClassResolver.INSTANCE );

		//TODO: Add other services

		// Standard ones in ORM (see org.hibernate.service.StandardServiceInitiators ) :

		//TODO: remove all services which are in the way:

		serviceInitiators.add( CfgXmlAccessServiceInitiator.INSTANCE );
		serviceInitiators.add( ConfigurationServiceInitiator.INSTANCE );
		serviceInitiators.add( PropertyAccessStrategyResolverInitiator.INSTANCE );

		serviceInitiators.add( ImportSqlCommandExtractorInitiator.INSTANCE );
		serviceInitiators.add( SchemaManagementToolInitiator.INSTANCE );

		serviceInitiators.add( JdbcEnvironmentInitiator.INSTANCE );
//        serviceInitiators.add( JndiServiceInitiator.INSTANCE );
//        serviceInitiators.add( JmxServiceInitiator.INSTANCE );

		serviceInitiators.add( PersisterClassResolverInitiator.INSTANCE );
//        serviceInitiators.add( RuntimeModelDescriptorFactoryServiceInitiator.INSTANCE );

		serviceInitiators.add( ConnectionProviderInitiator.INSTANCE );
		serviceInitiators.add( MultiTenantConnectionProviderInitiator.INSTANCE );
		serviceInitiators.add( DialectResolverInitiator.INSTANCE );
		serviceInitiators.add( DialectFactoryInitiator.INSTANCE );
		serviceInitiators.add( BatchBuilderInitiator.INSTANCE );
		serviceInitiators.add( JdbcServicesInitiator.INSTANCE );
		serviceInitiators.add( RefCursorSupportInitiator.INSTANCE );

		serviceInitiators.add( MutableIdentifierGeneratorFactoryInitiator.INSTANCE );

		serviceInitiators.add( JtaPlatformResolverInitiator.INSTANCE );
		serviceInitiators.add( JtaPlatformInitiator.INSTANCE );

		serviceInitiators.add( SessionFactoryServiceRegistryFactoryInitiator.INSTANCE );

//        serviceInitiators.add( RegionFactoryInitiator.INSTANCE );

		serviceInitiators.add( TransactionCoordinatorBuilderInitiator.INSTANCE );

		serviceInitiators.add( ManagedBeanRegistryInitiator.INSTANCE );
		serviceInitiators.add( EntityCopyObserverFactoryInitiator.INSTANCE );

		serviceInitiators.trimToSize();

		return Collections.unmodifiableList( serviceInitiators );
	}


}
