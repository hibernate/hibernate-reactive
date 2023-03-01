/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.boot.cfgxml.internal.CfgXmlAccessServiceInitiator;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.bytecode.internal.BytecodeProviderInitiator;
import org.hibernate.bytecode.internal.ProxyFactoryFactoryInitiator;
import org.hibernate.cache.internal.RegionFactoryInitiator;
import org.hibernate.engine.config.internal.ConfigurationServiceInitiator;
import org.hibernate.engine.jdbc.batch.internal.BatchBuilderInitiator;
import org.hibernate.engine.jdbc.cursor.internal.RefCursorSupportInitiator;
import org.hibernate.engine.jdbc.dialect.internal.DialectResolverInitiator;
import org.hibernate.engine.jdbc.internal.JdbcServicesInitiator;
import org.hibernate.engine.jndi.internal.JndiServiceInitiator;
import org.hibernate.event.internal.EntityCopyObserverFactoryInitiator;
import org.hibernate.persister.internal.PersisterFactoryInitiator;
import org.hibernate.property.access.internal.PropertyAccessStrategyResolverInitiator;
import org.hibernate.reactive.context.impl.VertxContextInitiator;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorServiceInitiator;
import org.hibernate.reactive.id.factory.spi.ReactiveIdentifierGeneratorFactoryInitiator;
import org.hibernate.reactive.pool.impl.ReactiveConnectionPoolInitiator;
import org.hibernate.reactive.pool.impl.SqlClientPoolConfigurationInitiator;
import org.hibernate.reactive.provider.service.NoJdbcConnectionProviderInitiator;
import org.hibernate.reactive.provider.service.NoJdbcEnvironmentInitiator;
import org.hibernate.reactive.provider.service.NoJdbcMultiTenantConnectionProviderInitiator;
import org.hibernate.reactive.provider.service.NoJtaPlatformInitiator;
import org.hibernate.reactive.provider.service.ReactiveDialectFactoryInitiator;
import org.hibernate.reactive.provider.service.ReactiveMarkerServiceInitiator;
import org.hibernate.reactive.provider.service.ReactivePersisterClassResolverInitiator;
import org.hibernate.reactive.provider.service.ReactiveSchemaManagementToolInitiator;
import org.hibernate.reactive.provider.service.ReactiveSessionFactoryBuilderInitiator;
import org.hibernate.reactive.provider.service.ReactiveSqmMultiTableMutationStrategyProviderInitiator;
import org.hibernate.reactive.provider.service.ReactiveValuesMappingProducerProviderInitiator;
import org.hibernate.reactive.vertx.impl.VertxInstanceInitiator;
import org.hibernate.resource.beans.spi.ManagedBeanRegistryInitiator;
import org.hibernate.resource.transaction.internal.TransactionCoordinatorBuilderInitiator;
import org.hibernate.service.internal.SessionFactoryServiceRegistryFactoryInitiator;
import org.hibernate.sql.ast.internal.JdbcParameterRendererInitiator;
import org.hibernate.tool.schema.internal.script.SqlScriptExtractorInitiator;

import static java.util.Collections.unmodifiableList;

/**
 * Provides the list of service initiators that Hibernate Reactive needs to integrate with Hibernate ORM.
 */
// FIXME: We need to double check this class because I've deleted some services
public final class ReactiveServiceInitiators {

	private ReactiveServiceInitiators() {
	}

	public static final List<StandardServiceInitiator<?>> LIST = buildInitialServiceInitiatorList();

	// Note that Quarkus uses a different list for the initialization of the services.
	// If you update this list makes sure to check that reactive still works with Quarkus
	// in native mode.
	private static List<StandardServiceInitiator<?>> buildInitialServiceInitiatorList() {
		final ArrayList<StandardServiceInitiator<?>> serviceInitiators = new ArrayList<>();

		serviceInitiators.add( SqlScriptExtractorInitiator.INSTANCE );

		serviceInitiators.add( ReactiveMutationExecutorServiceInitiator.INSTANCE );

		// Definitely exclusive to Hibernate Reactive, as it marks the registry as Reactive:
		serviceInitiators.add( ReactiveMarkerServiceInitiator.INSTANCE );

		// Exclusive to Hibernate Reactive:
		serviceInitiators.add( VertxInstanceInitiator.INSTANCE );
		serviceInitiators.add( VertxContextInitiator.INSTANCE );

		// Exclusive to Hibernate Reactive:
		serviceInitiators.add( SqlClientPoolConfigurationInitiator.INSTANCE );
		serviceInitiators.add( ReactiveConnectionPoolInitiator.INSTANCE );

		// Custom for Hibernate Reactive:
		serviceInitiators.add( ReactiveSessionFactoryBuilderInitiator.INSTANCE );

		serviceInitiators.add( BytecodeProviderInitiator.INSTANCE );
		serviceInitiators.add( ProxyFactoryFactoryInitiator.INSTANCE );

		serviceInitiators.add( CfgXmlAccessServiceInitiator.INSTANCE );
		serviceInitiators.add( ConfigurationServiceInitiator.INSTANCE );
		serviceInitiators.add( PropertyAccessStrategyResolverInitiator.INSTANCE );

		// Custom for Hibernate Reactive:
		serviceInitiators.add( NoJdbcEnvironmentInitiator.INSTANCE );

		serviceInitiators.add( JndiServiceInitiator.INSTANCE );

		// Custom for Hibernate Reactive:
		serviceInitiators.add( ReactivePersisterClassResolverInitiator.INSTANCE );
		serviceInitiators.add( PersisterFactoryInitiator.INSTANCE );

		// Custom for Hibernate Reactive:
		serviceInitiators.add( NoJdbcConnectionProviderInitiator.INSTANCE );
		serviceInitiators.add( NoJdbcMultiTenantConnectionProviderInitiator.INSTANCE );

		serviceInitiators.add( DialectResolverInitiator.INSTANCE );

		//Custom for Hibernate Reactive:
		serviceInitiators.add( ReactiveDialectFactoryInitiator.INSTANCE );
		serviceInitiators.add( BatchBuilderInitiator.INSTANCE );
		serviceInitiators.add( JdbcServicesInitiator.INSTANCE );
		serviceInitiators.add( RefCursorSupportInitiator.INSTANCE );

		// Custom for Hibernate Reactive:
		serviceInitiators.add( ReactiveSchemaManagementToolInitiator.INSTANCE );

		// Custom for Hibernate Reactive:
		serviceInitiators.add( ReactiveIdentifierGeneratorFactoryInitiator.INSTANCE);

		// Custom for Hibernate Reactive:
		serviceInitiators.add( NoJtaPlatformInitiator.INSTANCE );

		serviceInitiators.add( SessionFactoryServiceRegistryFactoryInitiator.INSTANCE );

		serviceInitiators.add( RegionFactoryInitiator.INSTANCE );

		serviceInitiators.add( TransactionCoordinatorBuilderInitiator.INSTANCE );

		serviceInitiators.add( ManagedBeanRegistryInitiator.INSTANCE );
		serviceInitiators.add( EntityCopyObserverFactoryInitiator.INSTANCE );

		//Custom for Hibernate Reactive:
		serviceInitiators.add( ReactiveValuesMappingProducerProviderInitiator.INSTANCE );

		//Custom for Hibernate Reactive:
		serviceInitiators.add( ReactiveSqmMultiTableMutationStrategyProviderInitiator.INSTANCE );

		// Default implementation
		serviceInitiators.add( JdbcParameterRendererInitiator.INSTANCE );

		serviceInitiators.trimToSize();

		return unmodifiableList( serviceInitiators );
	}
}
