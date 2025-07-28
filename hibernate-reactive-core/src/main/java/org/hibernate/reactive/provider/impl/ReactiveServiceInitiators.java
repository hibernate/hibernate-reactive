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
import org.hibernate.engine.jdbc.dialect.internal.DialectFactoryInitiator;
import org.hibernate.engine.jdbc.dialect.internal.DialectResolverInitiator;
import org.hibernate.engine.jdbc.internal.JdbcServicesInitiator;
import org.hibernate.engine.jdbc.internal.SqlStatementLoggerInitiator;
import org.hibernate.engine.jndi.internal.JndiServiceInitiator;
import org.hibernate.event.internal.EntityCopyObserverFactoryInitiator;
import org.hibernate.internal.util.cache.InternalCacheFactoryInitiator;
import org.hibernate.persister.internal.PersisterFactoryInitiator;
import org.hibernate.property.access.internal.PropertyAccessStrategyResolverInitiator;
import org.hibernate.reactive.context.impl.VertxContextInitiator;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorServiceInitiator;
import org.hibernate.reactive.loader.ast.internal.ReactiveBatchLoaderFactoryInitiator;
import org.hibernate.reactive.pool.impl.ReactiveConnectionPoolInitiator;
import org.hibernate.reactive.pool.impl.SqlClientPoolConfigurationInitiator;
import org.hibernate.reactive.provider.service.NativeParametersHandling;
import org.hibernate.reactive.provider.service.NoJdbcConnectionProviderInitiator;
import org.hibernate.reactive.provider.service.NoJdbcEnvironmentInitiator;
import org.hibernate.reactive.provider.service.NoJdbcMultiTenantConnectionProviderInitiator;
import org.hibernate.reactive.provider.service.NoJtaPlatformInitiator;
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
import org.hibernate.tool.schema.internal.script.SqlScriptExtractorInitiator;

import static java.util.Collections.unmodifiableList;

/**
 * Provides the list of service initiators that Hibernate Reactive needs to integrate with Hibernate ORM.
 */
public final class ReactiveServiceInitiators {

	private ReactiveServiceInitiators() {
	}

	public static final List<StandardServiceInitiator<?>> LIST = buildInitialServiceInitiatorList();

	// Note that Quarkus uses a different list for the initialization of the services:
	// If you update this list makes sure to check that reactive still works with Quarkus.
	// Also: please try to maintain the same order as Hibernate ORM for sake of comparisons.
	// See also test ServiceInitiatorsTest; among basic validations it prints a sorted summary
	// for convenience.
	private static List<StandardServiceInitiator<?>> buildInitialServiceInitiatorList() {
		final ArrayList<StandardServiceInitiator<?>> serviceInitiators = new ArrayList<>();

		// Custom for Hibernate Reactive: SessionFactoryBuilderService
		serviceInitiators.add( ReactiveSessionFactoryBuilderInitiator.INSTANCE );

		// [standard] BytecodeProvider
		serviceInitiators.add( BytecodeProviderInitiator.INSTANCE );

		// [standard] ProxyFactoryFactory
		serviceInitiators.add( ProxyFactoryFactoryInitiator.INSTANCE );

		// [standard] CfgXmlAccessService
		serviceInitiators.add( CfgXmlAccessServiceInitiator.INSTANCE );

		// [standard] ConfigurationService
		serviceInitiators.add( ConfigurationServiceInitiator.INSTANCE );

		// [standard] PropertyAccessStrategyResolver
		serviceInitiators.add( PropertyAccessStrategyResolverInitiator.INSTANCE );

		// [standard] SqlScriptCommandExtractor
		serviceInitiators.add( SqlScriptExtractorInitiator.INSTANCE );

		// Custom for Hibernate Reactive: SchemaManagementTool
		serviceInitiators.add( ReactiveSchemaManagementToolInitiator.INSTANCE );

		// Custom for Hibernate Reactive: JdbcEnvironment
		serviceInitiators.add( NoJdbcEnvironmentInitiator.INSTANCE );

		// [standard] JndiService
		serviceInitiators.add( JndiServiceInitiator.INSTANCE );

		// Custom for Hibernate Reactive: PersisterClassResolver
		serviceInitiators.add( ReactivePersisterClassResolverInitiator.INSTANCE );

		// [standard] PersisterFactory
		serviceInitiators.add( PersisterFactoryInitiator.INSTANCE );

		// Custom for Hibernate Reactive: ConnectionProvider [disabled]
		serviceInitiators.add( NoJdbcConnectionProviderInitiator.INSTANCE );

		// Custom for Hibernate Reactive: MultiTenantConnectionProvider [disabled]
		serviceInitiators.add( NoJdbcMultiTenantConnectionProviderInitiator.INSTANCE );

		// [standard] DialectResolver
		serviceInitiators.add( DialectResolverInitiator.INSTANCE );

		// [standard] DialectFactoryInitiator.INSTANCE
		serviceInitiators.add( DialectFactoryInitiator.INSTANCE );

		// Custom for Hibernate Reactive: MutationExecutorService
		serviceInitiators.add( ReactiveMutationExecutorServiceInitiator.INSTANCE );

		// [standard] BatchBuilder
		serviceInitiators.add( BatchBuilderInitiator.INSTANCE );

		// [standard] JdbcServices
		serviceInitiators.add( JdbcServicesInitiator.INSTANCE );

		// [standard] RefCursorSupport
		serviceInitiators.add( RefCursorSupportInitiator.INSTANCE );

		// JtaPlatformResolver: REMOVED

		// Custom for Hibernate Reactive: JtaPlatform [disabled]
		serviceInitiators.add( NoJtaPlatformInitiator.INSTANCE );

		// [standard] SessionFactoryServiceRegistryFactory
		serviceInitiators.add( SessionFactoryServiceRegistryFactoryInitiator.INSTANCE );

		// [standard] RegionFactory
		serviceInitiators.add( RegionFactoryInitiator.INSTANCE );

		// [standard] TransactionCoordinatorBuilder
		serviceInitiators.add( TransactionCoordinatorBuilderInitiator.INSTANCE );

		// [standard] ManagedBeanRegistry
		serviceInitiators.add( ManagedBeanRegistryInitiator.INSTANCE );

		// [standard] EntityCopyObserverFactory
		serviceInitiators.add( EntityCopyObserverFactoryInitiator.INSTANCE );

		// Custom for Hibernate Reactive: JdbcValuesMappingProducerProvider
		serviceInitiators.add( ReactiveValuesMappingProducerProviderInitiator.INSTANCE );

		// Custom for Hibernate Reactive: SqmMultiTableMutationStrategyProvider
		serviceInitiators.add( ReactiveSqmMultiTableMutationStrategyProviderInitiator.INSTANCE );

		// Custom for Hibernate Reactive: NativeParametersHandling
		serviceInitiators.add( NativeParametersHandling.INSTANCE );

		// [standard] SqlStatementLogger
		serviceInitiators.add( SqlStatementLoggerInitiator.INSTANCE );

		// Custom for Hibernate Reactive: BatchLoaderFactory
		serviceInitiators.add( ReactiveBatchLoaderFactoryInitiator.INSTANCE );

		// [standard] InternalCacheFactoryService
		serviceInitiators.add( InternalCacheFactoryInitiator.INSTANCE );

		// --- end of services defined by Hibernate ORM

		// --- custom ones follow:

		// ReactiveMarkerService - Definitely exclusive to Hibernate Reactive, as it marks this particular registry as Reactive:
		serviceInitiators.add( ReactiveMarkerServiceInitiator.INSTANCE );

		// VertxInstance - Exclusive to Hibernate Reactive:
		serviceInitiators.add( VertxInstanceInitiator.INSTANCE );

		// Context - Exclusive to Hibernate Reactive:
		serviceInitiators.add( VertxContextInitiator.INSTANCE );

		// SqlClientPoolConfiguration - Exclusive to Hibernate Reactive:
		serviceInitiators.add( SqlClientPoolConfigurationInitiator.INSTANCE );

		// ReactiveConnectionPool - Exclusive to Hibernate Reactive:
		serviceInitiators.add( ReactiveConnectionPoolInitiator.INSTANCE );

		// --- end of custom services.

		serviceInitiators.trimToSize();

		return unmodifiableList( serviceInitiators );
	}

}
