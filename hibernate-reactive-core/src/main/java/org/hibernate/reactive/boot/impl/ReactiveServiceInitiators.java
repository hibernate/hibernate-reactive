/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.impl;

import org.hibernate.boot.cfgxml.internal.CfgXmlAccessServiceInitiator;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.bytecode.internal.BytecodeProviderInitiator;
import org.hibernate.bytecode.internal.ProxyFactoryFactoryInitiator;
import org.hibernate.cache.internal.RegionFactoryInitiator;
import org.hibernate.engine.config.internal.ConfigurationServiceInitiator;
import org.hibernate.engine.jdbc.batch.internal.BatchBuilderInitiator;
import org.hibernate.engine.jdbc.connections.internal.MultiTenantConnectionProviderInitiator;
import org.hibernate.engine.jdbc.cursor.internal.RefCursorSupportInitiator;
import org.hibernate.engine.jdbc.dialect.internal.DialectFactoryInitiator;
import org.hibernate.engine.jdbc.dialect.internal.DialectResolverInitiator;
import org.hibernate.engine.jdbc.internal.JdbcServicesInitiator;
import org.hibernate.engine.jndi.internal.JndiServiceInitiator;
import org.hibernate.event.internal.EntityCopyObserverFactoryInitiator;
import org.hibernate.jmx.internal.JmxServiceInitiator;
import org.hibernate.persister.internal.PersisterFactoryInitiator;
import org.hibernate.property.access.internal.PropertyAccessStrategyResolverInitiator;
import org.hibernate.reactive.boot.service.NoJdbcConnectionProviderInitiator;
import org.hibernate.reactive.boot.service.NoJdbcEnvironmentInitiator;
import org.hibernate.reactive.boot.service.NoJtaPlatformInitiator;
import org.hibernate.reactive.boot.service.ReactiveQueryTranslatorFactoryInitiator;
import org.hibernate.reactive.id.impl.ReactiveIdentifierGeneratorFactoryInitiator;
import org.hibernate.reactive.jpa.impl.ReactivePersisterClassResolverInitiator;
import org.hibernate.reactive.pool.impl.SqlClientPoolInitiator;
import org.hibernate.reactive.vertx.impl.VertxInstanceInitiator;
import org.hibernate.resource.beans.spi.ManagedBeanRegistryInitiator;
import org.hibernate.resource.transaction.internal.TransactionCoordinatorBuilderInitiator;
import org.hibernate.service.internal.SessionFactoryServiceRegistryFactoryInitiator;
import org.hibernate.tool.hbm2ddl.ImportSqlCommandExtractorInitiator;
import org.hibernate.tool.schema.internal.SchemaManagementToolInitiator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ReactiveServiceInitiators {

    private ReactiveServiceInitiators() {
    }

    public static final List<StandardServiceInitiator> LIST = buildInitialServiceInitiatorList();

    private static List<StandardServiceInitiator> buildInitialServiceInitiatorList() {
        final ArrayList<StandardServiceInitiator> serviceInitiators = new ArrayList<StandardServiceInitiator>();

        // Exclusive to Hibernate Reactive:
        serviceInitiators.add( VertxInstanceInitiator.INSTANCE );

        // Exclusive to Hibernate Reactive:
        serviceInitiators.add( SqlClientPoolInitiator.INSTANCE );

        //Custom for Hibernate Reactive:
        serviceInitiators.add( ReactiveSessionFactoryBuilderInitiator.INSTANCE );

        serviceInitiators.add( BytecodeProviderInitiator.INSTANCE );
        serviceInitiators.add( ProxyFactoryFactoryInitiator.INSTANCE );

        serviceInitiators.add( CfgXmlAccessServiceInitiator.INSTANCE );
        serviceInitiators.add( ConfigurationServiceInitiator.INSTANCE );
        serviceInitiators.add( PropertyAccessStrategyResolverInitiator.INSTANCE );

        serviceInitiators.add( ImportSqlCommandExtractorInitiator.INSTANCE );
        serviceInitiators.add( SchemaManagementToolInitiator.INSTANCE );

        //Custom for Hibernate Reactive:
        serviceInitiators.add( NoJdbcEnvironmentInitiator.INSTANCE );

        serviceInitiators.add( JndiServiceInitiator.INSTANCE );
        serviceInitiators.add( JmxServiceInitiator.INSTANCE );

        //Custom for Hibernate Reactive:
        serviceInitiators.add( ReactivePersisterClassResolverInitiator.INSTANCE );
        serviceInitiators.add( PersisterFactoryInitiator.INSTANCE );

        //Custom for Hibernate Reactive:
        serviceInitiators.add( NoJdbcConnectionProviderInitiator.INSTANCE );

        serviceInitiators.add( MultiTenantConnectionProviderInitiator.INSTANCE );
        serviceInitiators.add( DialectResolverInitiator.INSTANCE );
        serviceInitiators.add( DialectFactoryInitiator.INSTANCE );
        serviceInitiators.add( BatchBuilderInitiator.INSTANCE );
        serviceInitiators.add( JdbcServicesInitiator.INSTANCE );
        serviceInitiators.add( RefCursorSupportInitiator.INSTANCE );

        //Custom for Hibernate Reactive:
        serviceInitiators.add( ReactiveQueryTranslatorFactoryInitiator.INSTANCE );

        //Custom for Hibernate Reactive:
        serviceInitiators.add( ReactiveIdentifierGeneratorFactoryInitiator.INSTANCE);


        //Custom for Hibernate Reactive:
        serviceInitiators.add( NoJtaPlatformInitiator.INSTANCE );

        serviceInitiators.add( SessionFactoryServiceRegistryFactoryInitiator.INSTANCE );

        serviceInitiators.add( RegionFactoryInitiator.INSTANCE );

        serviceInitiators.add( TransactionCoordinatorBuilderInitiator.INSTANCE );

        serviceInitiators.add( ManagedBeanRegistryInitiator.INSTANCE );
        serviceInitiators.add( EntityCopyObserverFactoryInitiator.INSTANCE );

        serviceInitiators.trimToSize();

        return Collections.unmodifiableList( serviceInitiators );
    }
}
