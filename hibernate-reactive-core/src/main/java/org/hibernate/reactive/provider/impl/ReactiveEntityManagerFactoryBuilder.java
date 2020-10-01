/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.impl;

import org.hibernate.boot.internal.MetadataImpl;
import org.hibernate.boot.internal.SessionFactoryBuilderImpl;
import org.hibernate.boot.internal.SessionFactoryOptionsBuilder;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryBuilderImplementor;
import org.hibernate.jpa.boot.internal.EntityManagerFactoryBuilderImpl;
import org.hibernate.jpa.boot.spi.PersistenceUnitDescriptor;
import org.hibernate.reactive.bulk.impl.ReactiveBulkIdStrategy;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.service.ReactiveSessionFactoryBuilder;

import javax.persistence.EntityManagerFactory;
import java.util.Map;

/**
 * Heavily inspired by {@link EntityManagerFactoryBuilderImpl}.
 * This is intentionally not supporting several integration points of Hibernate ORM:
 * we can't test them all, better to build up integration points gradually.
 */
public final class ReactiveEntityManagerFactoryBuilder extends EntityManagerFactoryBuilderImpl {

    public ReactiveEntityManagerFactoryBuilder(PersistenceUnitDescriptor persistenceUnitDescriptor, Map integration) {
        super( persistenceUnitDescriptor, integration );
    }

    //Overridden so to use a customized serviceregistry: see ReactiveServiceRegistryBuilder
    @Override
    protected StandardServiceRegistryBuilder getStandardServiceRegistryBuilder(BootstrapServiceRegistry bsr) {
        return ReactiveServiceRegistryBuilder.forJpa( bsr );
    }

    //Overridden to so provide a custom SessionFactoryBuilder; also, we want to avoid loading random
    //SessionFactoryBuilder implementations that might be found on the classpath.
    @Override
    public EntityManagerFactory build() {
        final MetadataImplementor metadata = metadata();

        SessionFactoryOptionsBuilder optionsBuilder = new SessionFactoryOptionsBuilder(
                metadata.getMetadataBuildingOptions().getServiceRegistry(),
                ( (MetadataImpl) metadata).getBootstrapContext()
        );
        optionsBuilder.enableCollectionInDefaultFetchGroup(true);
        optionsBuilder.applyMultiTableBulkIdStrategy( new ReactiveBulkIdStrategy( metadata ) );

        final SessionFactoryBuilderImpl defaultBuilder = new SessionFactoryBuilderImpl( metadata, optionsBuilder );
        final SessionFactoryBuilderImplementor reactiveSessionFactoryBuilder = new ReactiveSessionFactoryBuilder( metadata, defaultBuilder );
        populateSfBuilder( reactiveSessionFactoryBuilder, getStandardServiceRegistry() );

        try {
            return reactiveSessionFactoryBuilder.build();
        }
        catch (Exception e) {
            throw persistenceException( "Unable to build Hibernate SessionFactory", e );
        }
    }

}
