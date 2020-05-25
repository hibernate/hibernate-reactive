/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.jpa.impl;

import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.jpa.boot.internal.EntityManagerFactoryBuilderImpl;
import org.hibernate.jpa.boot.spi.PersistenceUnitDescriptor;
import org.hibernate.reactive.boot.impl.ReactiveServiceRegistryBuilder;

import java.util.Map;

/**
 * Heavily inspired by org.hibernate.jpa.boot.internal.EntityManagerFactoryBuilderImpl
 * This is intentionally not supporting several integration points of Hibernate ORM:
 * we can't test them all, better to build up integration points gradually.
 */
public final class ReactiveEntityManagerFactoryBuilder extends EntityManagerFactoryBuilderImpl {

    public ReactiveEntityManagerFactoryBuilder(PersistenceUnitDescriptor persistenceUnitDescriptor, Map integration) {
        super( persistenceUnitDescriptor, integration );
    }

    @Override
    protected StandardServiceRegistryBuilder getStandardServiceRegistryBuilder(BootstrapServiceRegistry bsr) {
        return ReactiveServiceRegistryBuilder.forJpa( bsr );
    }

}
