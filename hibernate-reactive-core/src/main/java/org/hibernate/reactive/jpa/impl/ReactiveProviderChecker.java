/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.jpa.impl;

import java.util.Map;

import org.hibernate.cfg.AvailableSettings;

import org.hibernate.jpa.boot.spi.PersistenceUnitDescriptor;
import org.jboss.logging.Logger;

/**
 * Helper for handling checks to see whether ReactivePersistenceProvider is the requested
 * {@link javax.persistence.spi.PersistenceProvider}
 *
 * Copied and adapted from org.hibernate.jpa.boot.spi.ProviderChecker
 */
public final class ReactiveProviderChecker {

    private static final Logger log = Logger.getLogger( org.hibernate.jpa.boot.spi.ProviderChecker.class );
    private static final String IMPLEMENTATION_NAME = ReactivePersistenceProvider.class.getName();

    /**
     * Does the descriptor and/or integration request Hibernate as the
     * {@link javax.persistence.spi.PersistenceProvider}?  Note that in the case of no requested provider being named
     * we assume we are the provider (the calls got to us somehow...)
     *
     * @param persistenceUnit The {@code <persistence-unit/>} descriptor.
     * @param integration The integration values.
     *
     * @return {@code true} if Hibernate should be the provider; {@code false} otherwise.
     */
    public static boolean isProvider(PersistenceUnitDescriptor persistenceUnit, Map integration) {
        // See if we (Hibernate) are the persistence provider
        return hibernateProviderNamesContain( extractRequestedProviderName( persistenceUnit, integration ) );
    }

    /**
     * Is the requested provider name one of the recognized Hibernate provider names?
     *
     * @param requestedProviderName The requested provider name to check against the recognized Hibernate names.
     *
     * @return {@code true} if Hibernate should be the provider; {@code false} otherwise.
     */
    public static boolean hibernateProviderNamesContain(String requestedProviderName) {
        if ( requestedProviderName == null ) {
            return false;
        }
        log.tracef(
                "Checking requested PersistenceProvider name [%s] against Hibernate provider names",
                requestedProviderName
        );
        return IMPLEMENTATION_NAME.equalsIgnoreCase( requestedProviderName );
    }

    /**
     * Extract the requested persistence provider name using the algorithm Hibernate uses.  Namely, a provider named
     * in the 'integration' map (under the key '{@value AvailableSettings#JPA_PERSISTENCE_PROVIDER}') is preferred, as per-spec, over
     * value specified in persistence unit.
     *
     * @param persistenceUnit The {@code <persistence-unit/>} descriptor.
     * @param integration The integration values.
     *
     * @return The extracted provider name, or {@code null} if none found.
     */
    public static String extractRequestedProviderName(PersistenceUnitDescriptor persistenceUnit, Map integration) {
        final String integrationProviderName = extractProviderName( integration );
        if ( integrationProviderName != null ) {
            log.debugf( "Integration provided explicit PersistenceProvider [%s]", integrationProviderName );
            return integrationProviderName;
        }

        final String persistenceUnitRequestedProvider = extractProviderName( persistenceUnit );
        if ( persistenceUnitRequestedProvider != null ) {
            log.debugf(
                    "Persistence-unit [%s] requested PersistenceProvider [%s]",
                    persistenceUnit.getName(),
                    persistenceUnitRequestedProvider
            );
            return persistenceUnitRequestedProvider;
        }

        // NOTE : if no provider requested we assume we are NOT the provider, leaving the responsibility to Hibernate ORM
        // (the classical, blocking version)
        log.debug( "No PersistenceProvider explicitly requested" );
        return "";
    }

    private static String extractProviderName(Map integration) {
        if ( integration == null ) {
            return null;
        }
        final String setting = (String) integration.get( AvailableSettings.JPA_PERSISTENCE_PROVIDER );
        return setting == null ? null : setting.trim();
    }

    private static String extractProviderName(PersistenceUnitDescriptor persistenceUnit) {
        final String persistenceUnitRequestedProvider = persistenceUnit.getProviderClassName();
        return persistenceUnitRequestedProvider == null ? null : persistenceUnitRequestedProvider.trim();
    }

    private ReactiveProviderChecker() {
    }
}
