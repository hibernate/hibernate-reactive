/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.jpa.boot.internal.PersistenceUnitInfoDescriptor;
import org.hibernate.jpa.boot.spi.EntityManagerFactoryBuilder;
import org.hibernate.jpa.boot.spi.PersistenceUnitDescriptor;
import org.hibernate.jpa.boot.spi.PersistenceXmlParser;
import org.hibernate.jpa.internal.util.PersistenceUtilHelper;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.provider.impl.ReactiveEntityManagerFactoryBuilder;
import org.hibernate.reactive.provider.impl.ReactiveProviderChecker;

import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceConfiguration;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.spi.LoadState;
import jakarta.persistence.spi.PersistenceProvider;
import jakarta.persistence.spi.PersistenceUnitInfo;
import jakarta.persistence.spi.ProviderUtil;

/**
 * A JPA {@link PersistenceProvider} for Hibernate Reactive.
 *
 * @see org.hibernate.jpa.HibernatePersistenceProvider
 */
public class ReactivePersistenceProvider implements PersistenceProvider {

	private static final Log log = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final PersistenceUtilHelper.MetadataCache cache = new PersistenceUtilHelper.MetadataCache();

	@Override
	public EntityManagerFactory createEntityManagerFactory(PersistenceConfiguration persistenceConfiguration) {
		// Same as ORM
		throw log.notYetImplemented();
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Note: per-spec, the values passed as {@code properties} override values found in {@code persistence.xml}
	 */
	@Override
	public EntityManagerFactory createEntityManagerFactory(String persistenceUnitName, Map properties) {
		log.tracef( "Starting createEntityManagerFactory for persistenceUnitName %s", persistenceUnitName );
		final Map<?, ?> immutableProperties = immutable( properties );
		final EntityManagerFactoryBuilder builder = getEntityManagerFactoryBuilderOrNull( persistenceUnitName, immutableProperties );
		if ( builder == null ) {
			log.trace( "Could not obtain matching EntityManagerFactoryBuilder, returning null" );
			return null;
		}
		return builder.build();
	}

	protected EntityManagerFactoryBuilder getEntityManagerFactoryBuilderOrNull(String persistenceUnitName, Map<?, ?> properties) {
		log.tracef( "Attempting to obtain correct EntityManagerFactoryBuilder for persistenceUnitName : %s", persistenceUnitName );

		final Map<?,?> integration = immutable( properties );
		final Collection<PersistenceUnitDescriptor> units = locatePersistenceUnits( integration );

		log.debugf( "Located and parsed %s persistence units; checking each", units.size() );

		if ( persistenceUnitName == null && units.size() > 1 ) {
			// no persistence-unit name to look for was given and we found multiple persistence-units
			throw log.noNameProvidedAndMultiplePersistenceUnitsFound();
		}

		for ( PersistenceUnitDescriptor persistenceUnit : units ) {
			log.debugf(
					"Checking persistence-unit [name=%s, explicit-provider=%s] against incoming persistence unit name [%s]",
					persistenceUnit.getName(),
					persistenceUnit.getProviderClassName(),
					persistenceUnitName
			);

			final boolean matches = persistenceUnitName == null || persistenceUnit.getName()
					.equals( persistenceUnitName );
			if ( !matches ) {
				log.debug( "Excluding from consideration due to name mis-match" );
				continue;
			}

			// See if we (Hibernate Reactive) are the persistence provider
			if ( !ReactiveProviderChecker.isProvider( persistenceUnit, properties ) ) {
				log.debug( "Excluding from consideration due to provider mis-match" );
				continue;
			}

			return getEntityManagerFactoryBuilder( persistenceUnit, properties );
		}

		log.debug( "Found no matching persistence units" );
		return null;
	}

	// Check before changing: may be overridden in Quarkus
	// This is basically a copy and paste of the method in HibernatePersistenceProvider
	protected Collection<PersistenceUnitDescriptor> locatePersistenceUnits(Map<?, ?> integration) {
		try {
			var parser = PersistenceXmlParser.create( integration, null, null );
			final List<URL> xmlUrls = parser.getClassLoaderService().locateResources( "META-INF/persistence.xml" );
			if ( xmlUrls.isEmpty() ) {
				log.unableToFindPersistenceXmlInClasspath();
				return List.of();
			}
			return parser.parse( xmlUrls ).values();
		}
		catch (Exception e) {
			log.debug( "Unable to locate persistence units", e );
			throw new PersistenceException( "Unable to locate persistence units", e );
		}
	}

	private static Map<?, ?> immutable(Map<?, ?> properties) {
		return properties == null ? Collections.emptyMap() : Collections.unmodifiableMap( properties );
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Note: per-spec, the values passed as {@code properties} override values found in {@link PersistenceUnitInfo}
	 */
	@Override
	public EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info, Map properties) {
		log.tracef( "Starting createContainerEntityManagerFactory : %s", info.getPersistenceUnitName() );

		return getEntityManagerFactoryBuilder( info, properties ).build();
	}

	@Override
	public void generateSchema(PersistenceUnitInfo info, Map map) {
		log.tracef( "Starting generateSchema : PUI.name=%s", info.getPersistenceUnitName() );

		final EntityManagerFactoryBuilder builder = getEntityManagerFactoryBuilder( info, map );
		builder.generateSchema();
	}

	@Override
	public boolean generateSchema(String persistenceUnitName, Map map) {
		log.tracef( "Starting generateSchema for persistenceUnitName %s", persistenceUnitName );

		final EntityManagerFactoryBuilder builder = getEntityManagerFactoryBuilderOrNull( persistenceUnitName, map );
		if ( builder == null ) {
			log.trace( "Could not obtain matching EntityManagerFactoryBuilder, returning false" );
			return false;
		}
		builder.generateSchema();
		return true;
	}

	protected EntityManagerFactoryBuilder getEntityManagerFactoryBuilder(PersistenceUnitInfo info, Map<?, ?> integration) {
		return getEntityManagerFactoryBuilder( new PersistenceUnitInfoDescriptor( info ), integration );
	}

	protected EntityManagerFactoryBuilder getEntityManagerFactoryBuilder(PersistenceUnitDescriptor persistenceUnitDescriptor, Map<?, ?> integration) {
		return new ReactiveEntityManagerFactoryBuilder( persistenceUnitDescriptor, integration );
	}

	private final ProviderUtil providerUtil = new ProviderUtil() {
		@Override
		public LoadState isLoadedWithoutReference(Object proxy, String property) {
			return PersistenceUtilHelper.isLoadedWithoutReference( proxy, property, cache );
		}

		@Override
		public LoadState isLoadedWithReference(Object proxy, String property) {
			return PersistenceUtilHelper.isLoadedWithReference( proxy, property, cache );
		}

		@Override
		public LoadState isLoaded(Object o) {
			return PersistenceUtilHelper.getLoadState( o );
		}
	};

	@Override
	public ProviderUtil getProviderUtil() {
		return providerUtil;
	}

}
