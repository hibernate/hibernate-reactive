/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider;

import org.hibernate.jpa.HibernatePersistenceProvider;
import org.hibernate.jpa.boot.internal.ParsedPersistenceXmlDescriptor;
import org.hibernate.jpa.boot.internal.PersistenceUnitInfoDescriptor;
import org.hibernate.jpa.boot.internal.PersistenceXmlParser;
import org.hibernate.jpa.boot.spi.EntityManagerFactoryBuilder;
import org.hibernate.jpa.boot.spi.PersistenceUnitDescriptor;
import org.hibernate.jpa.internal.util.PersistenceUtilHelper;

import org.hibernate.reactive.provider.impl.ReactiveEntityManagerFactoryBuilder;
import org.hibernate.reactive.provider.impl.ReactiveProviderChecker;
import org.jboss.logging.Logger;

import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import javax.persistence.spi.LoadState;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * JPA {@link PersistenceProvider} implementation specific to Hibernate Reactive
 * All specific configurations are set transparently for the user.
 */
public class ReactivePersistenceProvider implements PersistenceProvider {

	private static final Logger log = Logger.getLogger( HibernatePersistenceProvider.class );
	private final PersistenceUtilHelper.MetadataCache cache = new PersistenceUtilHelper.MetadataCache();

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Note: per-spec, the values passed as {@code properties} override values found in {@code persistence.xml}
	 */
	@Override
	public EntityManagerFactory createEntityManagerFactory(String persistenceUnitName, Map properties) {
		log.tracef( "Starting createEntityManagerFactory for persistenceUnitName %s", persistenceUnitName );
		final Map immutableProperties = immutable( properties );
		final EntityManagerFactoryBuilder builder = getEntityManagerFactoryBuilderOrNull( persistenceUnitName, immutableProperties );
		if ( builder == null ) {
			log.trace( "Could not obtain matching EntityManagerFactoryBuilder, returning null" );
			return null;
		}
		else {
			return builder.build();
		}
	}

	protected EntityManagerFactoryBuilder getEntityManagerFactoryBuilderOrNull(String persistenceUnitName, Map properties) {
		log.tracef( "Attempting to obtain correct EntityManagerFactoryBuilder for persistenceUnitName : %s", persistenceUnitName );

		final List<ParsedPersistenceXmlDescriptor> units;
		try {
			units = PersistenceXmlParser.locatePersistenceUnits( properties );
		}
		catch (Exception e) {
			log.debug( "Unable to locate persistence units", e );
			throw new PersistenceException( "Unable to locate persistence units", e );
		}

		log.debugf( "Located and parsed %s persistence units; checking each", units.size() );

		if ( persistenceUnitName == null && units.size() > 1 ) {
			// no persistence-unit name to look for was given and we found multiple persistence-units
			throw new PersistenceException( "No name provided and multiple persistence units found" );
		}

		for ( ParsedPersistenceXmlDescriptor persistenceUnit : units ) {
			log.debugf(
					"Checking persistence-unit [name=%s, explicit-provider=%s] against incoming persistence unit name [%s]",
					persistenceUnit.getName(),
					persistenceUnit.getProviderClassName(),
					persistenceUnitName
			);

			final boolean matches = persistenceUnitName == null || persistenceUnit.getName().equals( persistenceUnitName );
			if ( !matches ) {
				log.debug( "Excluding from consideration due to name mis-match" );
				continue;
			}

			// See if we (Hibernate Reactive) are the persistence provider
			if ( ! ReactiveProviderChecker.isProvider( persistenceUnit, properties ) ) {
				log.debug( "Excluding from consideration due to provider mis-match" );
				continue;
			}

			return getEntityManagerFactoryBuilder( persistenceUnit, properties );
		}

		log.debug( "Found no matching persistence units" );
		return null;
	}

	@SuppressWarnings("unchecked")
	private static Map immutable(Map properties) {
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

	protected EntityManagerFactoryBuilder getEntityManagerFactoryBuilder(PersistenceUnitInfo info, Map integration) {
		return getEntityManagerFactoryBuilder( new PersistenceUnitInfoDescriptor( info ), integration );
	}

	protected EntityManagerFactoryBuilder getEntityManagerFactoryBuilder(PersistenceUnitDescriptor persistenceUnitDescriptor, Map integration) {
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
			return PersistenceUtilHelper.isLoaded(o);
		}
	};

	@Override
	public ProviderUtil getProviderUtil() {
		return providerUtil;
	}

}
