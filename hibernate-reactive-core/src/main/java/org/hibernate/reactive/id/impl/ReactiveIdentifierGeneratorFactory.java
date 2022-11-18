/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import java.lang.invoke.MethodHandles;
import java.util.Properties;

import org.hibernate.MappingException;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.id.Configurable;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.id.SelectGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.id.enhanced.SequenceStructure;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.id.enhanced.TableStructure;
import org.hibernate.id.factory.internal.StandardIdentifierGeneratorFactory;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

public class ReactiveIdentifierGeneratorFactory extends StandardIdentifierGeneratorFactory {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ServiceRegistry serviceRegistry;

	public ReactiveIdentifierGeneratorFactory(ServiceRegistry serviceRegistry) {
		super( serviceRegistry );
		this.serviceRegistry = serviceRegistry;
	}

	public ReactiveIdentifierGeneratorFactory(ServiceRegistry serviceRegistry, boolean ignoreBeanContainer) {
		super( serviceRegistry, ignoreBeanContainer );
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public IdentifierGenerator createIdentifierGenerator(String strategy, Type type, Properties config) {
		final IdentifierGenerator generator = createIdentifier( strategy, type, config );
		if ( generator instanceof IdentifierGenerator ) {
			return augmentWithReactiveGenerator( generator, type, config );
		}

		if ( generator instanceof ReactiveIdentifierGenerator) {
			return  new ReactiveGeneratorWrapper<>( (ReactiveIdentifierGenerator<?>) generator );
		}

		final String entityName = config.getProperty( IdentifierGenerator.ENTITY_NAME );
		throw new MappingException( String.format( "Not an id generator [entity-name=%s]", entityName ) );
	}

	/**
	 * @see StandardIdentifierGeneratorFactory#createIdentifierGenerator(String, Type, Properties)
	 */
	private IdentifierGenerator createIdentifier(String strategy, Type type, Properties config) {
		try {
			final Class<? extends IdentifierGenerator> clazz = getIdentifierGeneratorClass( strategy );
			final IdentifierGenerator generator = clazz.getDeclaredConstructor().newInstance();
			generator.configure( type, config, serviceRegistry );
			return generator;
		}
		catch (Exception e) {
			final String entityName = config.getProperty( IdentifierGenerator.ENTITY_NAME );
			throw new MappingException( String.format( "Could not instantiate id generator [entity-name=%s]", entityName ), e );
		}
	}

	public IdentifierGenerator augmentWithReactiveGenerator(IdentifierGenerator generator, Type type, Properties params) {
		return augmentWithReactiveGenerator( serviceRegistry, generator, type, params );
	}

	public static IdentifierGenerator augmentWithReactiveGenerator(ServiceRegistry serviceRegistry, IdentifierGenerator generator, Type type, Properties params) {
		ReactiveIdentifierGenerator<?> reactiveGenerator;
		if ( generator instanceof SequenceStyleGenerator ) {
			DatabaseStructure structure = ( (SequenceStyleGenerator) generator ).getDatabaseStructure();
			if ( structure instanceof TableStructure ) {
				reactiveGenerator = new EmulatedSequenceReactiveIdentifierGenerator();
			}
			else if ( structure instanceof SequenceStructure ) {
				reactiveGenerator = new SequenceReactiveIdentifierGenerator();
			}
			else {
				throw LOG.unknownStructureType();
			}
		}
		else if ( generator instanceof TableGenerator ) {
			reactiveGenerator = new TableReactiveIdentifierGenerator();
		}
		else if ( generator instanceof SelectGenerator ) {
			throw LOG.selectGeneratorIsNotSupportedInHibernateReactive();
		}
		else {
			//nothing to do
			return generator;
		}

		//this is not the way ORM does this: instead it passes a
		//SqlStringGenerationContext to IdentifierGenerator.initialize()
		ConfigurationService cs = serviceRegistry.getService( ConfigurationService.class );
		if ( !params.containsKey( PersistentIdentifierGenerator.SCHEMA ) ) {
			String schema = cs.getSetting( Settings.DEFAULT_SCHEMA, StandardConverters.STRING );
			if ( schema != null ) {
				params.put( PersistentIdentifierGenerator.SCHEMA, schema );
			}
		}
		if ( !params.containsKey( PersistentIdentifierGenerator.CATALOG ) ) {
			String catalog = cs.getSetting( Settings.DEFAULT_CATALOG, StandardConverters.STRING );
			if ( catalog != null ) {
				params.put( PersistentIdentifierGenerator.CATALOG, catalog );
			}
		}

		( (Configurable) reactiveGenerator ).configure( type, params, serviceRegistry );
		return new ReactiveGeneratorWrapper<>( reactiveGenerator, generator );
	}

}
