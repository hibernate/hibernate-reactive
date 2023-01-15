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
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.generator.OnExecutionGenerator;
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

	@Override
	public Generator createIdentifierGenerator(String strategy, Type type, Properties config) {
		final Generator generator = super.createIdentifierGenerator( strategy, type, config );
		//FIXME: Not sure why we need all these instanceof
		if ( generator instanceof BeforeExecutionGenerator ) {
			return augmentWithReactiveGenerator( generator, type, config );
		}

		if ( generator instanceof OnExecutionGenerator ) {
			return augmentWithReactiveGenerator( generator, type, config );
		}

		if ( generator instanceof ReactiveIdentifierGenerator ) {
			return new ReactiveGeneratorWrapper( (ReactiveIdentifierGenerator) generator, type.getReturnedClass() );
		}

		final String entityName = config.getProperty( IdentifierGenerator.ENTITY_NAME );
		throw new MappingException( String.format( "Not an id generator [entity-name=%s]", entityName ) );
	}

	public Generator augmentWithReactiveGenerator(Generator generator, Type type, Properties params) {
		return augmentWithReactiveGenerator( serviceRegistry, generator, type, params );
	}

	public static Generator augmentWithReactiveGenerator(ServiceRegistry serviceRegistry, Generator generator, Type type, Properties params) {
		ReactiveIdentifierGenerator<?> reactiveGenerator;
		if ( generator instanceof SequenceStyleGenerator ) {
			DatabaseStructure structure = ( (SequenceStyleGenerator) generator ).getDatabaseStructure();
			if ( structure instanceof TableStructure ) {
				reactiveGenerator = new EmulatedSequenceReactiveIdentifierGenerator();
			}
			else if ( structure instanceof SequenceStructure ) {
				reactiveGenerator = new ReactiveSequenceIdentifierGenerator();
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
		return new ReactiveGeneratorWrapper( reactiveGenerator, (IdentifierGenerator) generator, type.getReturnedClass() );
	}

}
