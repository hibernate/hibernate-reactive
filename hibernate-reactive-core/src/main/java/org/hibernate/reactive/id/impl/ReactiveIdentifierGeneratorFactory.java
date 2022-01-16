/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.id.Configurable;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.id.SelectGenerator;
import org.hibernate.id.SequenceGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.id.enhanced.SequenceStructure;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.id.enhanced.TableStructure;
import org.hibernate.id.factory.internal.DefaultIdentifierGeneratorFactory;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.type.Type;

import java.lang.invoke.MethodHandles;
import java.util.Properties;

/**
 * @author Gavin King
 */
public class ReactiveIdentifierGeneratorFactory extends DefaultIdentifierGeneratorFactory {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private ServiceRegistryImplementor serviceRegistry;

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		super.injectServices(serviceRegistry);
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public IdentifierGenerator createIdentifierGenerator(String strategy, Type type, Properties config) {
		Object generator;
		try {
			Class<?> clazz = getIdentifierGeneratorClass( strategy );
			generator = clazz.getDeclaredConstructor().newInstance();
		}
		catch ( Exception e ) {
			final String entityName = config.getProperty( IdentifierGenerator.ENTITY_NAME );
			throw new MappingException( String.format( "Could not instantiate id generator [entity-name=%s]", entityName ), e );
		}

		if ( generator instanceof Configurable) {
			( (Configurable) generator ).configure( type, config, serviceRegistry );
		}

		IdentifierGenerator result;
		if ( generator instanceof IdentifierGenerator ) {
			result = augmentWithReactiveGenerator( (IdentifierGenerator) generator, type, config, serviceRegistry );
		}
		else if ( generator instanceof ReactiveIdentifierGenerator) {
			result = new ReactiveGeneratorWrapper<>( (ReactiveIdentifierGenerator<?>) generator );
		}
		else {
			final String entityName = config.getProperty( IdentifierGenerator.ENTITY_NAME );
			throw new MappingException( String.format( "Not an id generator [entity-name=%s]", entityName ) );
		}

		result.configure( type, config, serviceRegistry );

		return result;
	}

	private static IdentifierGenerator augmentWithReactiveGenerator(IdentifierGenerator generator,
																	Type type,
																	Properties params,
																	ServiceRegistryImplementor serviceRegistry) {
		ReactiveIdentifierGenerator<?> reactiveGenerator;
		if (generator instanceof SequenceStyleGenerator) {
			DatabaseStructure structure = ((SequenceStyleGenerator) generator).getDatabaseStructure();
			if (structure instanceof TableStructure) {
				reactiveGenerator = new EmulatedSequenceReactiveIdentifierGenerator();
			}
			else if (structure instanceof SequenceStructure) {
				reactiveGenerator = new SequenceReactiveIdentifierGenerator();
			}
			else {
				throw LOG.unknownStructureType();
			}
		}
		else if (generator instanceof TableGenerator) {
			reactiveGenerator = new TableReactiveIdentifierGenerator();
		}
		else if (generator instanceof SequenceGenerator) {
			reactiveGenerator = new SequenceReactiveIdentifierGenerator();
		}
		else if (generator instanceof SelectGenerator) {
			throw LOG.selectGeneratorIsNotSupportedInHibernateReactive();
		}
		else {
			//nothing to do
			return generator;
		}

		//this is not the way ORM does this: instead it passes a
		//SqlStringGenerationContext to IdentifierGenerator.initialize()
		ConfigurationService cs = serviceRegistry.getService(ConfigurationService.class);
		if ( !params.containsKey(PersistentIdentifierGenerator.SCHEMA) ) {
			String schema = cs.getSetting(Settings.DEFAULT_SCHEMA, StandardConverters.STRING);
			if ( schema!=null ) {
				params.put( PersistentIdentifierGenerator.SCHEMA, schema );
			}
		}
		if ( !params.containsKey(PersistentIdentifierGenerator.CATALOG) ) {
			String catalog = cs.getSetting(Settings.DEFAULT_CATALOG, StandardConverters.STRING);
			if ( catalog!=null ) {
				params.put( PersistentIdentifierGenerator.CATALOG, catalog );
			}
		}

		((Configurable) reactiveGenerator).configure( type, params, serviceRegistry );

		return new ReactiveGeneratorWrapper<>( reactiveGenerator, generator );
	}

}
