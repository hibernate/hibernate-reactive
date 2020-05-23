package org.hibernate.reactive.id.impl;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.id.Configurable;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.SelectGenerator;
import org.hibernate.id.SequenceGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.id.enhanced.SequenceStructure;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.id.enhanced.TableStructure;
import org.hibernate.id.factory.internal.DefaultIdentifierGeneratorFactory;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.type.Type;

import java.util.Properties;

/**
 * @author Gavin King
 */
public class ReactiveIdentifierGeneratorFactory extends DefaultIdentifierGeneratorFactory {

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

		if ( result instanceof Configurable) {
			( (Configurable) result ).configure( type, config, serviceRegistry );
		}

		return result;
	}

	private static IdentifierGenerator augmentWithReactiveGenerator(IdentifierGenerator generator, Type type, Properties params, ServiceRegistryImplementor serviceRegistry) {
		ReactiveIdentifierGenerator<?> reactiveGenerator;
		if (generator instanceof SequenceStyleGenerator) {
			DatabaseStructure structure = ((SequenceStyleGenerator) generator).getDatabaseStructure();
			if (structure instanceof TableStructure) {
				reactiveGenerator = new TableReactiveIdentifierGenerator(true);
			}
			else if (structure instanceof SequenceStructure) {
				reactiveGenerator = new SequenceReactiveIdentifierGenerator();
			}
			else {
				throw new IllegalStateException("unknown structure type");
			}
		}
		else if (generator instanceof TableGenerator) {
			reactiveGenerator = new TableReactiveIdentifierGenerator(false);
		}
		else if (generator instanceof SequenceGenerator) {
			reactiveGenerator = new SequenceReactiveIdentifierGenerator();
		}
		else if (generator instanceof SelectGenerator) {
			//TODO: this is easy to fix!
			throw new HibernateException("SelectGenerator is not yet supported in Hibernate Reactive");
		}
		else {
			//nothing to do
			return generator;
		}

		((Configurable) reactiveGenerator).configure( type, params, serviceRegistry );

		return new ReactiveGeneratorWrapper<>( reactiveGenerator, generator );
	}

}
