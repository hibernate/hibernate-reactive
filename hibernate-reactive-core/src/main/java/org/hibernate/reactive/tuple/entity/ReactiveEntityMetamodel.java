/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.tuple.entity;

import java.util.function.Function;


import org.hibernate.generator.Generator;
import org.hibernate.generator.GeneratorCreationContext;
import org.hibernate.id.Configurable;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.SelectGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.id.enhanced.SequenceStructure;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.id.enhanced.TableStructure;
import org.hibernate.mapping.GeneratorCreator;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.id.impl.EmulatedSequenceReactiveIdentifierGenerator;
import org.hibernate.reactive.id.impl.ReactiveGeneratorWrapper;
import org.hibernate.reactive.id.impl.ReactiveSequenceIdentifierGenerator;
import org.hibernate.reactive.id.impl.TableReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.tuple.entity.EntityMetamodel;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

public class ReactiveEntityMetamodel extends EntityMetamodel {

	private static final Log LOG = make( Log.class, lookup() );

	public ReactiveEntityMetamodel(
			PersistentClass persistentClass,
			EntityPersister persister,
			RuntimeModelCreationContext creationContext) {
		this(
				persistentClass,
				persister,
				creationContext,
				s -> buildIdGenerator( s, persistentClass, creationContext )
		);
	}

	public ReactiveEntityMetamodel(
			PersistentClass persistentClass,
			EntityPersister persister,
			RuntimeModelCreationContext creationContext,
			Function<String, Generator> generatorSupplier) {
		super( persistentClass, persister, creationContext, generatorSupplier );
	}

	private static Generator buildIdGenerator(
			String rootName,
			PersistentClass persistentClass,
			RuntimeModelCreationContext creationContext) {
		final Generator existing = creationContext.getGenerators().get( rootName );
		if ( existing != null ) {
			return existing;
		}
		else {
			SimpleValue identifier = (SimpleValue) persistentClass.getIdentifier();
			GeneratorCreator customIdGeneratorCreator = identifier.getCustomIdGeneratorCreator();
			identifier.setCustomIdGeneratorCreator( context -> {
				Generator generator = customIdGeneratorCreator.createGenerator( context );
				return augmentWithReactiveGenerator( generator, context, creationContext );
			} );
			final Generator idgenerator = identifier
					// returns the cached Generator if it was already created
					.createGenerator(
							creationContext.getDialect(),
							persistentClass.getRootClass(),
							persistentClass.getIdentifierProperty(),
							creationContext.getGeneratorSettings()
					);
			creationContext.getGenerators().put( rootName, idgenerator );
			return idgenerator;
		}
	}

	public static Generator augmentWithReactiveGenerator(
			Generator generator,
			GeneratorCreationContext creationContext,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		if ( generator instanceof SequenceStyleGenerator ) {
			final DatabaseStructure structure = ( (SequenceStyleGenerator) generator ).getDatabaseStructure();
			if ( structure instanceof TableStructure ) {
				return initialize( (IdentifierGenerator) generator, new EmulatedSequenceReactiveIdentifierGenerator( (TableStructure) structure, runtimeModelCreationContext ), creationContext );
			}
			if ( structure instanceof SequenceStructure ) {
				return initialize( (IdentifierGenerator) generator, new ReactiveSequenceIdentifierGenerator( structure, runtimeModelCreationContext ), creationContext );
			}
			throw LOG.unknownStructureType();
		}
		if ( generator instanceof TableGenerator ) {
			return initialize(
					(IdentifierGenerator) generator,
					new TableReactiveIdentifierGenerator( (TableGenerator) generator, runtimeModelCreationContext ),
					creationContext
			);
		}
		if ( generator instanceof SelectGenerator ) {
			throw LOG.selectGeneratorIsNotSupportedInHibernateReactive();
		}
		//nothing to do
		return generator;
	}

	private static Generator initialize(
			IdentifierGenerator idGenerator,
			ReactiveIdentifierGenerator<?> reactiveIdGenerator,
			GeneratorCreationContext creationContext) {
		( (Configurable) reactiveIdGenerator ).initialize( creationContext.getSqlStringGenerationContext() );
		return new ReactiveGeneratorWrapper( reactiveIdGenerator, idGenerator );
	}
}
