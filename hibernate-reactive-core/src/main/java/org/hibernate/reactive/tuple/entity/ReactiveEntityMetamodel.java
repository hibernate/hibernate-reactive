/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.tuple.entity;

import java.util.Set;
import java.util.function.Function;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.bytecode.spi.BytecodeEnhancementMetadata;
import org.hibernate.generator.Generator;
import org.hibernate.generator.GeneratorCreationContext;
import org.hibernate.id.CompositeNestedGeneratedValueGenerator;
import org.hibernate.id.Configurable;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.SelectGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.id.enhanced.SequenceStructure;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.id.enhanced.TableStructure;
import org.hibernate.mapping.GeneratorSettings;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.RootClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.mapping.Value;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.reactive.bythecode.spi.ReactiveBytecodeEnhancementMetadataPojoImplAdapter;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.id.impl.EmulatedSequenceReactiveIdentifierGenerator;
import org.hibernate.reactive.id.impl.ReactiveCompositeNestedGeneratedValueGenerator;
import org.hibernate.reactive.id.impl.ReactiveGeneratorWrapper;
import org.hibernate.reactive.id.impl.ReactiveSequenceIdentifierGenerator;
import org.hibernate.reactive.id.impl.TableReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.CompositeType;
import org.hibernate.type.Type;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * @deprecated No Longer used
 */
@Deprecated(since = "4.2", forRemoval = true)
public class ReactiveEntityMetamodel extends EntityMetamodel {

	private static final Log LOG = make( Log.class, lookup() );

	public ReactiveEntityMetamodel(
			PersistentClass persistentClass,
			RuntimeModelCreationContext creationContext) {
		this(
				persistentClass,
				creationContext,
				s -> buildIdGenerator( s, persistentClass, creationContext )
		);
	}

	public ReactiveEntityMetamodel(
			PersistentClass persistentClass,
			RuntimeModelCreationContext creationContext,
			Function<String, Generator> generatorSupplier) {
		super( persistentClass, creationContext, generatorSupplier );
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
			final SimpleValue identifier = (SimpleValue) persistentClass.getIdentifier();
			final Generator idgenerator = augmentWithReactiveGenerator(
					identifier.createGenerator(
							creationContext.getDialect(),
							persistentClass.getRootClass(),
							persistentClass.getIdentifierProperty(),
							creationContext.getGeneratorSettings()
					),
					new IdGeneratorCreationContext(
							persistentClass.getRootClass(),
							persistentClass.getIdentifierProperty(),
							creationContext.getGeneratorSettings(),
							identifier,
							creationContext
					),
					creationContext );
			creationContext.getGenerators().put( rootName, idgenerator );
			return idgenerator;
		}
	}

	public static Generator augmentWithReactiveGenerator(
			Generator generator,
			GeneratorCreationContext creationContext,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		if ( generator instanceof SequenceStyleGenerator sequenceStyleGenerator) {
			final DatabaseStructure structure = sequenceStyleGenerator.getDatabaseStructure();
			if ( structure instanceof TableStructure ) {
				return initialize( (IdentifierGenerator) generator, new EmulatedSequenceReactiveIdentifierGenerator( (TableStructure) structure, runtimeModelCreationContext ), creationContext );
			}
			if ( structure instanceof SequenceStructure ) {
				return initialize( (IdentifierGenerator) generator, new ReactiveSequenceIdentifierGenerator( structure, runtimeModelCreationContext ), creationContext );
			}
			throw LOG.unknownStructureType();
		}
		if ( generator instanceof TableGenerator tableGenerator ) {
			return initialize(
					(IdentifierGenerator) generator,
					new TableReactiveIdentifierGenerator( tableGenerator, runtimeModelCreationContext ),
					creationContext
			);
		}
		if ( generator instanceof SelectGenerator ) {
			throw LOG.selectGeneratorIsNotSupportedInHibernateReactive();
		}
		if ( generator instanceof CompositeNestedGeneratedValueGenerator compositeNestedGeneratedValueGenerator ) {
			final ReactiveCompositeNestedGeneratedValueGenerator reactiveCompositeNestedGeneratedValueGenerator = new ReactiveCompositeNestedGeneratedValueGenerator(
					compositeNestedGeneratedValueGenerator,
					creationContext,
					runtimeModelCreationContext
			);
			return initialize(
					(IdentifierGenerator) generator,
					reactiveCompositeNestedGeneratedValueGenerator,
					creationContext
			);
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

	private record IdGeneratorCreationContext(
			RootClass rootClass,
			Property property,
			GeneratorSettings defaults,
			SimpleValue identifier,
			RuntimeModelCreationContext buildingContext) implements GeneratorCreationContext {

		@Override
		public Database getDatabase() {
			return buildingContext.getBootModel().getDatabase();
		}

		@Override
		public ServiceRegistry getServiceRegistry() {
			return buildingContext.getBootstrapContext().getServiceRegistry();
		}

		@Override
		public SqlStringGenerationContext getSqlStringGenerationContext() {
			return defaults.getSqlStringGenerationContext();
		}

		@Override
		public String getDefaultCatalog() {
			return defaults.getDefaultCatalog();
		}

		@Override
		public String getDefaultSchema() {
			return defaults.getDefaultSchema();
		}

		@Override
		public RootClass getRootClass() {
			return rootClass;
		}

		@Override
		public PersistentClass getPersistentClass() {
			return rootClass;
		}

		@Override
		public Property getProperty() {
			return property;
		}

		@Override
		public Value getValue() {
			return identifier;
		}

		@Override
		public Type getType() {
			return identifier.getType();
		}
	}

	@Override
	protected BytecodeEnhancementMetadata getBytecodeEnhancementMetadataPojo(PersistentClass persistentClass, RuntimeModelCreationContext creationContext, Set<String> idAttributeNames, CompositeType nonAggregatedCidMapper, boolean collectionsInDefaultFetchGroupEnabled) {
		return ReactiveBytecodeEnhancementMetadataPojoImplAdapter.from(
				persistentClass,
				idAttributeNames,
				nonAggregatedCidMapper,
				collectionsInDefaultFetchGroupEnabled,
				creationContext.getMetadata()
		);
	}

}
