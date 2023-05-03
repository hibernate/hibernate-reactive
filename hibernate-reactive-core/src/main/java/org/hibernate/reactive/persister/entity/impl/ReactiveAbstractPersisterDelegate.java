/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.FetchMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.CascadeStyle;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.generator.Generator;
import org.hibernate.id.IdentityGenerator;
import org.hibernate.loader.ast.spi.BatchLoaderFactory;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.EntityValuedModelPart;
import org.hibernate.metamodel.mapping.ManagedMappingType;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.metamodel.mapping.internal.GeneratedValuesProcessor;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationHelper;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.metamodel.model.domain.NavigableRole;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.property.access.spi.PropertyAccess;
import org.hibernate.query.named.NamedQueryMemento;
import org.hibernate.reactive.loader.ast.internal.ReactiveMultiIdEntityLoaderStandard;
import org.hibernate.reactive.loader.ast.internal.ReactiveSingleIdEntityLoaderProvidedQueryImpl;
import org.hibernate.reactive.loader.ast.internal.ReactiveSingleIdEntityLoaderStandardImpl;
import org.hibernate.reactive.loader.ast.internal.ReactiveSingleUniqueKeyEntityLoaderStandard;
import org.hibernate.reactive.loader.ast.spi.ReactiveMultiIdEntityLoader;
import org.hibernate.reactive.loader.ast.spi.ReactiveNaturalIdLoader;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleUniqueKeyEntityLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.metamodel.mapping.internal.ReactivePluralAttributeMapping;
import org.hibernate.reactive.metamodel.mapping.internal.ReactiveToOneAttributeMapping;
import org.hibernate.reactive.sql.results.internal.ReactiveEntityResultImpl;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.type.EntityType;

import static org.hibernate.pretty.MessageHelper.infoString;

/**
 * @see org.hibernate.persister.entity.AbstractEntityPersister
 */
public class ReactiveAbstractPersisterDelegate {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveSingleIdEntityLoader<Object> singleIdEntityLoader;
	private final ReactiveMultiIdEntityLoader<?> multiIdEntityLoader;

	private final EntityPersister entityDescriptor;

	private Map<SingularAttributeMapping, ReactiveSingleUniqueKeyEntityLoader<Object>> uniqueKeyLoadersNew;

	public ReactiveAbstractPersisterDelegate(
			final EntityPersister entityPersister,
			final PersistentClass persistentClass,
			final RuntimeModelCreationContext creationContext) {
		SessionFactoryImplementor factory = creationContext.getSessionFactory();
		singleIdEntityLoader = createReactiveSingleIdEntityLoader(
				entityPersister,
				persistentClass,
				creationContext,
				factory,
				entityPersister.getEntityName()
		);
		multiIdEntityLoader = new ReactiveMultiIdEntityLoaderStandard<>( entityPersister, persistentClass, factory );
		entityDescriptor = entityPersister;
	}

	public ReactiveSingleIdEntityLoader<Object> getSingleIdEntityLoader() {
		return singleIdEntityLoader;
	}


	public <T> DomainResult<T> createDomainResult(
			EntityValuedModelPart assemblerCreationState,
			NavigablePath navigablePath,
			TableGroup tableGroup,
			String resultVariable,
			DomainResultCreationState creationState) {
		final ReactiveEntityResultImpl entityResult = new ReactiveEntityResultImpl( navigablePath, assemblerCreationState, tableGroup, resultVariable );
		entityResult.afterInitialize( entityResult, creationState );
		//noinspection unchecked
		return entityResult;
	}

	/**
	 * @see org.hibernate.persister.entity.AbstractEntityPersister#multiLoad(Object[], EventSource, MultiIdLoadOptions)`
	 */
	public <K> CompletionStage<? extends List<?>> multiLoad(
			K[] ids,
			EventSource session,
			MultiIdLoadOptions loadOptions) {
		return multiIdEntityLoader.load( ids, loadOptions, session );
	}

	private static ReactiveSingleIdEntityLoader<Object> createReactiveSingleIdEntityLoader(
			EntityMappingType entityDescriptor,
			PersistentClass bootDescriptor,
			RuntimeModelCreationContext creationContext,
			SessionFactoryImplementor factory,
			String entityName) {
		int batchSize = batchSize( bootDescriptor, factory );
		if ( bootDescriptor.getLoaderName() != null ) {
			// We must resolve the named query on-demand through the boot model because it isn't initialized yet
			final NamedQueryMemento namedQueryMemento = factory.getQueryEngine().getNamedObjectRepository().resolve(
					factory,
					creationContext.getBootModel(),
					bootDescriptor.getLoaderName()
			);
			if ( namedQueryMemento == null ) {
				throw new IllegalArgumentException( "Could not resolve named load-query [" + entityName + "] : " + bootDescriptor.getLoaderName() );
			}
			return new ReactiveSingleIdEntityLoaderProvidedQueryImpl<>( entityDescriptor, namedQueryMemento );
		}

		if ( batchSize > 1 ) {
			return createBatchingIdEntityLoader( entityDescriptor, batchSize, factory );
		}

		return new ReactiveSingleIdEntityLoaderStandardImpl<>( entityDescriptor, factory );
	}

	private static ReactiveSingleIdEntityLoader<Object> createBatchingIdEntityLoader(
			EntityMappingType entityDescriptor,
			int domainBatchSize,
			SessionFactoryImplementor factory) {
		return (ReactiveSingleIdEntityLoader) factory.getServiceRegistry()
				.getService( BatchLoaderFactory.class )
				.createEntityBatchLoader( domainBatchSize, entityDescriptor, factory );
	}

	private static int batchSize(PersistentClass bootDescriptor, SessionFactoryImplementor factory) {
		int batchSize = bootDescriptor.getBatchSize();
		if ( batchSize == -1 ) {
			batchSize = factory.getSessionFactoryOptions().getDefaultBatchFetchSize();
		}
		return batchSize;
	}

	public CompletionStage<Void> processInsertGeneratedProperties(
			Object id,
			Object entity,
			Object[] state,
			GeneratedValuesProcessor processor,
			SharedSessionContractImplementor session,
			String entityName) {
		if ( processor == null ) {
			throw new UnsupportedOperationException( "Entity has no insert-generated properties - `" + entityName + "`" );
		}

		ReactiveGeneratedValuesProcessor reactiveGeneratedValuesProcessor = new ReactiveGeneratedValuesProcessor(
				processor.getSelectStatement(),
				processor.getGeneratedValuesToSelect(),
				processor.getJdbcParameters(),
				processor.getEntityDescriptor(),
				processor.getSessionFactory()
		);
		return reactiveGeneratedValuesProcessor.processGeneratedValues( id, entity, state, session );
	}

	public CompletionStage<Void> processUpdateGeneratedProperties(
			Object id,
			Object entity,
			Object[] state,
			GeneratedValuesProcessor processor,
			SharedSessionContractImplementor session,
			String entityName) {
		if ( processor == null ) {
			throw new UnsupportedOperationException( "Entity has no update-generated properties - `" + entityName + "`" );
		}

		ReactiveGeneratedValuesProcessor reactiveGeneratedValuesProcessor = new ReactiveGeneratedValuesProcessor(
				processor.getSelectStatement(),
				processor.getGeneratedValuesToSelect(),
				processor.getJdbcParameters(),
				processor.getEntityDescriptor(),
				processor.getSessionFactory()
		);
		return reactiveGeneratedValuesProcessor.processGeneratedValues( id, entity, state, session );
	}

	public Map<SingularAttributeMapping, ReactiveSingleUniqueKeyEntityLoader<Object>> getUniqueKeyLoadersNew() {
		return uniqueKeyLoadersNew;
	}

	protected ReactiveSingleUniqueKeyEntityLoader<Object> getReactiveUniqueKeyLoader(
			EntityPersister entityDescriptor,
			SingularAttributeMapping attribute) {
		if ( uniqueKeyLoadersNew == null ) {
			uniqueKeyLoadersNew = new IdentityHashMap<>();
		}
		return uniqueKeyLoadersNew.computeIfAbsent(
				attribute,
				key -> new ReactiveSingleUniqueKeyEntityLoaderStandard<>(
						entityDescriptor,
						key
				)
		);
	}

	public CompletionStage<Object> load(
			EntityPersister persister,
			Object id,
			Object optionalObject,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Fetching entity: {0}", MessageHelper.infoString( persister, id, persister.getFactory() ) );
		}
		return optionalObject == null
				? singleIdEntityLoader.load( id, lockOptions, readOnly, session )
				: singleIdEntityLoader.load( id, optionalObject, lockOptions, readOnly, session );
	}

	public Generator reactive(Generator generator) {
		return generator instanceof IdentityGenerator ? new ReactiveIdentityGenerator() : generator;
	}

	public CompletionStage<Object> loadEntityIdByNaturalId(
			Object[] orderedNaturalIdValues, LockOptions lockOptions, SharedSessionContractImplementor session) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracef( "Resolving natural-id [%s] to id : %s ",
						Arrays.asList( orderedNaturalIdValues ),
						infoString( entityDescriptor )
			);
		}

		return ( (ReactiveNaturalIdLoader) entityDescriptor.getNaturalIdLoader() ).resolveNaturalIdToId(
				orderedNaturalIdValues,
				session
		);
	}

	public AttributeMapping buildSingularAssociationAttributeMapping(
			String attrName,
			NavigableRole navigableRole,
			int stateArrayPosition,
			int fetchableIndex,
			Property bootProperty,
			ManagedMappingType declaringType,
			EntityPersister declaringEntityPersister,
			EntityType attrType,
			PropertyAccess propertyAccess,
			CascadeStyle cascadeStyle,
			MappingModelCreationProcess creationProcess) {
		return MappingModelCreationHelper
				.buildSingularAssociationAttributeMapping(
						attrName,
						navigableRole,
						stateArrayPosition,
						fetchableIndex,
						bootProperty,
						declaringType,
						declaringEntityPersister,
						attrType,
						propertyAccess,
						cascadeStyle,
						creationProcess,
						ReactiveToOneAttributeMapping::new
				);
	}

	public AttributeMapping buildPluralAttributeMapping(
			String attrName,
			int stateArrayPosition,
			int fetchableIndex,
			Property bootProperty,
			ManagedMappingType declaringType,
			PropertyAccess propertyAccess,
			CascadeStyle cascadeStyle,
			FetchMode fetchMode,
			MappingModelCreationProcess creationProcess) {
		return MappingModelCreationHelper
				.buildPluralAttributeMapping(
						attrName,
						stateArrayPosition,
						fetchableIndex,
						bootProperty,
						declaringType,
						propertyAccess,
						cascadeStyle,
						fetchMode,
						creationProcess,
						ReactivePluralAttributeMapping::new
				);
	}
}
