/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.FetchMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.bytecode.spi.BytecodeEnhancementMetadata;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.CascadeStyle;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.Generator;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.loader.ast.spi.MultiIdEntityLoader;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.loader.ast.spi.SingleIdEntityLoader;
import org.hibernate.loader.ast.spi.SingleUniqueKeyEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.ManagedMappingType;
import org.hibernate.metamodel.mapping.NaturalIdMapping;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.metamodel.model.domain.NavigableRole;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.SingleTableEntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.persister.entity.mutation.UpdateCoordinator;
import org.hibernate.property.access.spi.PropertyAccess;
import org.hibernate.reactive.bythecode.spi.ReactiveBytecodeEnhancementMetadataPojoImplAdapter;
import org.hibernate.reactive.loader.ast.internal.ReactiveSingleIdArrayLoadPlan;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleUniqueKeyEntityLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.metamodel.mapping.internal.ReactiveRuntimeModelCreationContext;
import org.hibernate.reactive.persister.entity.mutation.ReactiveAbstractDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveInsertCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.type.CompositeType;
import org.hibernate.type.EntityType;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;


/**
 * A {@link ReactiveEntityPersister} backed by {@link SingleTableEntityPersister}
 * and {@link ReactiveAbstractEntityPersister}.
 */
public class ReactiveSingleTableEntityPersister extends SingleTableEntityPersister implements ReactiveAbstractEntityPersister {

	private static final Log LOG = make( Log.class, lookup() );

	private final ReactiveAbstractPersisterDelegate reactiveDelegate;

	public ReactiveSingleTableEntityPersister(
			final PersistentClass persistentClass,
			final EntityDataAccess cacheAccessStrategy,
			final NaturalIdDataAccess naturalIdRegionAccessStrategy,
			final RuntimeModelCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, new ReactiveRuntimeModelCreationContext( creationContext ) );
		reactiveDelegate = new ReactiveAbstractPersisterDelegate( this, persistentClass, new ReactiveRuntimeModelCreationContext( creationContext ) );
	}

	@Override
	protected BytecodeEnhancementMetadata getBytecodeEnhancementMetadataPojo(
			PersistentClass persistentClass,
			RuntimeModelCreationContext creationContext,
			Set<String> idAttributeNames,
			CompositeType nonAggregatedCidMapper,
			boolean collectionsInDefaultFetchGroupEnabled) {
			return ReactiveBytecodeEnhancementMetadataPojoImplAdapter
				.from( persistentClass, idAttributeNames, nonAggregatedCidMapper, collectionsInDefaultFetchGroupEnabled, creationContext.getMetadata() );
	}

	@Override
	public GeneratedValuesMutationDelegate createInsertDelegate() {
		return ReactiveAbstractEntityPersister.super.createReactiveInsertDelegate();
	}

	@Override
	public SingleIdEntityLoader<?> determineLoaderToUse(SharedSessionContractImplementor session, LockOptions lockOptions) {
		return super.determineLoaderToUse( session, lockOptions );
	}

	@Override
	protected GeneratedValuesMutationDelegate createUpdateDelegate() {
		return ReactiveAbstractEntityPersister.super.createReactiveUpdateDelegate();
	}

	@Override
	protected SingleIdEntityLoader<?> buildSingleIdEntityLoader() {
		return reactiveDelegate.buildSingleIdEntityLoader();
	}

	@Override
	protected MultiIdEntityLoader<?> buildMultiIdLoader() {
		return reactiveDelegate.buildMultiIdEntityLoader();
	}

	@Override
	protected UpdateCoordinator buildUpdateCoordinator() {
		return ReactiveCoordinatorFactory.buildUpdateCoordinator( this, getFactory() );
	}

	@Override
	protected InsertCoordinator buildInsertCoordinator() {
		return ReactiveCoordinatorFactory.buildInsertCoordinator( this, getFactory() );
	}

	@Override
	protected DeleteCoordinator buildDeleteCoordinator() {
		return ReactiveCoordinatorFactory.buildDeleteCoordinator( super.getSoftDeleteMapping(), this, getFactory() );
	}

	@Override
	protected UpdateCoordinator buildMergeCoordinator() {
		return ReactiveCoordinatorFactory.buildMergeCoordinator( this, getFactory() );
	}

	@Override
	public Generator getGenerator() throws HibernateException {
		return reactiveDelegate.reactive( super.getGenerator() );
	}

	@Override
	public <T> DomainResult<T> createDomainResult(
			NavigablePath navigablePath,
			TableGroup tableGroup,
			String resultVariable,
			DomainResultCreationState creationState) {
		return reactiveDelegate.createDomainResult( this, navigablePath, tableGroup, resultVariable, creationState );
	}

	@Override
	protected AttributeMapping buildSingularAssociationAttributeMapping(
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
		return reactiveDelegate.buildSingularAssociationAttributeMapping(
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
				creationProcess
		);
	}

	@Override
	protected AttributeMapping buildPluralAttributeMapping(
			String attrName,
			int stateArrayPosition,
			int fetchableIndex,
			Property bootProperty,
			ManagedMappingType declaringType,
			PropertyAccess propertyAccess,
			CascadeStyle cascadeStyle,
			FetchMode fetchMode,
			MappingModelCreationProcess creationProcess) {
		return reactiveDelegate.buildPluralAttributeMapping(
				attrName,
				stateArrayPosition,
				fetchableIndex,
				bootProperty,
				declaringType,
				propertyAccess,
				cascadeStyle,
				fetchMode,
				creationProcess
		);
	}

	@Override
	public boolean initializeLazyProperty(String fieldName, Object entity, EntityEntry entry, int lazyIndex, Object selectedValue) {
		return super.initializeLazyProperty(fieldName, entity, entry, lazyIndex, selectedValue);
	}

	@Override
	public Object initializeLazyPropertiesFromDatastore(final Object entity, final Object id, final EntityEntry entry, final String fieldName, final SharedSessionContractImplementor session) {
		return reactiveInitializeLazyPropertiesFromDatastore( entity, id, entry, fieldName, session );
	}

	@Override
	public Object insert(Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "insertReactive" );
	}

	@Override
	public void insert(Object id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "insertReactive" );
	}

	@Override
	public void delete(Object id, Object version, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw LOG.nonReactiveMethodCall( "deleteReactive" );
	}

	@Override
	public void update(
			Object id,
			Object[] fields,
			int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields,
			Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "updateReactive" );
	}

	@Override
	public void merge(
			Object id,
			Object[] values,
			int[] dirtyAttributeIndexes,
			boolean hasDirtyCollection,
			Object[] oldValues,
			Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "mergeReactive" );
	}

	@Override
	protected EntityIdentifierMapping generateIdentifierMapping(
			Supplier<?> templateInstanceCreator,
			PersistentClass bootEntityDescriptor,
			MappingModelCreationProcess creationProcess) {
		return reactiveDelegate.convertEntityIdentifierMapping( super.generateIdentifierMapping(
				templateInstanceCreator,
				bootEntityDescriptor,
				creationProcess
		) );
	}

	/**
	 * Process properties generated with an insert
	 *
	 * @see AbstractEntityPersister#processInsertGeneratedProperties(Object, Object, Object[], SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveProcessInsertGenerated(Object id, Object entity, Object[] state, GeneratedValues generatedValues, SharedSessionContractImplementor session) {
		return reactiveDelegate
				.processInsertGeneratedProperties( id, entity, state, getInsertGeneratedValuesProcessor(), generatedValues, session, getEntityName() );
	}

	/**
	 * Process properties generated with an update
	 *
	 * @see AbstractEntityPersister#processUpdateGeneratedProperties(Object, Object, Object[], SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveProcessUpdateGenerated(Object id, Object entity, Object[] state, GeneratedValues generatedValues, SharedSessionContractImplementor session) {
		return reactiveDelegate
				.processUpdateGeneratedProperties( id, entity, state, getUpdateGeneratedValuesProcessor(), generatedValues, session, getEntityName() );
	}

	/**
	 * @see AbstractEntityPersister#loadEntityIdByNaturalId(Object[], LockOptions, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<?> reactiveLoadEntityIdByNaturalId(Object[] orderedNaturalIdValues, LockOptions lockOptions, SharedSessionContractImplementor session) {
		verifyHasNaturalId();
		return reactiveDelegate.loadEntityIdByNaturalId( orderedNaturalIdValues, lockOptions, session );
	}

	@Override
	public CompletionStage<?> reactiveLoad(Object id, Object optionalObject, LockMode lockMode, SharedSessionContractImplementor session) {
		return reactiveLoad( id, optionalObject, new LockOptions().setLockMode( lockMode ), session );
	}

	@Override
	public Object load(Object id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session) {
		return reactiveLoad( id, optionalObject, lockOptions, session );
	}

	@Override
	public CompletionStage<?> reactiveLoad(Object id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session) {
		return doReactiveLoad( id, optionalObject, lockOptions, null, session );
	}

	@Override
	public Object load(Object id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session, Boolean readOnly) {
		return reactiveLoad( id, optionalObject, lockOptions, session, readOnly );
	}

	@Override
	public CompletionStage<?> reactiveLoad(Object id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session, Boolean readOnly) {
		return doReactiveLoad( id, optionalObject, lockOptions, readOnly, session );
	}

	private CompletionStage<?> doReactiveLoad(Object id, Object optionalObject, LockOptions lockOptions, Boolean readOnly, SharedSessionContractImplementor session) {
		return reactiveDelegate.load( this, id, optionalObject, lockOptions, readOnly, session );
	}

	@Override
	public CompletionStage<GeneratedValues> insertReactive(Object[] fields, Object entity, SharedSessionContractImplementor session) {
		return ( (ReactiveInsertCoordinatorStandard) getInsertCoordinator() ).coordinateReactiveInsert( entity, null, fields, session, true );
	}

	@Override
	public CompletionStage<GeneratedValues> insertReactive(Object id, Object[] fields, Object entity, SharedSessionContractImplementor session) {
		return ( (ReactiveInsertCoordinatorStandard) getInsertCoordinator() ).coordinateReactiveInsert( entity, id, fields, session, false );
	}

	@Override
	public CompletionStage<Void> deleteReactive(Object id, Object version, Object entity, SharedSessionContractImplementor session) {
		return ( (ReactiveAbstractDeleteCoordinator) getDeleteCoordinator() ).reactiveDelete( entity, id, version, session );
	}

	/**
	 * Update an object
	 */
	@Override
	public CompletionStage<GeneratedValues> updateReactive(
			final Object id,
			final Object[] values,
			int[] dirtyAttributeIndexes,
			final boolean hasDirtyCollection,
			final Object[] oldValues,
			final Object oldVersion,
			final Object object,
			final Object rowId,
			final SharedSessionContractImplementor session) throws HibernateException {
		return ( (ReactiveUpdateCoordinator) getUpdateCoordinator() )
				// This is different from Hibernate ORM because our reactive update coordinator cannot be share among
				// multiple update operations
				.makeScopedCoordinator()
				.reactiveUpdate( object, id, rowId, values, oldVersion, oldValues, dirtyAttributeIndexes, hasDirtyCollection, session );
	}

	/**
	 * Merge an object
	 *
	 * @see SingleTableEntityPersister#merge(Object, Object[], int[], boolean, Object[], Object, Object, Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> mergeReactive(
			final Object id,
			final Object[] values,
			int[] dirtyAttributeIndexes,
			final boolean hasDirtyCollection,
			final Object[] oldValues,
			final Object oldVersion,
			final Object object,
			final Object rowId,
			SharedSessionContractImplementor session) {
		return ( (ReactiveUpdateCoordinator) getMergeCoordinator() )
				// This is different from Hibernate ORM because our reactive update coordinator cannot be share among
				// multiple update operations
				.makeScopedCoordinator()
				.reactiveUpdate( object, id, rowId, values, oldVersion, oldValues, dirtyAttributeIndexes, hasDirtyCollection, session )
				.thenCompose( CompletionStages::voidFuture );
	}

	@Override
	public <K> CompletionStage<? extends List<?>> reactiveMultiLoad(K[] ids, SharedSessionContractImplementor session, MultiIdLoadOptions loadOptions) {
		return reactiveDelegate.multiLoad( ids, session, loadOptions );
	}

	@Override
	public Object loadEntityIdByNaturalId(Object[] naturalIdValues, LockOptions lockOptions, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "loadEntityIdByNaturalId" );
	}

	@Override
	public Object loadByUniqueKey(String propertyName, Object uniqueKey, SharedSessionContractImplementor session) {
		return loadByUniqueKey( propertyName, uniqueKey, null, session );
	}

	@Override
	public Object loadByUniqueKey(String propertyName, Object uniqueKey, Boolean readOnly, SharedSessionContractImplementor session) {
		return reactiveLoadByUniqueKey( propertyName, uniqueKey, readOnly, session );
	}

	@Override
	public CompletionStage<Object> reactiveLoadByUniqueKey(String propertyName, Object uniqueKey, SharedSessionContractImplementor session) throws HibernateException {
		return reactiveLoadByUniqueKey( propertyName, uniqueKey, null, session );
	}

	@Override
	public CompletionStage<Object> reactiveLoadByUniqueKey(String propertyName, Object uniqueKey, Boolean readOnly, SharedSessionContractImplementor session) throws HibernateException {
		return getReactiveUniqueKeyLoader( propertyName )
				.load( uniqueKey, LockOptions.NONE, readOnly, session );
	}

	@Override
	protected SingleUniqueKeyEntityLoader<?> getUniqueKeyLoader(String attributeName, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "getReactiveUniqueKeyLoader" );
	}

	protected ReactiveSingleUniqueKeyEntityLoader<Object> getReactiveUniqueKeyLoader(String attributeName) {
		return reactiveDelegate.getReactiveUniqueKeyLoader( this, (SingularAttributeMapping) findByPath( attributeName ) );
	}

	@Override
	public ReactiveSingleIdArrayLoadPlan reactiveGetSQLLazySelectLoadPlan(String fetchGroup) {
		return this.getLazyLoadPlanByFetchGroup( getSubclassPropertyNameClosure() ).get(fetchGroup );
	}

	@Override
	public NaturalIdMapping generateNaturalIdMapping(MappingModelCreationProcess creationProcess, PersistentClass bootEntityDescriptor) {
		return ReactiveAbstractEntityPersister.super.generateNaturalIdMapping(creationProcess, bootEntityDescriptor);
	}
}
