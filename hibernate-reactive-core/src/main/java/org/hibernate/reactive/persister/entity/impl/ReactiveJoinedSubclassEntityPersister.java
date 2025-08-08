/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.FetchMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.CascadeStyle;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.Generator;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.loader.ast.spi.MultiIdEntityLoader;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.loader.ast.spi.SingleIdEntityLoader;
import org.hibernate.loader.ast.spi.SingleUniqueKeyEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.ManagedMappingType;
import org.hibernate.metamodel.mapping.NaturalIdMapping;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.metamodel.model.domain.NavigableRole;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.JoinedSubclassEntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.persister.entity.mutation.UpdateCoordinator;
import org.hibernate.property.access.spi.PropertyAccess;
import org.hibernate.reactive.loader.ast.internal.ReactiveSingleIdArrayLoadPlan;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleUniqueKeyEntityLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.metamodel.mapping.internal.ReactiveRuntimeModelCreationContext;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveInsertCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.tree.from.TableGroup;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.graph.DomainResultCreationState;
import org.hibernate.type.EntityType;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * An {@link ReactiveEntityPersister} backed by {@link JoinedSubclassEntityPersister}
 * and {@link ReactiveAbstractEntityPersister}.
 */
public class ReactiveJoinedSubclassEntityPersister extends JoinedSubclassEntityPersister
		implements ReactiveAbstractEntityPersister {

	private static final Log LOG = make( Log.class, lookup() );

	private final ReactiveAbstractPersisterDelegate reactiveDelegate;

	public ReactiveJoinedSubclassEntityPersister(
			final PersistentClass persistentClass,
			final EntityDataAccess cacheAccessStrategy,
			final NaturalIdDataAccess naturalIdRegionAccessStrategy,
			final RuntimeModelCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, new ReactiveRuntimeModelCreationContext( creationContext ) );
		reactiveDelegate = new ReactiveAbstractPersisterDelegate( this, persistentClass, new ReactiveRuntimeModelCreationContext( creationContext ) );
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
	public SingleIdEntityLoader<?> determineLoaderToUse(SharedSessionContractImplementor session, LockOptions lockOptions) {
		return super.determineLoaderToUse( session, lockOptions );
	}

	@Override
	protected InsertCoordinator buildInsertCoordinator() {
		return ReactiveCoordinatorFactory.buildInsertCoordinator( this, getFactory() );
	}

	@Override
	protected UpdateCoordinator buildUpdateCoordinator() {
		return ReactiveCoordinatorFactory.buildUpdateCoordinator( this, getFactory() );
	}

	@Override
	protected DeleteCoordinator buildDeleteCoordinator() {
		return ReactiveCoordinatorFactory.buildDeleteCoordinator( super.getSoftDeleteMapping(), this, getFactory() );
	}

	@Override
	public Generator getGenerator() throws HibernateException {
		return reactiveDelegate.reactive( super.getGenerator() );
	}

	/**
	 * @see AbstractEntityPersister#createDomainResult(NavigablePath, TableGroup, String, DomainResultCreationState)
	 */
	@Override
	public <T> DomainResult<T> createDomainResult(
			NavigablePath navigablePath,
			TableGroup tableGroup,
			String resultVariable,
			DomainResultCreationState creationState) {
		return reactiveDelegate.createDomainResult( this, navigablePath, tableGroup, resultVariable, creationState );
	}

	@Override
	public NaturalIdMapping generateNaturalIdMapping(MappingModelCreationProcess creationProcess, PersistentClass bootEntityDescriptor) {
		return ReactiveAbstractEntityPersister.super.generateNaturalIdMapping(creationProcess, bootEntityDescriptor);
	}

	@Override
	public CompletionStage<GeneratedValues> insertReactive(Object id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		return ( (ReactiveInsertCoordinatorStandard) getInsertCoordinator() ).coordinateReactiveInsert( object, id, fields, session, false );
	}

	@Override
	public CompletionStage<GeneratedValues> insertReactive(Object[] fields, Object object, SharedSessionContractImplementor session) {
		return ( (ReactiveInsertCoordinatorStandard) getInsertCoordinator() ).coordinateReactiveInsert( object, null, fields, session, true );
	}

	@Override
	public CompletionStage<Void> deleteReactive(Object id, Object version, Object object, SharedSessionContractImplementor session) {
		return ( (ReactiveDeleteCoordinator) getDeleteCoordinator() ).reactiveDelete( object, id, version, session );
	}

	@Override
	public CompletionStage<GeneratedValues> updateReactive(
			Object id,
			Object[] values,
			int[] dirtyAttributeIndexes,
			boolean hasDirtyCollection,
			Object[] oldValues,
			Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session) {
		return ( (ReactiveUpdateCoordinator) getUpdateCoordinator() )
				// This is different from Hibernate ORM because our reactive update coordinator cannot be share among
				// multiple update operations
				.makeScopedCoordinator()
				.reactiveUpdate( object, id, rowId, values, oldVersion, oldValues, dirtyAttributeIndexes, hasDirtyCollection, session );
	}

	/**
	 * Merge an Object
	 *
	 * @see #merge(Object, Object[], int[], boolean, Object[], Object, Object, Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> mergeReactive(
			Object id,
			Object[] values,
			int[] dirtyAttributeIndexes,
			boolean hasDirtyCollection,
			Object[] oldValues,
			Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session) {
		// This is different from Hibernate ORM because our reactive update coordinator cannot be share among
		// multiple update operations
		return ( (ReactiveUpdateCoordinator) getMergeCoordinator() )
				.makeScopedCoordinator()
				.reactiveUpdate( object, id, rowId, values, oldVersion, oldValues, dirtyAttributeIndexes, hasDirtyCollection, session )
				.thenCompose( CompletionStages::voidFuture );
	}

	@Override
	public <K> CompletionStage<? extends List<?>> reactiveMultiLoad(K[] ids, SharedSessionContractImplementor session, MultiIdLoadOptions loadOptions) {
		return reactiveDelegate.multiLoad( ids, session, loadOptions );
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

	/**
	 * @see #mergeReactive(Object, Object[], int[], boolean, Object[], Object, Object, Object, SharedSessionContractImplementor)
	 */
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
	public boolean initializeLazyProperty(String fieldName, Object entity, EntityEntry entry, int lazyIndex, Object selectedValue) {
		return super.initializeLazyProperty( fieldName, entity, entry, lazyIndex, selectedValue );
	}

	/**
	 * Process properties generated with an insert
	 *
	 * @see AbstractEntityPersister#processInsertGeneratedProperties(Object, Object, Object[], GeneratedValues, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveProcessInsertGenerated(Object id, Object entity, Object[] state, GeneratedValues generatedValues, SharedSessionContractImplementor session) {
		return reactiveDelegate.processInsertGeneratedProperties( id, entity, state,
				getInsertGeneratedValuesProcessor(), generatedValues, session, getEntityName() );
	}

	/**
	 * Process properties generated with an update
	 *
	 * @see AbstractEntityPersister#processUpdateGeneratedProperties(Object, Object, Object[], SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveProcessUpdateGenerated(Object id, Object entity, Object[] state, GeneratedValues generatedValues, SharedSessionContractImplementor session) {
		return reactiveDelegate.processUpdateGeneratedProperties( id, entity, state,
				getUpdateGeneratedValuesProcessor(), generatedValues, session, getEntityName() );
	}

	@Override
	protected Object initializeLazyPropertiesFromDatastore(Object entity, Object id, EntityEntry entry, String fieldName, SharedSessionContractImplementor session) {
		return reactiveInitializeLazyPropertiesFromDatastore( entity, id, entry, fieldName, session );
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

	/**
	 * @see AbstractEntityPersister#loadEntityIdByNaturalId(Object[], LockOptions, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<?> reactiveLoadEntityIdByNaturalId(Object[] orderedNaturalIdValues, LockOptions lockOptions, SharedSessionContractImplementor session) {
		verifyHasNaturalId();
		return reactiveDelegate.loadEntityIdByNaturalId( orderedNaturalIdValues, lockOptions, session );
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

}
