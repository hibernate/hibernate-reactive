/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.generator.Generator;
import org.hibernate.id.IdentityGenerator;
import org.hibernate.jdbc.Expectation;
import org.hibernate.loader.ast.internal.SingleIdArrayLoadPlan;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.loader.ast.spi.SingleUniqueKeyEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.UnionSubclassEntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.persister.entity.mutation.UpdateCoordinator;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleUniqueKeyEntityLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveInsertCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;
import org.hibernate.reactive.util.impl.CompletionStages;


/**
 * An {@link ReactiveEntityPersister} backed by {@link UnionSubclassEntityPersister}
 * and {@link ReactiveAbstractEntityPersister}.
 */
public class ReactiveUnionSubclassEntityPersister extends UnionSubclassEntityPersister implements ReactiveAbstractEntityPersister {

	private static final Log log = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveAbstractPersisterDelegate reactiveDelegate;

	public ReactiveUnionSubclassEntityPersister(
			final PersistentClass persistentClass,
			final EntityDataAccess cacheAccessStrategy,
			final NaturalIdDataAccess naturalIdRegionAccessStrategy,
			final RuntimeModelCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );
		reactiveDelegate = new ReactiveAbstractPersisterDelegate( this, persistentClass, creationContext );
	}

	@Override
	protected void validateGenerator() {
		if ( super.getGenerator() instanceof IdentityGenerator ) {
			throw new MappingException( "Cannot use identity column key generation with <union-subclass> mapping for: " + getEntityName() );
		}
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
		return ReactiveCoordinatorFactory.buildDeleteCoordinator( this, getFactory() );
	}

	@Override
	public Generator getGenerator() throws HibernateException {
		return reactiveDelegate.reactive( super.getGenerator() );
	}

	@Override
	public ReactiveSingleIdEntityLoader<?> getReactiveSingleIdEntityLoader() {
		return reactiveDelegate.getSingleIdEntityLoader();
	}

	@Override
	public String[][] getLazyPropertyColumnAliases() {
		return super.getLazyPropertyColumnAliases();
	}

	@Override
	public boolean check(int rows, Object id, int tableNumber, Expectation expectation, PreparedStatement statement, String sql) throws HibernateException {
		return super.check(rows, id, tableNumber, expectation, statement, sql);
	}

	@Override
	public boolean initializeLazyProperty(String fieldName, Object entity, EntityEntry entry, int lazyIndex, Object selectedValue) {
		return super.initializeLazyProperty( fieldName, entity, entry, lazyIndex, selectedValue );
	}

	/**
	 * @see #insertReactive(Object[], Object, SharedSessionContractImplementor)
	 */
	@Override
	public Object insert(Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "insertReactive" );
	}


	/**
	 * @see #insertReactive(Object[], Object, SharedSessionContractImplementor)
	 */
	@Override
	public void insert(Object id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "insertReactive" );
	}


	/**
	 * @see #deleteReactive(Object, Object, Object, SharedSessionContractImplementor)
	 */
	@Override
	public void delete(Object id, Object version, Object object, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "deleteReactive" );
	}


	/**
	 * @see #updateReactive(Object, Object[], int[], boolean, Object[], Object, Object, Object, SharedSessionContractImplementor)
	 */
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
	 * Process properties generated with an insert
	 *
	 * @see AbstractEntityPersister#processInsertGeneratedProperties(Object, Object, Object[], SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveProcessInsertGenerated(Object id, Object entity, Object[] state, SharedSessionContractImplementor session) {
		return reactiveDelegate.processInsertGeneratedProperties( id, entity, state, session, getEntityName() );
	}

	/**
	 * Process properties generated with an update
	 *
	 * @see AbstractEntityPersister#processUpdateGeneratedProperties(Object, Object, Object[], SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveProcessUpdateGenerated(Object id, Object entity, Object[] state, SharedSessionContractImplementor session) {
		return reactiveDelegate.processUpdateGeneratedProperties( id, entity, state, session, getEntityName() );

	}

	@Override
	protected Object initializeLazyPropertiesFromDatastore(Object entity, Object id, EntityEntry entry, String fieldName, SharedSessionContractImplementor session) {
		return reactiveInitializeLazyPropertiesFromDatastore( entity, id, entry, fieldName, session );
	}

	@Override
	public CompletionStage<Object> reactiveLoad(Object id, Object optionalObject, LockMode lockMode, SharedSessionContractImplementor session) {
		return reactiveLoad( id, optionalObject, new LockOptions().setLockMode( lockMode ), session );
	}

	@Override
	public Object load(Object id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session) {
		return reactiveLoad( id, optionalObject, lockOptions, session );
	}

	@Override
	public CompletionStage<Object> reactiveLoad(Object id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session) {
		return doReactiveLoad( id, optionalObject, lockOptions, null, session );
	}

	@Override
	public Object load(Object id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session, Boolean readOnly) {
		return reactiveLoad( id, optionalObject, lockOptions, session, readOnly );
	}

	@Override
	public CompletionStage<Object> reactiveLoad(Object id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session, Boolean readOnly) {
		return doReactiveLoad( id, optionalObject, lockOptions, readOnly, session );
	}

	private CompletionStage<Object> doReactiveLoad(Object id, Object optionalObject, LockOptions lockOptions, Boolean readOnly, SharedSessionContractImplementor session) {
		return reactiveDelegate.load( this, id, optionalObject, lockOptions, readOnly, session );
	}

	@Override
	public CompletionStage<Void> insertReactive(Object id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		return ( (ReactiveInsertCoordinator) getInsertCoordinator() )
				.coordinateReactiveInsert( id, fields, object, session )
				.thenCompose( CompletionStages::voidFuture );
	}

	@Override
	public CompletionStage<Object> insertReactive(Object[] fields, Object object, SharedSessionContractImplementor session) {
		return ( (ReactiveInsertCoordinator) getInsertCoordinator() )
				.coordinateReactiveInsert( null, fields, object, session );
	}

	@Override
	public CompletionStage<Void> deleteReactive(Object id, Object version, Object object, SharedSessionContractImplementor session) {
		return ( (ReactiveDeleteCoordinator) getDeleteCoordinator() ).coordinateReactiveDelete( object, id, version, session );
	}

	@Override
	public CompletionStage<Void> updateReactive(
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
				.coordinateReactiveUpdate( object, id, rowId, values, oldVersion, oldValues, dirtyAttributeIndexes, hasDirtyCollection, session );
	}

	@Override
	public CompletionStage<? extends List<?>> reactiveMultiLoad(Object[] ids, SessionImplementor session, MultiIdLoadOptions loadOptions) {
		return reactiveDelegate.multiLoad( ids, session, loadOptions );
	}

	@Override
	public Object loadEntityIdByNaturalId(Object[] naturalIdValues, LockOptions lockOptions, SharedSessionContractImplementor session) {
		throw LOG.notYetImplemented();
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
	public CompletionStage<Object> reactiveLoadEntityIdByNaturalId(Object[] naturalIdValues, LockOptions lockOptions, SharedSessionContractImplementor session) {
		throw LOG.notYetImplemented();
	}

	@Override
	protected SingleUniqueKeyEntityLoader<?> getUniqueKeyLoader(String attributeName) {
		throw new UnsupportedOperationException( "use the reactive method: #getReactiveUniqueKeyLoader(String)" );
	}

	protected ReactiveSingleUniqueKeyEntityLoader<Object> getReactiveUniqueKeyLoader(String attributeName) {
		return reactiveDelegate.getReactiveUniqueKeyLoader( this, (SingularAttributeMapping) findByPath( attributeName ) );
	}

	@Override
	public CompletionStage<Object> reactiveLoadEntityIdByNaturalId(Object[] orderedNaturalIdValues, LockOptions lockOptions, EventSource session) {
		throw LOG.notYetImplemented();
	}

	@Override
	public SingleIdArrayLoadPlan getSQLLazySelectLoadPlan(String fetchGroup) {
		throw new UnsupportedOperationException();
	}
}
