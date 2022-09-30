/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.jdbc.Expectation;
import org.hibernate.loader.ast.internal.SingleIdArrayLoadPlan;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.loader.ast.spi.SingleUniqueKeyEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.SingleTableEntityPersister;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleUniqueKeyEntityLoader;


/**
 * An {@link ReactiveEntityPersister} backed by {@link SingleTableEntityPersister}
 * and {@link ReactiveAbstractEntityPersister}.
 */
public class ReactiveSingleTableEntityPersister extends SingleTableEntityPersister
		implements ReactiveAbstractEntityPersister {

	private final ReactiveAbstractPersisterDelegate reactiveDelegate;

	public ReactiveSingleTableEntityPersister(
			final PersistentClass persistentClass,
			final EntityDataAccess cacheAccessStrategy,
			final NaturalIdDataAccess naturalIdRegionAccessStrategy,
			final RuntimeModelCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );
		reactiveDelegate = new ReactiveAbstractPersisterDelegate( this, persistentClass, creationContext );
	}

	@Override
	public IdentifierGenerator getIdentifierGenerator() throws HibernateException {
		return reactiveDelegate.reactive( super.getIdentifierGenerator() );
	}

	@Override
	public boolean check(int rows, Object id, int tableNumber, Expectation expectation, PreparedStatement statement, String sql) throws HibernateException {
		return super.check( rows, id, tableNumber, expectation,statement, sql  );
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
	public String[][] getLazyPropertyColumnAliases() {
		return super.getLazyPropertyColumnAliases();
	}

	@Override
	public String[] getUpdateStrings(boolean byRowId, boolean hasUninitializedLazyProperties) {
		return super.getUpdateStrings(byRowId, hasUninitializedLazyProperties);
	}

	@Override
	public ReactiveSingleIdEntityLoader<?> getReactiveSingleIdEntityLoader() {
		return reactiveDelegate.getSingleIdEntityLoader();
	}

	@Override
	public String generateSelectVersionString() {
		String sql = super.generateSelectVersionString();
		return parameters().process( sql );
	}

	@Override
	public String generateDeleteString(int j, boolean includeVersion) {
		String sql = super.generateDeleteString( j, includeVersion );
		return parameters().process( sql );
	}

	@Override
	public String generateUpdateString(boolean[] includeProperty, int j, boolean useRowId) {
		String sql = super.generateUpdateString( includeProperty, j, useRowId );
		return parameters().process( sql );
	}

	@Override
	public String generateUpdateString(boolean[] includeProperty, int j, Object[] oldFields, boolean useRowId) {
		String sql = super.generateUpdateString( includeProperty, j, oldFields, useRowId );
		return parameters().process( sql );
	}

	@Override
	public String generateInsertString(boolean[] includeProperty, int j) {
		String sql = super.generateInsertString( includeProperty, j );
		return parameters().process( sql, includeProperty.length );
	}

	@Override
	public boolean hasProxy() {
		return hasEnhancedProxy();
	}

	@Override
	public Object insert(
			Object[] fields,
			boolean[] notNull,
			String sql,
			Object object,
			SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(
			Object id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			Object object,
			SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public Object insert(Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(Object id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void delete(
			Object id,
			Object version,
			int j,
			Object object,
			String sql,
			SharedSessionContractImplementor session,
			Object[] loadedState) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void delete(
			Object id, Object version, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void updateOrInsert(
			Object id,
			Object[] fields,
			Object[] oldFields,
			Object rowId,
			boolean[] includeProperty,
			int j,
			Object oldVersion,
			Object object,
			String sql,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public boolean update(
			Object id,
			Object[] fields,
			Object[] oldFields,
			Object rowId,
			boolean[] includeProperty,
			int j,
			Object oldVersion,
			Object object,
			String sql,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
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
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
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
	public CompletionStage<? extends List<?>> reactiveMultiLoad(Object[] ids, SessionImplementor session, MultiIdLoadOptions loadOptions) {
		return reactiveDelegate.multiLoad( ids, session, loadOptions );
	}

	/**
	 * @deprecated use {@link #reactivePreInsertInMemoryValueGeneration(Object[], Object, SharedSessionContractImplementor)}
	 */
	@Override
	@Deprecated
	protected void preInsertInMemoryValueGeneration(Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException("Use reactivePreInsertInMemoryValueGeneration instead");
	}

	@Override
	public Object loadEntityIdByNaturalId(Object[] naturalIdValues, LockOptions lockOptions, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException("not yet implemented");
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
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public CompletionStage<Object> reactiveLoadEntityIdByNaturalId(Object[] orderedNaturalIdValues, LockOptions lockOptions, EventSource session) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	protected SingleUniqueKeyEntityLoader<?> getUniqueKeyLoader(String attributeName) {
		throw new UnsupportedOperationException( "use the reactive method: #getReactiveUniqueKeyLoader(String)" );
	}

	protected ReactiveSingleUniqueKeyEntityLoader<Object> getReactiveUniqueKeyLoader(String attributeName) {
		return reactiveDelegate.getReactiveUniqueKeyLoader( this, (SingularAttributeMapping) findByPath( attributeName ) );
	}

	@Override
	public SingleIdArrayLoadPlan getSQLLazySelectLoadPlan(String fetchGroup) {
		// TODO Auto-generated method stub
		return null;
	}

}
