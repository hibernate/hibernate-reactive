/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.CascadingActions;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.jdbc.Expectation;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.entity.JoinedSubclassEntityPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.loader.entity.ReactiveUniqueEntityLoader;
import org.hibernate.reactive.loader.entity.impl.ReactiveBatchingEntityLoaderBuilder;
import org.hibernate.reactive.loader.entity.impl.ReactiveCascadeEntityLoader;

import java.io.Serializable;
import java.sql.PreparedStatement;

/**
 * An {@link ReactiveEntityPersister} backed by {@link JoinedSubclassEntityPersister}
 *  * amd {@link ReactiveAbstractEntityPersister}.
 */
public class ReactiveJoinedSubclassEntityPersister extends JoinedSubclassEntityPersister implements ReactiveAbstractEntityPersister {

	public ReactiveJoinedSubclassEntityPersister(
			PersistentClass persistentClass,
			EntityDataAccess cacheAccessStrategy,
			NaturalIdDataAccess naturalIdRegionAccessStrategy,
			PersisterCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );
	}

	@Override
	protected void createLoaders() {
		super.createLoaders();

		getLoaders().put( "merge", new ReactiveCascadeEntityLoader( this, CascadingActions.MERGE, getFactory() ) );
		getLoaders().put( "refresh", new ReactiveCascadeEntityLoader( this, CascadingActions.REFRESH, getFactory() ) );
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockMode lockMode, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		return ReactiveBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockMode, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockOptions lockOptions, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		return ReactiveBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockOptions, getFactory(), loadQueryInfluencers );
	}

	@Override
	public Serializable insert(
			Object[] fields, boolean[] notNull, String sql, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(
			Serializable id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			Object object,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public Serializable insert(
			Object[] fields, Object object, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(Serializable id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void delete(
			Serializable id,
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
			Serializable id, Object version, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void updateOrInsert(
			Serializable id,
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
			Serializable id,
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
			Serializable id,
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

	@Override
	public ReactiveUniqueEntityLoader getAppropriateLoader(LockOptions lockOptions, SharedSessionContractImplementor session) {
		return (ReactiveUniqueEntityLoader) super.getAppropriateLoader(lockOptions, session);
	}

	@Override
	public void preInsertInMemoryValueGeneration(Object[] fields, Object object,
												 SharedSessionContractImplementor session) {
		super.preInsertInMemoryValueGeneration(fields, object, session);
	}

	@Override
	public String[] getUpdateStrings(boolean byRowId, boolean hasUninitializedLazyProperties) {
		return super.getUpdateStrings(byRowId, hasUninitializedLazyProperties);
	}

	@Override
	public boolean check(int rows, Serializable id, int tableNumber,
						 Expectation expectation, PreparedStatement statement) throws HibernateException {
		return super.check(rows, id, tableNumber, expectation, statement);
	}
}
