/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.engine.spi.*;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.loader.ReactiveOuterJoinLoader;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;

public class ReactiveCollectionLoader extends ReactiveOuterJoinLoader
		implements ReactiveCollectionInitializer {
	private final QueryableCollection collectionPersister;

	public ReactiveCollectionLoader(
			QueryableCollection collectionPersister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super(factory, loadQueryInfluencers);
		this.collectionPersister = collectionPersister;
	}

	protected QueryableCollection collectionPersister() {
		return collectionPersister;
	}

	@Override
	protected boolean isSubselectLoadingEnabled() {
		return hasSubselectLoadableCollections();
	}

	/**
	 * @deprecated use {@link #reactiveInitialize(Serializable, SharedSessionContractImplementor)}
	 */
	@Override
	@Deprecated
	public void initialize(Serializable id, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Use the reactive method instead: reactiveInitialize");
	}

	@Override
	public CompletionStage<Void> reactiveInitialize(Serializable id, SharedSessionContractImplementor session) throws HibernateException {
		return reactiveLoadCollection( (SessionImplementor) session, id, getKeyType() );
	}

	/**
	 * Called by subclasses that initialize collections
	 */
	public CompletionStage<Void> reactiveLoadCollection(
			final SessionImplementor session,
			final Serializable id,
			final Type type) throws HibernateException {
		if (LOG.isDebugEnabled()) {
			LOG.debugf(
					"Loading collection: %s",
					MessageHelper.collectionInfoString(collectionPersister(), id, getFactory())
			);
		}

		Serializable[] ids = new Serializable[]{id};
		QueryParameters parameters = new QueryParameters( new Type[]{type}, ids, ids );
		return doReactiveQueryAndInitializeNonLazyCollections( session, parameters, true )
				.handle( (list, e) -> {
					if (e instanceof JDBCException) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
								((JDBCException) e).getSQLException(),
								"could not initialize a collection: " +
										MessageHelper.collectionInfoString(collectionPersister(), id, getFactory()),
								getSQLString()
						);
					} else if (e != null) {
						CompletionStages.rethrow(e);
					}
					LOG.debug("Done loading collection");
					return null;
				} );
	}

	/**
	 * Called by wrappers that batch initialize collections
	 */
	public final CompletionStage<Void> reactiveLoadCollectionBatch(
			final SessionImplementor session,
			final Serializable[] ids,
			final Type type) throws HibernateException {
		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Batch loading collection: %s",
					MessageHelper.collectionInfoString( getCollectionPersisters()[0], ids, getFactory() )
			);
		}

		Type[] idTypes = new Type[ids.length];
		Arrays.fill( idTypes, type );
		QueryParameters parameters = new QueryParameters( idTypes, ids, ids );
		return doReactiveQueryAndInitializeNonLazyCollections( session, parameters, true )
				.handle( (list, e) -> {
					if (e instanceof JDBCException) {
						throw getFactory().getJdbcServices().getSqlExceptionHelper().convert(
								((JDBCException) e).getSQLException(),
								"could not initialize a collection batch: " +
										MessageHelper.collectionInfoString( getCollectionPersisters()[0], ids, getFactory() ),
								getSQLString()
						);
					} else if (e != null) {
						CompletionStages.rethrow(e);
					}
					LOG.debug("Done batch load");
					return null;
				} );
	}

	protected Type getKeyType() {
		return collectionPersister.getKeyType();
	}

	@Override
	public String toString() {
		return getClass().getName() + '(' + collectionPersister.getRole() + ')';
	}

}
