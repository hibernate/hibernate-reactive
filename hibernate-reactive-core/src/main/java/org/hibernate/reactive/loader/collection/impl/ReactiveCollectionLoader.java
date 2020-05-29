/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.collection.CollectionLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.reactive.loader.ReactiveLoader;
import org.hibernate.reactive.loader.collection.ReactiveCollectionInitializer;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.collectionInfoString;

/**
 * A reactific {@link org.hibernate.loader.collection.CollectionLoader}.
 *
 * @see org.hibernate.loader.collection.CollectionLoader
 */
public class ReactiveCollectionLoader extends CollectionLoader
		implements ReactiveCollectionInitializer, ReactiveLoader {

	public ReactiveCollectionLoader(
			QueryableCollection collectionPersister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super(collectionPersister, factory, loadQueryInfluencers);
	}

	protected CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies) {
		return doReactiveQueryAndInitializeNonLazyCollections(
				getSQLString(),
				session,
				queryParameters,
				returnProxies,
				null
		);
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
					collectionInfoString(collectionPersister(), id, getFactory())
			);
		}

		Serializable[] ids = new Serializable[]{id};
		QueryParameters parameters = new QueryParameters( new Type[]{type}, ids, ids );
		return doReactiveQueryAndInitializeNonLazyCollections( session, parameters, true )
				.handle( (list, err) -> {
					CompletionStages.logSqlException( err,
							() -> "could not initialize a collection: " +
									collectionInfoString( collectionPersister(), id, getFactory() ),
							getSQLString()
					);
					LOG.debug("Done loading collection");
					return CompletionStages.returnNullorRethrow( err );
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
					collectionInfoString( getCollectionPersisters()[0], ids, getFactory() )
			);
		}

		Type[] idTypes = new Type[ids.length];
		Arrays.fill( idTypes, type );
		QueryParameters parameters = new QueryParameters( idTypes, ids, ids );
		return doReactiveQueryAndInitializeNonLazyCollections( session, parameters, true )
				.handle( (list, err) -> {
					CompletionStages.logSqlException( err,
							() -> "could not initialize a collection batch: " +
									collectionInfoString( getCollectionPersisters()[0], ids, getFactory() ),
							getSQLString()
					);
					LOG.debug("Done batch load");
					return CompletionStages.returnNullorRethrow( err );
				} );
	}

	@Override
	public String preprocessSQL(String sql,
								QueryParameters queryParameters,
								SessionFactoryImplementor factory,
								List<AfterLoadAction> afterLoadActions) {
		//this is needed here for the case of fetching a collection with filters
		return super.preprocessSQL(sql, queryParameters, factory, afterLoadActions);
	}

	@Override @SuppressWarnings("unchecked")
	public List<Object> processResultSet(ResultSet resultSet,
										 QueryParameters queryParameters,
										 SharedSessionContractImplementor session,
										 boolean returnProxies,
										 ResultTransformer forcedResultTransformer,
										 int maxRows, List<AfterLoadAction> afterLoadActions) throws SQLException {
		return super.processResultSet(
				resultSet,
				queryParameters,
				session,
				returnProxies,
				forcedResultTransformer,
				maxRows,
				afterLoadActions
		);
	}
}
