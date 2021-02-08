/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jdbc.Expectation;
import org.hibernate.jdbc.Expectations;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.loader.collection.impl.ReactiveBatchingCollectionInitializerBuilder;
import org.hibernate.reactive.loader.collection.ReactiveCollectionInitializer;
import org.hibernate.reactive.loader.collection.impl.ReactiveSubselectOneToManyLoader;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.jboss.logging.Logger;

public class ReactiveOneToManyPersister extends OneToManyPersister implements ReactiveCollectionPersister {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger( CoreMessageLogger.class, ReactiveOneToManyPersister.class.getName() );

	public ReactiveOneToManyPersister(Collection collectionBinding, CollectionDataAccess cacheAccessStrategy, PersisterCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
	}

	public CompletionStage<Void> reactiveInitialize(Serializable key, SharedSessionContractImplementor session)
			throws HibernateException {
		return getAppropriateInitializer( key, session ).reactiveInitialize( key, session );
	}

	@Override
	protected ReactiveCollectionInitializer createCollectionInitializer(LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		return ReactiveBatchingCollectionInitializerBuilder.getBuilder( getFactory() )
				.createBatchingOneToManyInitializer( this, batchSize, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected ReactiveCollectionInitializer createSubselectInitializer(SubselectFetch subselect, SharedSessionContractImplementor session) {
		return new ReactiveSubselectOneToManyLoader(
				this,
				subselect.toSubselectString( getCollectionType().getLHSPropertyName() ),
				subselect.getResult(),
				subselect.getQueryParameters(),
				subselect.getNamedParameterLocMap(),
				session.getFactory(),
				session.getLoadQueryInfluencers()
		);
	}

	protected ReactiveCollectionInitializer getAppropriateInitializer(Serializable key, SharedSessionContractImplementor session) {
		return (ReactiveCollectionInitializer) super.getAppropriateInitializer(key, session);
	}

	@Override
	public CompletionStage<Void> recreateReactive(
			PersistentCollection collection, Serializable id, SharedSessionContractImplementor session) {
		return CompletionStages.voidFuture();
	}

	private ReactiveConnection getReactiveConnection(SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
	}

	@Override
	public CompletionStage<Void> removeReactive(Serializable id, SharedSessionContractImplementor session)
			throws HibernateException {
		ReactiveConnection reactiveConnection = getReactiveConnection( session );
		if ( !isInverse && isRowDeleteEnabled() ) {
			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"Deleting collection: %s",
						MessageHelper.collectionInfoString( this, id, getFactory() )
				);
			}

			// Remove all the old entries

			int offset = 1;
			final PreparedStatement st;
			Expectation expectation = Expectations.appropriateExpectation( getDeleteAllCheckStyle() );
			boolean callable = isDeleteAllCallable();

			List<Object> params = new ArrayList<>();
			params.add( id );
			String sql = getSQLDeleteString();

			return CompletionStages.voidFuture()
					.thenCompose( s -> reactiveConnection.update( sql, params.toArray( new Object[0] )))
					.thenCompose(CompletionStages::voidFuture);
		}
		return CompletionStages.voidFuture();
	}

	@Override
	public CompletionStage<Void> reactiveDeleteRows(
			PersistentCollection collection, Serializable id, SharedSessionContractImplementor session) {
		if ( isInverse ) {
			return CompletionStages.voidFuture();
		}

		if ( !isRowDeleteEnabled() ) {
			return CompletionStages.voidFuture();
		}

		//TODO: See AbstractCollectionPersister#deleteRows
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> reactiveInsertRows(
			PersistentCollection collection, Serializable id, SharedSessionContractImplementor session) {

		if ( isInverse ) {
			return CompletionStages.voidFuture();
		}

		if ( !isRowDeleteEnabled() ) {
			return CompletionStages.voidFuture();
		}

		// TODO: See AbstractCollectionPersister#insertRows
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> reactiveUpdateRows(
			PersistentCollection collection, Serializable id, SharedSessionContractImplementor session) {
		if ( !isInverse && collection.isRowUpdatePossible() ) {
			// TODO: See AbstractCollectionPersister#updateRows
			throw new UnsupportedOperationException();
		}
		return CompletionStages.voidFuture();
	}
}
