/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.BasicCollectionPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.jboss.logging.Logger;

public class ReactiveBasicCollectionPersister extends BasicCollectionPersister implements ReactiveCollectionPersister {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			ReactiveBasicCollectionPersister.class.getName()
	);
	private static final String VALUE_STR = "(?, ?)";

	public ReactiveBasicCollectionPersister(
			Collection collectionBinding,
			CollectionDataAccess cacheAccessStrategy,
			PersisterCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
	}

	private ReactiveConnection getReactiveConnection(SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
	}

	public CompletionStage<Void> recreateReactive(
			PersistentCollection collection,
			Serializable id,
			SharedSessionContractImplementor session)
			throws HibernateException {
		ReactiveConnection reactiveConnection = getReactiveConnection( session );

		if ( isInverse ) {
			return CompletionStages.voidFuture();
		}

		if ( !isRowInsertEnabled() ) {
			return CompletionStages.voidFuture();
		}

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Inserting collection: %s",
					MessageHelper.collectionInfoString( this, collection, id, session )
			);
		}

		// create all the new entries
		Iterator entries = collection.entries( this );

		if ( entries.hasNext() ) {
			collection.preInsert( this );

			List<Object> valuePairList = new ArrayList<>();

			while ( entries.hasNext() ) {
				valuePairList.add( id );
				valuePairList.add( entries.next() );
			}

			// In order to insert element collection values create a native query with placeholders followed by
			// value sets
			return reactiveConnection
					.update(
							getSQLInsertElementCollectionString(
									valuePairList.size() / 2 ),
							valuePairList.toArray( new Object[0] )
					)
					.thenAccept( ignore -> {
					} );
		}

		return CompletionStages.voidFuture();
	}

	// FIXME: this method currently only works for List<String> element collections and uses a VALUE_STR = "(?, ?)"
	// will not work for List, Map, etc... of @Embedded entity (i.e. complex type)
	private String getSQLInsertElementCollectionString(int numRows) {
		String defaultInsertString = getSQLInsertRowString();
		StringBuilder buf = new StringBuilder( defaultInsertString.length() + ( numRows + 2 ) * VALUE_STR.length() );
		buf.append( defaultInsertString );
		for ( int i = 1; i < numRows; i++ ) {
			buf.append( ", " );
			buf.append( VALUE_STR );
		}
		return buf.toString();
	}

	public CompletionStage<Integer> removeReactive(Serializable id, SharedSessionContractImplementor session)
			throws HibernateException {
		ReactiveConnection reactiveConnection = getReactiveConnection( session );
		if ( !isInverse && isRowDeleteEnabled() ) {
			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"Deleting collection: %s",
						MessageHelper.collectionInfoString( this, id, getFactory() )
				);
			}

			List<Object> params = new ArrayList<>();
			params.add( id );
			String sql = getSQLDeleteString();
			return reactiveConnection.update( sql, params.toArray( new Object[0] ) );
		}
		return CompletionStages.completedFuture( -1 );
	}

	@Override
	public CompletionStage<Void> reactiveDeleteRows(
			PersistentCollection collection,
			Serializable id,
			SharedSessionContractImplementor session) throws HibernateException {
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
	public CompletionStage<Void> reactiveUpdateRows(
			PersistentCollection collection,
			Serializable id,
			SharedSessionContractImplementor session) throws HibernateException {
		if ( !isInverse && collection.isRowUpdatePossible() ) {
			// TODO: See AbstractCollectionPersister#updateRows
			throw new UnsupportedOperationException();
		}
		return CompletionStages.voidFuture();
	}
	@Override
	public CompletionStage<Void> reactiveInsertRows(
			PersistentCollection collection,
			Serializable id,
			SharedSessionContractImplementor session) throws HibernateException {
		if ( isInverse ) {
			return CompletionStages.voidFuture();
		}

		if ( !isRowDeleteEnabled() ) {
			return CompletionStages.voidFuture();
		}

		// FIXME: Not sure what to implement here for OneToMany use-case
		// Maybe nothing due to the ReactiveCollectionUpdate/Remove/Recreate Action classes perform necessary operations
		// on the collection (basically delete and replace rather than operating on individual rows
		throw new UnsupportedOperationException();
	}

}
