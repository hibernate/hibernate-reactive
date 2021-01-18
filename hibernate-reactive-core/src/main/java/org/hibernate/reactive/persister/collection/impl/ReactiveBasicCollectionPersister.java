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
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
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

		CompletionStage<?> loop = CompletionStages.voidFuture();
		// FIXME: This is just a non working POC
		if ( entries.hasNext() ) {
			collection.preInsert( this );

			int i = 0;
			int count = 0;

			while ( entries.hasNext() ) {
				final Object entry = entries.next();
				if ( collection.entryExists( entry, i ) ) {
					int offset = 1;
					Object[] paramValues = PreparedStatementAdaptor.bind(
							st -> {
								int loc = writeKey( st, id, offset, session );
								if ( hasIdentifier ) {
									loc = writeIdentifier( st, collection.getIdentifier( entry, i ), loc, session );
								}
								loc = writeElement( st, collection.getElement( entry ), loc, session );

								// FIXME: Set the other parameters as well
							}
					);
					loop = loop.thenCompose( v -> reactiveConnection
													 .update( getSQLInsertRowString(), paramValues ));
				}
			}

			return loop.thenCompose( CompletionStages::voidFuture );
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
