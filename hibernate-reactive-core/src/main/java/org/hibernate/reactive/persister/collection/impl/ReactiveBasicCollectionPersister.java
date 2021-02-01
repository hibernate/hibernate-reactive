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
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.jboss.logging.Logger;

/**
 *
 *     -- >> see  AbstractCollectionPersister for ORM implementations
 * 				// TODO: Check out batching logic: See AbstractCollectionPersister for usage
 * 				// boolean useBatch = expectation.canBeBatched();
 */
public class ReactiveBasicCollectionPersister extends BasicCollectionPersister implements ReactiveCollectionPersister {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			ReactiveBasicCollectionPersister.class.getName()
	);
	private final Parameters parameters;

	public ReactiveBasicCollectionPersister(
			Collection collectionBinding,
			CollectionDataAccess cacheAccessStrategy,
			PersisterCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
		this.parameters = Parameters.instance( getFactory().getJdbcServices().getDialect() );
	}

	private ReactiveConnection getReactiveConnection(SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
	}

	@Override
	protected String getSQLInsertRowString() {
		String sql = super.getSQLInsertRowString();
		return parameters.process( sql );
	}

	@Override
	protected String getSQLDeleteRowString() {
		String sql = super.getSQLDeleteRowString();
		return parameters.process( sql );
	}

	@Override
	protected String getSQLDeleteString() {
		String sql = super.getSQLDeleteString();
		return parameters.process( sql );
	}

	public CompletionStage<Void> recreateReactive(
			PersistentCollection collection,
			Serializable id,
			SharedSessionContractImplementor session)
			throws HibernateException {
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

		CompletionStage<Integer> loop = CompletionStages.zeroFuture();
		ReactiveConnection reactiveConnection = getReactiveConnection( session );

		// create all the new entries
		Iterator entries = collection.entries( this );

		if ( entries.hasNext() ) {
			collection.preInsert( this );

			int i = 0;

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
								if ( hasIndex /* && !indexIsFormula */) {
									loc = writeIndex( st, collection.getIndex( entry, i, this ), loc, session );
								}
								loc = writeElement( st, collection.getElement( entry ), loc, session );

								collection.afterRowInsert( this, entry, i );
							}
					);
					loop = loop.thenCompose( total -> reactiveConnection
							.update( getSQLInsertRowString(), paramValues )
							.thenApply( insertCount -> total + insertCount ) );
				}
			}
		}

		return loop.thenAccept( total -> LOG.debugf( "Done inserting collection: %s rows inserted", total ) );
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

		CompletionStage<Integer> loop = CompletionStages.zeroFuture();
		ReactiveConnection reactiveConnection = getReactiveConnection( session );

		boolean deleteByIndex = hasIndex && !indexContainsFormula;

		Iterator deletes = collection.getDeletes( this, !deleteByIndex );

		int offset = 1;
		int i = 0;

		while ( deletes.hasNext() ) {
			final Object entry = deletes.next();
			if ( collection.entryExists( entry, i ) ) {
				Object[] paramValues = PreparedStatementAdaptor.bind(
						st -> {
							int loc = offset;
							if ( hasIdentifier ) {
								writeIdentifier( st, entry, loc, session );
							}
							else {
								loc = writeKey( st, id, loc, session );
								if ( deleteByIndex ) {
									writeIndexToWhere( st, entry, loc, session );
								}
								else {
									writeElementToWhere( st, entry, loc, session );
								}
							}
						}
				);
				loop = loop.thenCompose( total -> reactiveConnection
						.update( getSQLDeleteRowString(), paramValues )
						.thenApply( deleteCount -> total + deleteCount ) );
			}
		}

		return loop.thenAccept( total -> LOG.debugf( "Done deleting rows: %s deleted", total ) );
	}

	@Override
	public CompletionStage<Void> reactiveUpdateRows(
			PersistentCollection collection,
			Serializable id,
			SharedSessionContractImplementor session) throws HibernateException {

		// Currently there is no "updateRows()" method in AbstractCollectionPersister
		// Not sure if this method, reactiveUpdateRows() is needed.

		// TODO:  @Barry
		// Can each element in the collection be mapped unequivocally to a single row in the database?
		// Generally bags and sets are the only collections that cannot be.
		// So Maps, for instance will return isRowUpdatePossible() == TRUE
		// and require doUpdateRows() to be replaced by a reactive

		if ( !isInverse && collection.isRowUpdatePossible() ) {

			LOG.debugf( "Updating rows of collection: %s#%s", getNavigableRole().getFullPath(), id );

			// update all the modified entries
			// NOTE this method call uses a JDBC connection and will fail for Map ElementCollection type
			// May also have to override getSQLDeleteString() to correctly parameterize to handle the SQL for the PG use case
			// SEE getSQLInsertRowString() in this class for an example
			int count = doUpdateRows( id, collection, session );

			LOG.debugf( "Done updating rows: %s updated", count );
		}

		return CompletionStages.voidFuture();
	}

	/**
	 * @see org.hibernate.persister.collection.AbstractCollectionPersister#insertRows(PersistentCollection, Serializable, SharedSessionContractImplementor)
	 */
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

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Inserting rows of collection: %s",
					MessageHelper.collectionInfoString( this, collection, id, session )
			);
		}

		collection.preInsert( this );
		// FIXME @Barry: Should we keep it? What's Expectation for?
		// Expectation expectation = Expectations.appropriateExpectation( getInsertCheckStyle() );

		CompletionStage<Integer> loop = CompletionStages.zeroFuture();
		ReactiveConnection reactiveConnection = getReactiveConnection( session );
		Iterator entries = collection.entries( this );

		int i = 0;
		while ( entries.hasNext() ) {
			int offset = 1;
			final Object entry = entries.next();
			if ( collection.entryExists( entry, i )
					&& collection.needsInserting( entry, i, elementType ) ) {

				// TODO: Check batching: See AbstractCollectionPersister#insertRows
				// boolean useBatch = expectation.canBeBatched();

				Object[] paramValues = PreparedStatementAdaptor.bind(
						st -> {
							int locOffset = writeKey( st, id, offset, session );

							if ( hasIdentifier ) {
								locOffset = writeIdentifier( st, collection.getIdentifier( entry, i ), locOffset, session );
							}
							if ( hasIndex /* && !indexIsFormula */) {
								locOffset = writeIndex( st, collection.getIndex( entry, i, this ), locOffset, session );
							}
							locOffset = writeElement( st, collection.getElement( entry ), locOffset, session );

							collection.afterRowInsert( this, entry, i );
						}
				);
				loop = loop.thenCompose( total -> reactiveConnection
						.update( getSQLInsertRowString(), paramValues )
						.thenApply( insertCount -> total + insertCount ) );
			}
		}
		return loop.thenAccept( total -> LOG.debugf( "Done inserting rows: %s inserted", total ) );
	}

}
