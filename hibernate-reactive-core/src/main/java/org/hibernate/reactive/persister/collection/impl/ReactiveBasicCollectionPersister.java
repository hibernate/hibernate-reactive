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

	@Override
	protected String getSQLUpdateRowString() {
		String sql = super.getSQLUpdateRowString();
		return parameters.process( sql );
	}

	/**
	 * @see org.hibernate.persister.collection.AbstractCollectionPersister#recreate(PersistentCollection, Serializable, SharedSessionContractImplementor)
	 */
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

		collection.preInsert( this );

		return CompletionStages.total(
				entryArray(collection),
				entry -> loop.thenCompose( total -> reactiveConnection
								.update( getSQLInsertRowString(), paramValues( entry, collection, id,session ))
								.thenApply( insertCount -> total + insertCount ) ) )
				.thenAccept( total -> LOG.debugf( "Done inserting rows: %s inserted", total ) );
	}

	/**
	 * @see org.hibernate.persister.collection.AbstractCollectionPersister#remove(Serializable, SharedSessionContractImplementor)
	 */
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

			List<Object> params = new ArrayList<>();
			params.add( id );
			String sql = getSQLDeleteString();
			return CompletionStages.voidFuture()
					.thenCompose( s -> reactiveConnection.update( sql, params.toArray( new Object[0] )))
					.thenCompose(CompletionStages::voidFuture);
		}
		return CompletionStages.voidFuture();
	}

	/**
	 * @see org.hibernate.persister.collection.AbstractCollectionPersister#deleteRows(PersistentCollection, Serializable, SharedSessionContractImplementor)
	 */
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

		return CompletionStages.total(
				entryArray( collection, deleteByIndex ),
				entry -> loop.thenCompose( total -> reactiveConnection
						.update( getSQLDeleteRowString(), paramValues( entry, id ,session, deleteByIndex ) )
						.thenApply( deleteCount -> total + deleteCount ) ) )
				.thenAccept( total -> LOG.debugf( "Done removing rows: %s removed", total ) );
	}

	/**
	 * @see org.hibernate.persister.collection.AbstractCollectionPersister#updateRows(PersistentCollection, Serializable, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveUpdateRows(
			PersistentCollection collection,
			Serializable id,
			SharedSessionContractImplementor session) throws HibernateException {

		if ( !isInverse && collection.isRowUpdatePossible() ) {
			// TODO:  convert AbstractCollectionPersister.doUpdateRows() to reactive logic
			// update all the modified entries
			// NOTE this method call uses a JDBC connection and will fail for Map ElementCollection type
			// Generally bags and sets are the only collections that cannot be mapped to a single row in the database
			// So Maps, for instance will return isRowUpdatePossible() == TRUE

			throw new UnsupportedOperationException();
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

		CompletionStage<Integer> loop = CompletionStages.zeroFuture();
		ReactiveConnection reactiveConnection = getReactiveConnection( session );

		return CompletionStages.total(
				entryArray(collection),
				entry -> loop.thenCompose( total -> reactiveConnection
						.update( getSQLInsertRowString(), paramValues( entry, collection, id ,session ) )
						.thenApply( insertCount -> total + insertCount ) ) )
				.thenAccept( total -> LOG.debugf( "Done inserting rows: %s inserted", total ) );
	}


	/*
	 * converts the entities of a PersistentCollection into an Object[] array
	 */
	private Object[] entryArray(PersistentCollection collection) {
		List<Object> theList = new ArrayList<>();
		Iterator entries = collection.entries( this );
		while ( entries.hasNext() ) {
			final Object entry = entries.next();
			if ( collection.entryExists( entry, 0 ) ) {
				theList.add(entry);
			}
		}

		return theList.toArray( new Object[0] );
	}

	/*
	 * converts the entities of a PersistentCollection into an Object[] array for the delete row(s) use case.
	 */
	private Object[] entryArray(PersistentCollection collection, boolean deleteByIndex) {
		List<Object> theList = new ArrayList<>();
		Iterator deletes = collection.getDeletes( this, !deleteByIndex );
		while ( deletes.hasNext() ) {
			final Object entry = deletes.next();
			if ( collection.entryExists( entry, 0 ) ) {
				theList.add(entry);
			}
		}

		return theList.toArray( new Object[0] );
	}

	/*
	 * generates and returns parameter values for a reactiveRecreate() and reactiveInsertRows() methods
	 * compatible with getSQLInsertRowString() and query string
	 */
	private Object[] paramValues(Object entry, PersistentCollection collection, Serializable id, SharedSessionContractImplementor session) {
		return PreparedStatementAdaptor.bind(
				st -> {
					int i = 0;
					int loc = writeKey( st, id, 1, session );

					if ( hasIdentifier ) {
						loc = writeIdentifier( st, collection.getIdentifier( entry, i ), loc, session );
					}
					if ( hasIndex /* && !indexIsFormula */) {
						loc = writeIndex( st, collection.getIndex( entry, i, this ), loc, session );
					}
					writeElement( st, collection.getElement( entry ), loc, session );

					collection.afterRowInsert( this, entry, i );
				}
		);
	}

	/*
	 * generates and returns parameter values for a reactiveDeleteRows() method
	 * compatible with getSQLDeleteRowString() query string
	 */
	private Object[] paramValues(Object entry, Serializable id, SharedSessionContractImplementor session, boolean deleteByIndex) {
		return PreparedStatementAdaptor.bind(
				st -> {
					int loc = 1;
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
	}

}
