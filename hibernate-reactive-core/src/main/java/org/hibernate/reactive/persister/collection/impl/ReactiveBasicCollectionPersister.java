/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.io.Serializable;
import java.util.Iterator;
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

import static org.hibernate.reactive.util.impl.CompletionStages.total;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

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
		if ( isInverse || !isRowInsertEnabled() ) {
			return voidFuture();
		}

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Inserting collection: %s",
					MessageHelper.collectionInfoString( this, collection, id, session )
			);
		}

		ReactiveConnection reactiveConnection = getReactiveConnection( session );
		collection.preInsert( this );
		Iterator<?> entries = collection.entries(this );
		return total(
				entries,
				(entry, index) -> reactiveConnection.update(
						getSQLInsertRowString(),
						insertRowsParamValues( entry, index+1, collection, id, session )
				)
				//TODO: compose() a reactive version of collection.afterRowInsert()
		).thenAccept( total -> LOG.debugf( "Done inserting rows: %s inserted", total ) );
	}

	/**
	 * @see org.hibernate.persister.collection.AbstractCollectionPersister#remove(Serializable, SharedSessionContractImplementor)
	 */
	public CompletionStage<Void> removeReactive(Serializable id, SharedSessionContractImplementor session)
			throws HibernateException {
		if ( !isInverse && isRowDeleteEnabled() ) {
			if ( LOG.isDebugEnabled() ) {
				LOG.debugf(
						"Deleting collection: %s",
						MessageHelper.collectionInfoString( this, id, getFactory() )
				);
			}

			return getReactiveConnection( session )
					.update( getSQLDeleteString(), new Object[]{ id } )
					.thenCompose( CompletionStages::voidFuture );
		}
		return voidFuture();
	}

	/**
	 * @see org.hibernate.persister.collection.AbstractCollectionPersister#deleteRows(PersistentCollection, Serializable, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> reactiveDeleteRows(
			PersistentCollection collection,
			Serializable id,
			SharedSessionContractImplementor session) throws HibernateException {
		if ( isInverse || !isRowDeleteEnabled() ) {
			return voidFuture();
		}

		ReactiveConnection reactiveConnection = getReactiveConnection( session );
		Iterator<?> deletes = collection.getDeletes(this, !deleteByIndex() );
		return total(
				deletes,
				(entry, index) -> reactiveConnection.update(
						getSQLDeleteRowString(),
						deleteRowsParamValues( entry, index+1, id, session )
				)
		).thenAccept( total -> LOG.debugf( "Done removing rows: %s removed", total ) );
	}

	private boolean deleteByIndex() {
		return !isOneToMany() && hasIndex && !indexContainsFormula;
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
		return voidFuture();
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
			return voidFuture();
		}

		if ( !isRowDeleteEnabled() ) {
			return voidFuture();
		}

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Inserting rows of collection: %s",
					MessageHelper.collectionInfoString( this, collection, id, session )
			);
		}

		collection.preInsert( this );

		ReactiveConnection reactiveConnection = getReactiveConnection( session );
		Iterator<?> entries = collection.entries(this );
		return total(
				entries,
				(entry, index) -> reactiveConnection.update(
						getSQLInsertRowString(),
						insertRowsParamValues( entry, index, collection, id, session )
				)
				//TODO: compose() a reactive version of collection.afterRowInsert()
		).thenAccept( total -> LOG.debugf( "Done inserting rows: %s inserted", total ) );
	}

	private Object[] insertRowsParamValues(Object entry, int index,
										   PersistentCollection collection, Serializable id,
										   SharedSessionContractImplementor session) {
		int offset = 1;
 		return PreparedStatementAdaptor.bind(
				st -> {
					int loc = writeKey( st, id, offset , session );
					if ( hasIdentifier ) {
						loc = writeIdentifier( st, collection.getIdentifier( entry, index ), loc, session );
					}
					if ( hasIndex /* && !indexIsFormula */) {
						loc = writeIndex( st, collection.getIndex( entry, index, this ), loc, session );
					}
					writeElement( st, collection.getElement( entry ), loc, session );
				}
		);
	}

	private Object[] deleteRowsParamValues(Object entry, int offset, Serializable id,
										   SharedSessionContractImplementor session) {
		return PreparedStatementAdaptor.bind(
				st -> {
					int loc = offset;
					if ( hasIdentifier ) {
						writeIdentifier( st, entry, loc, session );
					}
					else {
						loc = writeKey( st, id, loc, session );
						if ( deleteByIndex() ) {
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
