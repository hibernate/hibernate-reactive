/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.BasicCollectionPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.loader.collection.ReactiveCollectionInitializer;
import org.hibernate.reactive.loader.collection.impl.ReactiveBatchingCollectionInitializerBuilder;
import org.hibernate.reactive.loader.collection.impl.ReactiveSubselectCollectionLoader;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.collectionInfoString;
import static org.hibernate.reactive.util.impl.CompletionStages.total;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.zeroFuture;

/**
 * A reactive {@link BasicCollectionPersister}
 */
// TODO: Check out batching logic: See AbstractCollectionPersister for usage
// boolean useBatch = expectation.canBeBatched();
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

	public CompletionStage<Void> reactiveInitialize(Serializable key, SharedSessionContractImplementor session)
			throws HibernateException {
		return getAppropriateInitializer( key, session ).reactiveInitialize( key, session );
	}

	@Override
	protected ReactiveCollectionInitializer createCollectionInitializer(LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		return ReactiveBatchingCollectionInitializerBuilder.getBuilder( getFactory() )
				.createBatchingCollectionInitializer( this, batchSize, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected ReactiveCollectionInitializer createSubselectInitializer(SubselectFetch subselect, SharedSessionContractImplementor session) {
		return new ReactiveSubselectCollectionLoader(
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
					collectionInfoString( this, collection, id, session )
			);
		}

		ReactiveConnection reactiveConnection = getReactiveConnection( session );
		//TODO: compose() reactive version of collection.preInsert()
		Iterator<?> entries = collection.entries( this );
		return total(
				entries,
				(entry, index) -> {
					if ( collection.entryExists( entry, index ) ) {
						return reactiveConnection.update(
								getSQLInsertRowString(),
								insertRowsParamValues( entry, index, collection, id, session )
						);
						//TODO: compose() reactive version of collection.afterRowInsert()
					}
					else {
						return zeroFuture();
					}
				}
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
						collectionInfoString( this, id, getFactory() )
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

		Iterator<?> deletes = collection.getDeletes( this, !deleteByIndex() );
		return total(
				deletes,
				(entry, index) -> getReactiveConnection( session ).update(
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
			// NOTE this method call uses a JDBC connection and will fail for Map ElementCollection type
			// Generally bags and sets are the only collections that cannot be mapped to a single row in the database
			// So Maps, for instance will return isRowUpdatePossible() == TRUE
			// A map of type Map<String, String> ends up calling this method, however
			// for delete/insert of rows is performed via the reactiveDeleteRows() and
			// reactiveInsertRows()... so returning a voidFuture()
			return voidFuture();
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
					collectionInfoString( this, collection, id, session )
			);
		}

		ReactiveConnection reactiveConnection = getReactiveConnection( session );
		//TODO: compose() reactive version of collection.preInsert()
		Iterator<?> entries = collection.entries(this );
		return total(
				entries,
				(entry, index) -> {
					if ( collection.needsInserting( entry, index, elementType ) ) {
						return reactiveConnection.update(
								getSQLInsertRowString(),
								insertRowsParamValues( entry, index, collection, id, session )
						);
						//TODO: compose() a reactive version of collection.afterRowInsert()
					}
					else {
						return zeroFuture();
					}
				}
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
