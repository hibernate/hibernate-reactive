/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.ExecuteUpdateResultCheckStyle;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.jdbc.Expectation;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.BasicCollectionPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.loader.collection.ReactiveCollectionInitializer;
import org.hibernate.reactive.loader.collection.impl.ReactiveBatchingCollectionInitializerBuilder;
import org.hibernate.reactive.loader.collection.impl.ReactiveSubselectCollectionLoader;
import org.hibernate.reactive.pool.impl.Parameters;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.jdbc.Expectations.appropriateExpectation;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive {@link BasicCollectionPersister}
 */
public class ReactiveBasicCollectionPersister extends BasicCollectionPersister
		implements ReactiveAbstractCollectionPersister {
	private final Parameters parameters;

	public ReactiveBasicCollectionPersister(Collection collectionBinding,
											CollectionDataAccess cacheAccessStrategy,
											PersisterCreationContext creationContext)
			throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
		this.parameters = Parameters.instance( getFactory().getJdbcServices().getDialect() );
	}

	public CompletionStage<Void> reactiveInitialize(Serializable key,
													SharedSessionContractImplementor session) {
		return getAppropriateInitializer( key, session ).reactiveInitialize( key, session );
	}

	@Override
	protected ReactiveCollectionInitializer createCollectionInitializer(LoadQueryInfluencers loadQueryInfluencers) {
		return ReactiveBatchingCollectionInitializerBuilder.getBuilder( getFactory() )
				.createBatchingCollectionInitializer( this, batchSize, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected ReactiveCollectionInitializer createSubselectInitializer(SubselectFetch subselect,
																	   SharedSessionContractImplementor session) {
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

	protected ReactiveCollectionInitializer getAppropriateInitializer(Serializable key,
																	  SharedSessionContractImplementor session) {
		return (ReactiveCollectionInitializer) super.getAppropriateInitializer(key, session);
	}


	@Override
	public boolean isRowDeleteEnabled() {
		return super.isRowDeleteEnabled();
	}

	@Override
	public boolean isRowInsertEnabled() {
		return super.isRowInsertEnabled();
	}

	@Override
	public boolean hasIdentifier() {
		return super.hasIdentifier;
	}

	@Override
	public boolean indexContainsFormula() {
		return super.indexContainsFormula;
	}

	@Override
	public int writeElement(PreparedStatement st, Object element, int loc,
							SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeElement(st, element, loc, session);
	}

	@Override
	public int writeIndex(PreparedStatement st, Object index, int loc,
						  SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeIndex(st, index, loc, session);
	}

	@Override
	public int writeKey(PreparedStatement st, Serializable id, int offset,
						SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeKey(st, id, offset, session);
	}

	@Override
	public int writeElementToWhere(PreparedStatement st, Object entry, int loc,
								   SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeElementToWhere(st, entry, loc, session);
	}

	@Override
	public int writeIndexToWhere(PreparedStatement st, Object entry, int loc,
								 SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeIndexToWhere(st, entry, loc, session);
	}

	@Override
	public String getSQLInsertRowString() {
		String sql = super.getSQLInsertRowString();
		return parameters.process( sql );
	}

	@Override
	public String getSQLDeleteRowString() {
		String sql = super.getSQLDeleteRowString();
		return parameters.process( sql );
	}

	@Override
	public String getSQLDeleteString() {
		String sql = super.getSQLDeleteString();
		return parameters.process( sql );
	}

	@Override
	public String getSQLUpdateRowString() {
		String sql = super.getSQLUpdateRowString();
		return parameters.process( sql );
	}

	@Override
	public ExecuteUpdateResultCheckStyle getInsertCheckStyle() {
		return super.getInsertCheckStyle();
	}

	@Override
	public ExecuteUpdateResultCheckStyle getDeleteCheckStyle() {
		return super.getDeleteCheckStyle();
	}

	/**
	 * @see BasicCollectionPersister#doUpdateRows(Serializable, PersistentCollection, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> doReactiveUpdateRows(Serializable id, PersistentCollection collection,
														 SharedSessionContractImplementor session) {
		if ( ArrayHelper.isAllFalse( elementColumnIsSettable ) ) {
			return voidFuture();
		}

		List<Object> entries = entryList( collection );
		if ( !needsUpdate( collection, entries ) ) {
			return voidFuture();
		}

		Expectation expectation = appropriateExpectation( getUpdateCheckStyle() );
		boolean useBatch = expectation.canBeBatched() && session.getConfiguredJdbcBatchSize() > 1;
		return loop(
				orderedIndices( collection, entries ),
				index -> {
					Object entry = entries.get( index );
					if ( collection.needsUpdating( entry, index, elementType ) ) {
						return getReactiveConnection( session ).update(
								getSQLUpdateRowString(),
								updateRowsParamValues( entry, index, collection, id, session ),
								useBatch,
								new ExpectationAdaptor( expectation, getSQLUpdateRowString(), getSQLExceptionConverter() )
						);
					}
					else {
						return voidFuture();
					}
				}
		);
	}

	private Integer[] orderedIndices(PersistentCollection collection, List<Object> elements) {
		Integer[] indices = new Integer[ elements.size() ];
		if ( collection.isElementRemoved() ) {
			// the update should be done starting from the end of the list
			for ( int i = elements.size() - 1, j = 0; i >= 0; i-- ) {
				indices[j++] = i;
			}
		}
		else {
			for ( int i = 0, j = 0; i < elements.size(); i++ ) {
				indices[j++] = i;
			}
		}
		return indices;
	}

	private Object[] updateRowsParamValues(Object entry, int i, PersistentCollection collection, Serializable id,
										   SharedSessionContractImplementor session) {
		int offset = 1;
		return PreparedStatementAdaptor.bind( st -> {
			int loc = writeElement( st, collection.getElement( entry ), offset, session );
			if ( hasIdentifier ) {
				writeIdentifier( st, collection.getIdentifier( entry, i ), loc, session );
			}
			else {
				loc = writeKey(st, id, loc, session);
				if ( hasIndex && !indexContainsFormula ) {
					writeIndexToWhere( st, collection.getIndex( entry, i, this ), loc, session );
				}
				else {
					writeElementToWhere( st, collection.getSnapshotElement( entry, i ), loc, session );
				}
			}
		} );
	}
}
