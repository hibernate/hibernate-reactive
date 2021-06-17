/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import org.hibernate.HibernateException;
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
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.loader.collection.ReactiveCollectionInitializer;
import org.hibernate.reactive.loader.collection.impl.ReactiveBatchingCollectionInitializerBuilder;
import org.hibernate.reactive.loader.collection.impl.ReactiveSubselectOneToManyLoader;
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
 * A reactive {@link OneToManyPersister}
 */
//TODO: reactive version of writeIndex() for indexed @OneToMany associations with an @OrderColumn
public class ReactiveOneToManyPersister extends OneToManyPersister
		implements ReactiveAbstractCollectionPersister {

	private Parameters parameters() {
		return Parameters.instance( getFactory().getJdbcServices().getDialect() );
	}

	public ReactiveOneToManyPersister(Collection collectionBinding,
									  CollectionDataAccess cacheAccessStrategy,
									  PersisterCreationContext creationContext)
			throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
	}

	public CompletionStage<Void> reactiveInitialize(Serializable key,
													SharedSessionContractImplementor session) {
		return getAppropriateInitializer( key, session ).reactiveInitialize( key, session );
	}

	@Override
	protected ReactiveCollectionInitializer createCollectionInitializer(LoadQueryInfluencers loadQueryInfluencers) {
		return ReactiveBatchingCollectionInitializerBuilder.getBuilder( getFactory() )
				.createBatchingOneToManyInitializer( this, batchSize, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected ReactiveCollectionInitializer createSubselectInitializer(SubselectFetch subselect,
																	   SharedSessionContractImplementor session) {
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
	protected String generateInsertRowString() {
		String sql = super.generateInsertRowString();
		return parameters().process( sql );
	}

	@Override
	protected String generateUpdateRowString() {
		String sql = super.generateUpdateRowString();
		return parameters().process( sql );
	}

	@Override
	protected String generateDeleteRowString() {
		String sql = super.generateDeleteRowString();
		return parameters().process( sql );
	}

	@Override
	protected String generateDeleteString() {
		String sql = super.generateDeleteString();
		return parameters().process( sql );
	}

	@Override
	public String getSQLInsertRowString() {
		return super.getSQLInsertRowString();
	}

	@Override
	public String getSQLUpdateRowString() {
		return super.getSQLUpdateRowString();
	}

	@Override
	public String getSQLDeleteRowString() {
		return super.getSQLDeleteRowString();
	}

	@Override
	public String getSQLDeleteString() {
		return super.getSQLDeleteString();
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
	 * @see OneToManyPersister#doUpdateRows(Serializable, PersistentCollection, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Void> doReactiveUpdateRows(Serializable id, PersistentCollection collection,
														 SharedSessionContractImplementor session) {

		List<Object> entries = entryList( collection );
		if ( !needsUpdate( collection, entries ) ) {
			return voidFuture();
		}

		CompletionStage<Void> result = voidFuture();
		if ( isRowDeleteEnabled() ) {
			Expectation deleteExpectation = appropriateExpectation( getDeleteCheckStyle() );
			result = result.thenCompose( v -> loop( 0, entries.size(),
					i -> collection.needsUpdating( entries.get( i ), i, elementType ),  // will still be issued when it used to be null
					i -> {
							Object entry = entries.get(i);
							int offset = 1;
							return getReactiveConnection( session ).update(
									getSQLDeleteRowString(),
									PreparedStatementAdaptor.bind( st -> {
										int loc = writeKey( st, id, offset, session );
										writeElementToWhere( st, collection.getSnapshotElement( entry, i ), loc, session );
									} ),
									deleteExpectation.canBeBatched(),
									new ExpectationAdaptor( deleteExpectation, getSQLDeleteRowString(), getSQLExceptionConverter() )
							);
					}
			) );
		}
		if ( isRowInsertEnabled() ) {
			Expectation insertExpectation = appropriateExpectation( getInsertCheckStyle() );
			result = result.thenCompose( v -> loop( 0, entries.size(),
					i -> collection.needsUpdating( entries.get( i ), i, elementType ),  // will still be issued when it used to be null
					i -> {
						Object entry = entries.get(i);
						return getReactiveConnection( session ).update(
								getSQLInsertRowString(),
								PreparedStatementAdaptor.bind( st -> {
									int offset = 1;
									offset += insertExpectation.prepare( st );
									int loc = writeKey( st, id, offset, session );
									if ( hasIndex && !indexContainsFormula ) {
										loc = writeIndexToWhere( st, collection.getIndex( entry, i, this ), loc, session );
									}
									writeElementToWhere( st, collection.getElement( entry ), loc, session );
								} ),
								insertExpectation.canBeBatched(),
								new ExpectationAdaptor( insertExpectation, getSQLInsertRowString(), getSQLExceptionConverter() )
						);
					}
			) );
		}
		return result;
	}

	@Override
	public CompletionStage<Void> recreateReactive(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session) throws HibernateException {
		return reactiveWriteIndex( collection, id, session,
				ReactiveAbstractCollectionPersister.super.recreateReactive( collection, id, session ) );
	}

	@Override
	public CompletionStage<Void> reactiveInsertRows(PersistentCollection collection, Serializable id, SharedSessionContractImplementor session) throws HibernateException {
		return reactiveWriteIndex( collection, id, session,
				ReactiveAbstractCollectionPersister.super.reactiveInsertRows( collection, id, session ) );
	}

	private CompletionStage<Void> reactiveWriteIndex(PersistentCollection collection, Serializable id,
													 SharedSessionContractImplementor session,
													 CompletionStage<Void> stage) {
		if ( isInverse
				&& hasIndex && !indexContainsFormula
				&& ArrayHelper.countTrue( indexColumnIsSettable ) > 0 ) {

			List<Object> entries = entryList( collection );
			if ( entries.isEmpty() ) {
				return stage;
			}

			Expectation expectation = appropriateExpectation( getUpdateCheckStyle() );
			return stage.thenCompose( v -> loop( 0, entries.size(),
					index -> {
						Object entry = entries.get( index );
						return entry != null && collection.entryExists( entry, index );
					},
					index -> {
						Object entry = entries.get( index );
						return getReactiveConnection( session ).update(
								getSQLUpdateRowString(),
								PreparedStatementAdaptor.bind( st -> {
									int offset = 1;
									offset += expectation.prepare( st );
									if ( hasIdentifier ) {
										offset = writeIdentifier(
												st,
												collection.getIdentifier( entry, index ),
												offset,
												session
										);
									}
									offset = writeIndex(
											st,
											collection.getIndex( entry, index, this ),
											offset,
											session
									);
									offset = writeElement( st, collection.getElement(entry), offset, session );
								} ),
								expectation.canBeBatched(),
								new ExpectationAdaptor( expectation, getSQLUpdateRowString(), getSQLExceptionConverter() )
						);
					}
			) );
		}
		else {
			return stage;
		}
	}
}
