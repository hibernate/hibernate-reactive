/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.ExecuteUpdateResultCheckStyle;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.loader.ast.spi.CollectionLoader;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.BasicCollectionPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoader;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoaderSubSelectFetch;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.util.impl.CompletionStages;

/**
 * A reactive {@link BasicCollectionPersister}
 */
public class ReactiveBasicCollectionPersister extends BasicCollectionPersister implements ReactiveAbstractCollectionPersister {

	private Parameters parameters() {
		return Parameters.instance( getFactory().getJdbcServices().getDialect() );
	}

	public ReactiveBasicCollectionPersister(Collection collectionBinding, CollectionDataAccess cacheAccessStrategy, PersisterCreationContext creationContext)
			throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
	}

	@Override
	protected CollectionLoader createCollectionLoader(LoadQueryInfluencers loadQueryInfluencers) {
		return createReactiveCollectionLoader( loadQueryInfluencers );
	}

	@Override
	protected CollectionLoader createSubSelectLoader(SubselectFetch subselect, SharedSessionContractImplementor session) {
		return new ReactiveCollectionLoaderSubSelectFetch(
				getAttributeMapping(),
				null,
				subselect,
				session
		);
	}

	@Override
	public CompletionStage<Void> reactiveInitialize(Object key, SharedSessionContractImplementor session) {
		return ( (ReactiveCollectionLoader) determineLoaderToUse( key, session ) )
				.reactiveLoad( key, session )
				.thenCompose( CompletionStages::voidFuture );
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
	public ExecuteUpdateResultCheckStyle getInsertCheckStyle() {
		return null;
	}

	@Override
	public ExecuteUpdateResultCheckStyle getDeleteCheckStyle() {
		return null;
	}

	@Override
	public int writeElement(PreparedStatement st, Object element, int loc, SharedSessionContractImplementor session) {
		return 0;
	}

	@Override
	public int writeIndex(PreparedStatement st, Object index, int loc, SharedSessionContractImplementor session) {
		return 0;
	}

	@Override
	public int writeIdentifier(
			PreparedStatement st,
			Object identifier,
			int loc,
			SharedSessionContractImplementor session) {
		return 0;
	}

	@Override
	public int writeKey(PreparedStatement st, Object id, int offset, SharedSessionContractImplementor session) {
		return 0;
	}

	@Override
	public int writeElementToWhere(
			PreparedStatement st,
			Object entry,
			int loc,
			SharedSessionContractImplementor session) throws SQLException {
		return 0;
	}

	@Override
	public int writeIndexToWhere(PreparedStatement st, Object entry, int loc, SharedSessionContractImplementor session) {
		return 0;
	}

	@Override
	public CompletionStage<Void> doReactiveUpdateRows(Object id, PersistentCollection collection, SharedSessionContractImplementor session) {
		return null;
	}
}
