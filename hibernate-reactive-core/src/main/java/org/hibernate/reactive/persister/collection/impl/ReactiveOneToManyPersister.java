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
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.loader.collection.ReactiveCollectionInitializer;
import org.hibernate.reactive.loader.collection.impl.ReactiveBatchingCollectionInitializerBuilder;
import org.hibernate.reactive.loader.collection.impl.ReactiveSubselectOneToManyLoader;
import org.hibernate.reactive.pool.impl.Parameters;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

/**
 * A reactive {@link OneToManyPersister}
 */
//TODO: reactive version of writeIndex() for indexed @OneToMany associations with an @OrderColumn
public class ReactiveOneToManyPersister extends OneToManyPersister implements ReactiveAbstractCollectionPersister {

	private final Parameters parameters;

	public ReactiveOneToManyPersister(Collection collectionBinding,
									  CollectionDataAccess cacheAccessStrategy,
									  PersisterCreationContext creationContext)
			throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
		this.parameters = Parameters.instance( getFactory().getJdbcServices().getDialect() );
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
	public int writeElement(PreparedStatement st, Object element, int loc, SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeElement(st, element, loc, session);
	}

	@Override
	public int writeIndex(PreparedStatement st, Object index, int loc, SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeIndex(st, index, loc, session);
	}

	@Override
	public int writeKey(PreparedStatement st, Serializable id, int offset, SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeKey(st, id, offset, session);
	}

	@Override
	public int writeElementToWhere(PreparedStatement st, Object entry, int loc, SharedSessionContractImplementor session)
			throws SQLException {
		return super.writeElementToWhere(st, entry, loc, session);
	}

	@Override
	public int writeIndexToWhere(PreparedStatement st, Object entry, int loc, SharedSessionContractImplementor session)
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
}
