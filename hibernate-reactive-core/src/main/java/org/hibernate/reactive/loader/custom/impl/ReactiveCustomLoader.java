/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.custom.impl;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.QueryException;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.custom.CustomLoader;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.loader.custom.Return;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.param.ParameterBinder;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.reactive.loader.CachingReactiveLoader;
import org.hibernate.reactive.loader.ReactiveLoaderBasedLoader;
import org.hibernate.reactive.loader.ReactiveLoaderBasedResultSetProcessor;
import org.hibernate.reactive.loader.ReactiveResultSetProcessor;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

/**
 * A reactive {@link org.hibernate.loader.Loader} for native SQL queries.
 *
 * @author Gavin King
 */
public class ReactiveCustomLoader<T> extends CustomLoader implements CachingReactiveLoader<T>, ReactiveLoaderBasedLoader {

	private static class ProcessedCustomQuery implements CustomQuery {
		private final CustomQuery delegate;
		private final String processedSQL;

		ProcessedCustomQuery(CustomQuery customQuery, Parameters parameters) {
			this.delegate = customQuery;
			this.processedSQL = parameters.process( customQuery.getSQL() );
		}

		@Override
		public String getSQL() {
			return processedSQL;
		}

		@Override
		public Set<String> getQuerySpaces() {
			return delegate.getQuerySpaces();
		}

		@Override
		public List<ParameterBinder> getParameterValueBinders() {
			return delegate.getParameterValueBinders();
		}

		@Override
		public List<Return> getCustomQueryReturns() {
			return delegate.getCustomQueryReturns();
		}
	}

	private final ReactiveResultSetProcessor resultSetProcessor;
	private final Parameters parameters;

	public ReactiveCustomLoader(CustomQuery customQuery, SessionFactoryImplementor factory) {
		super( new ProcessedCustomQuery( customQuery, Parameters.instance( factory.getJdbcServices().getDialect() ) ), factory);
		this.resultSetProcessor = new ReactiveLoaderBasedResultSetProcessor( this );
		this.parameters = Parameters.instance( factory.getJdbcServices().getDialect() );
	}

	@Override
	public Parameters parameters() {
		return parameters;
	}

	public CompletionStage<List<T>> reactiveList(
			SharedSessionContractImplementor session,
			QueryParameters queryParameters) throws HibernateException {
		return reactiveListIgnoreQueryCache( getSQLString(), getQueryIdentifier(), session, queryParameters );
	}

	@Override
	public List<Object> processResultSet(ResultSet resultSet,
										 QueryParameters queryParameters,
										 SharedSessionContractImplementor session,
										 boolean returnProxies,
										 ResultTransformer forcedResultTransformer,
										 int maxRows, List<AfterLoadAction> afterLoadActions) {
		throw new UnsupportedOperationException( "use #reactiveProcessResultSet instead." );
	}

	@Override
	public ReactiveResultSetProcessor getReactiveResultSetProcessor() {
		return resultSetProcessor;
	}

	@Override
	public String preprocessSQL(String sql,
								QueryParameters queryParameters,
								SessionFactoryImplementor factory,
								List<AfterLoadAction> afterLoadActions) {
		return super.preprocessSQL(sql, queryParameters, factory, afterLoadActions);
	}

	@Override
	public void discoverTypes(QueryParameters queryParameters, ResultSet resultSet) {
		if ( queryParameters.hasAutoDiscoverScalarTypes() ) {
			super.autoDiscoverTypes(resultSet);
		}
	}

	@Override
	public boolean[] includeInResultRow() {
		return super.includeInResultRow();
	}

	@Override @SuppressWarnings("unchecked")
	public List<Object> getReactiveResultFromQueryCache(SharedSessionContractImplementor session, QueryParameters queryParameters, Set<Serializable> querySpaces, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key) {
		return super.getResultFromQueryCache(session, queryParameters, querySpaces, resultTypes, queryCache, key);
	}

	@Override
	public void putReactiveResultInQueryCache(SharedSessionContractImplementor session, QueryParameters queryParameters, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key, List<Object> cachableList) {
		super.putResultInQueryCache(session, queryParameters, resultTypes, queryCache, key, cachableList);
	}

	@Override
	public ResultTransformer resolveResultTransformer(ResultTransformer resultTransformer) {
		return super.resolveResultTransformer(resultTransformer);
	}

	@Override
	public String[] getResultRowAliases() {
		return super.getResultRowAliases();
	}

	@Override
	public boolean areResultSetRowsTransformedImmediately() {
		return super.areResultSetRowsTransformedImmediately();
	}

	@Override @SuppressWarnings("unchecked")
	public List<Object> getResultList(List results, ResultTransformer resultTransformer) throws QueryException {
		return super.getResultList(results, resultTransformer);
	}

	@Override
	public void bindToPreparedStatement(PreparedStatement adaptor,
										QueryParameters queryParameters,
										LimitHandler limitHandler,
										SharedSessionContractImplementor session) throws SQLException {
		super.bindPreparedStatement(adaptor, queryParameters, limitHandler, session);
	}

	@Override
	public Loadable[] getEntityPersisters() {
		return super.getEntityPersisters();
	}

	@Override
	public CollectionPersister[] getCollectionPersisters() {
		return super.getCollectionPersisters();
	}

	@Override
	public boolean isSubselectLoadingEnabled() {
		return super.isSubselectLoadingEnabled();
	}

	@Override
	public List<Object> getRowsFromResultSet(
			ResultSet rs,
			QueryParameters queryParameters,
			SharedSessionContractImplementor session,
			boolean returnProxies,
			ResultTransformer forcedResultTransformer,
			int maxRows,
			List<Object> hydratedObjects,
			List<EntityKey[]> subselectResultKeys) throws SQLException {
		return super.getRowsFromResultSet( rs,
				queryParameters,
				session,
				returnProxies,
				forcedResultTransformer,
				maxRows,
				hydratedObjects,
				subselectResultKeys);
	}

	@Override
	public void createSubselects(
			final List keys,
			final QueryParameters queryParameters,
			final SharedSessionContractImplementor session) {
		super.createSubselects( keys, queryParameters, session );
	}

	@Override
	public void endCollectionLoad(Object resultSetId, SharedSessionContractImplementor session, CollectionPersister collectionPersister) {
		super.endCollectionLoad( resultSetId, session, collectionPersister );
	}
}
