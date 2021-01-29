/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.hql.impl;

import org.hibernate.HibernateException;
import org.hibernate.QueryException;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.engine.spi.*;
import org.hibernate.hql.internal.ast.QueryTranslatorImpl;
import org.hibernate.hql.internal.ast.tree.SelectClause;
import org.hibernate.loader.hql.QueryLoader;
import org.hibernate.loader.plan.exec.query.spi.NamedParameterContext;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.reactive.loader.ReactiveLoaderBasedLoader;
import org.hibernate.reactive.loader.CachingReactiveLoader;
import org.hibernate.reactive.loader.ReactiveLoaderBasedResultSetProcessor;
import org.hibernate.reactive.loader.ReactiveResultSetProcessor;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * A reactive {@link QueryLoader} for HQL queries.
 */
public class ReactiveQueryLoader extends QueryLoader implements CachingReactiveLoader, ReactiveLoaderBasedLoader {

	private final QueryTranslatorImpl queryTranslator;
	private final SessionFactoryImplementor factory;
	private final SelectClause selectClause;
	private final ReactiveResultSetProcessor resultSetProcessor;
	private final Parameters parameters;

	public ReactiveQueryLoader(
			QueryTranslatorImpl queryTranslator,
			SessionFactoryImplementor factory,
			SelectClause selectClause) {
		super( queryTranslator, factory, selectClause );
		this.queryTranslator = queryTranslator;
		this.factory = factory;
		this.parameters = Parameters.instance( factory.getJdbcServices().getDialect() );
		this.selectClause = selectClause;
		this.resultSetProcessor = new ReactiveLoaderBasedResultSetProcessor( this ) {
			public CompletionStage<List<Object>> reactiveExtractResults(ResultSet rs,
																		SharedSessionContractImplementor session,
																		QueryParameters queryParameters,
																		NamedParameterContext namedParameterContext,
																		boolean returnProxies, boolean readOnly,
																		ResultTransformer forcedResultTransformer,
																		List<AfterLoadAction> afterLoadActionList) throws SQLException {
				final RowSelection rowSelection = queryParameters.getRowSelection();
				final ResultSet resultSetPreprocessed = preprocessResultSet(
						rs,
						rowSelection,
						getLimitHandler( rowSelection ),
						false,
						session
				);
				return super.reactiveExtractResults(
						resultSetPreprocessed,
						session,
						queryParameters,
						namedParameterContext,
						returnProxies,
						readOnly,
						forcedResultTransformer,
						afterLoadActionList
				);
			}
		};
	}

	@Override
	public Parameters parameters() {
		return parameters;
	}

	public CompletionStage<List<Object>> reactiveList(
			SharedSessionContractImplementor session,
			QueryParameters queryParameters) throws HibernateException {
		checkQuery( queryParameters );
		return reactiveList(
				session,
				queryParameters,
				queryTranslator.getQuerySpaces(),
				selectClause.getQueryReturnTypes()
		);
	}

	/**
	 * Return the query results, using the query cache, called
	 * by subclasses that implement cacheable queries
	 * @see QueryLoader#list(SharedSessionContractImplementor, QueryParameters, Set, Type[])
	 */
	protected CompletionStage<List<Object>> reactiveList(
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final Set<Serializable> querySpaces,
			final Type[] resultTypes) throws HibernateException {

		final boolean cacheable = factory.getSessionFactoryOptions().isQueryCacheEnabled()
				&& queryParameters.isCacheable();

		String processedSQL = parameters().process( getSQLString() );
		if ( cacheable ) {
			return reactiveListUsingQueryCache( processedSQL, getQueryIdentifier(), session, queryParameters, querySpaces, resultTypes );
		}
		else {
			return reactiveListIgnoreQueryCache( processedSQL, getQueryIdentifier(), session, queryParameters );
		}
	}

	@Override
	public List<Object> processResultSet(ResultSet resultSet,
										 QueryParameters queryParameters,
										 SharedSessionContractImplementor session,
										 boolean returnProxies,
										 ResultTransformer forcedResultTransformer,
										 int maxRows, List<AfterLoadAction> afterLoadActions) throws SQLException {
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
	public void createSubselects(List keys, QueryParameters queryParameters, SharedSessionContractImplementor session) {
		super.createSubselects( keys, queryParameters, session );
	}

	@Override
	public void endCollectionLoad(Object resultSetId, SharedSessionContractImplementor session, CollectionPersister collectionPersister) {
		super.endCollectionLoad( resultSetId, session, collectionPersister );
	}
}
