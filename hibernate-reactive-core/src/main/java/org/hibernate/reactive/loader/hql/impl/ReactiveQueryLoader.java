/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.hql.impl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LockOptions;
import org.hibernate.QueryException;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.*;
import org.hibernate.hql.internal.ast.QueryTranslatorImpl;
import org.hibernate.hql.internal.ast.tree.SelectClause;
import org.hibernate.loader.hql.QueryLoader;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.loader.CachingReactiveLoader;
import org.hibernate.reactive.sql.impl.Parameters;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * A reactive {@link QueryLoader} for HQL queries.
 */
public class ReactiveQueryLoader extends QueryLoader implements CachingReactiveLoader {

	private final QueryTranslatorImpl queryTranslator;
	private final SessionFactoryImplementor factory;
	private final SelectClause selectClause;

	public ReactiveQueryLoader(
			QueryTranslatorImpl queryTranslator,
			SessionFactoryImplementor factory,
			SelectClause selectClause) {
		super( queryTranslator, factory, selectClause );
		this.queryTranslator = queryTranslator;
		this.factory = factory;
		this.selectClause = selectClause;
	}

	public CompletionStage<List<Object>> reactiveList(
			SessionImplementor session,
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
			final SessionImplementor session,
			final QueryParameters queryParameters,
			final Set<Serializable> querySpaces,
			final Type[] resultTypes) throws HibernateException {

		final boolean cacheable = factory.getSessionFactoryOptions().isQueryCacheEnabled()
				&& queryParameters.isCacheable();

		if ( cacheable ) {
			return reactiveListUsingQueryCache( getSQLString(), getQueryIdentifier(), session, queryParameters, querySpaces, resultTypes );
		}
		else {
			return reactiveListIgnoreQueryCache( getSQLString(), getQueryIdentifier(), session, queryParameters );
		}
	}

	@Override @SuppressWarnings("unchecked")
	public List<Object> processResultSet(ResultSet rs,
										 QueryParameters queryParameters,
										 SharedSessionContractImplementor session,
										 boolean returnProxies,
										 ResultTransformer forcedResultTransformer,
										 int maxRows, List<AfterLoadAction> afterLoadActions) throws SQLException {
		final RowSelection rowSelection = queryParameters.getRowSelection();
		final ResultSet resultSetPreprocessed = preprocessResultSet(
				rs,
				rowSelection,
				getLimitHandler( rowSelection )
		);
		return super.processResultSet(resultSetPreprocessed, queryParameters, session, returnProxies,
				forcedResultTransformer, maxRows, afterLoadActions);
	}

	@Override
	public String preprocessSQL(String sql,
								QueryParameters queryParameters,
								SessionFactoryImplementor factory,
								List<AfterLoadAction> afterLoadActions) {
		return Parameters.processParameters(
				super.preprocessSQL(sql, queryParameters, factory, afterLoadActions),
				factory.getDialect()
		);
	}

	@Override
	public boolean[] includeInResultRow() {
		return super.includeInResultRow();
	}

	@Override @SuppressWarnings("unchecked")
	public List<Object> getResultFromQueryCache(SessionImplementor session, QueryParameters queryParameters, Set<Serializable> querySpaces, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key) {
		return super.getResultFromQueryCache(session, queryParameters, querySpaces, resultTypes, queryCache, key);
	}

	@Override
	public void putResultInQueryCache(SessionImplementor session, QueryParameters queryParameters, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key, List<Object> cachableList) {
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
	public Object[] toParameterArray(QueryParameters queryParameters, SharedSessionContractImplementor session) {
		PreparedStatementAdaptor adaptor = new PreparedStatementAdaptor();
		try {
			bindPreparedStatement(
					adaptor,
					queryParameters,
					getLimitHandler(queryParameters.getRowSelection()),
					session
			);
			return adaptor.getParametersAsArray();
		}
		catch (SQLException e) {
				//can never happen
				throw new JDBCException("error binding parameters", e);
		}
	}

	/**
	 * This is based on private method,
	 * {@link Loader#processResultSet(ResultSet, RowSelection, LimitHandler limitHandler, boolean, SharedSessionContractImplementor),
	 */
	private ResultSet preprocessResultSet(
			ResultSet resultSet,
			final RowSelection selection,
			final LimitHandler limitHandler
	) throws SQLException, HibernateException {
		if ( !limitHandler.supportsLimitOffset()
				|| !LimitHelper.useLimit( limitHandler, selection ) ) {
			for (int i = 0,
				 firstRow = LimitHelper.getFirstRow(selection);
				 i < firstRow; i++ ) {
				resultSet.next();
			}
		}
		return resultSet;
	}

	/**
	 * This is based on the code related to binding a PreparedStatement in {@link Loader#}prepareQueryStatement},
	 * with modifications.
	 */
	private final PreparedStatement bindPreparedStatement(
			final PreparedStatement st,
			final QueryParameters queryParameters,
			final LimitHandler limitHandler,
			final SharedSessionContractImplementor session) throws SQLException, HibernateException {

		final Dialect dialect = getFactory().getDialect();
		final RowSelection selection = queryParameters.getRowSelection();
		final boolean callable = queryParameters.isCallable();

		int col = 1;
		//TODO: can we limit stored procedures ?!
		col += limitHandler.bindLimitParametersAtStartOfQuery( selection, st, col );

		if ( callable ) {
			col = dialect.registerResultSetOutParameter( (CallableStatement) st, col );
		}

		col += bindParameterValues( st, queryParameters, col, session );

		col += limitHandler.bindLimitParametersAtEndOfQuery( selection, st, col );

		limitHandler.setMaxRows( selection, st );

		// no support for these options in Reactive
//		if ( selection != null ) {
//			if ( selection.getTimeout() != null ) {
//				st.setQueryTimeout( selection.getTimeout() );
//			}
//			if ( selection.getFetchSize() != null ) {
//				st.setFetchSize( selection.getFetchSize() );
//			}
//		}

		// handle lock timeout...
		LockOptions lockOptions = queryParameters.getLockOptions();
		if ( lockOptions != null ) {
			if ( lockOptions.getTimeOut() != LockOptions.WAIT_FOREVER ) {
				if ( !dialect.supportsLockTimeouts() ) {
					if ( LOG.isDebugEnabled() ) {
						LOG.debugf(
								"Lock timeout [%s] requested but dialect reported to not support lock timeouts",
								lockOptions.getTimeOut()
						);
					}
				}
				else if ( dialect.isLockTimeoutParameterized() ) {
					st.setInt( col++, lockOptions.getTimeOut() );
				}
			}
		}

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Bound [{0}] parameters total", col );
		}

		return st;
	}

}
