/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.custom.impl;

import org.hibernate.HibernateException;
import org.hibernate.QueryException;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.custom.CustomLoader;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.reactive.loader.CachingReactiveLoader;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.sql.impl.Parameters.processParameters;

/**
 * A reactive {@link org.hibernate.loader.Loader} for native SQL queries.
 *
 * @author Gavin King
 */
public class ReactiveCustomLoader extends CustomLoader implements CachingReactiveLoader {

	public ReactiveCustomLoader(CustomQuery customQuery, SessionFactoryImplementor factory) {
		super(customQuery, factory);
	}

	public CompletionStage<List<Object>> reactiveList(
			SharedSessionContractImplementor session,
			QueryParameters queryParameters) throws HibernateException {
		return reactiveListIgnoreQueryCache( getSQLString(), getQueryIdentifier(), session, queryParameters );
	}

	@Override @SuppressWarnings("unchecked")
	public List<Object> processResultSet(ResultSet resultSet,
										 QueryParameters queryParameters,
										 SharedSessionContractImplementor session,
										 boolean returnProxies,
										 ResultTransformer forcedResultTransformer,
										 int maxRows, List<AfterLoadAction> afterLoadActions) throws SQLException {
		return super.processResultSet(resultSet, queryParameters, session, returnProxies,
				forcedResultTransformer, maxRows, afterLoadActions);
	}

	@Override
	public String preprocessSQL(String sql,
								QueryParameters queryParameters,
								SessionFactoryImplementor factory,
								List<AfterLoadAction> afterLoadActions) {
		String processed = super.preprocessSQL(sql, queryParameters, factory, afterLoadActions);
		return processParameters( processed, factory.getJdbcServices().getDialect() );
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
	public void bindToPreparedStatement(PreparedStatement adaptor,
										QueryParameters queryParameters,
										LimitHandler limitHandler,
										SharedSessionContractImplementor session) throws SQLException {
		super.bindPreparedStatement(adaptor, queryParameters, limitHandler, session);
	}
}
