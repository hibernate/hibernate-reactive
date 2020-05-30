/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.query.spi.NativeSQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.param.ParameterBinder;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.sql.impl.Parameters;

import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

public class ReactiveNativeSQLQueryPlan extends NativeSQLQueryPlan {

	private final String sourceQuery;
	private final CustomQuery customQuery;

	public ReactiveNativeSQLQueryPlan(String sourceQuery, CustomQuery customQuery) {
		super(sourceQuery, customQuery);
		this.sourceQuery = sourceQuery;
		this.customQuery = customQuery;
	}

	@Override
	public int performExecuteUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Use performExecuteReactiveUpdate instead" );
	}

	public CompletionStage<Integer> performExecuteReactiveUpdate(QueryParameters queryParameters, ReactiveSession session) {
		SharedSessionContractImplementor sessionContract = session.getSharedContract();

		coordinateSharedCacheCleanup(sessionContract);

		if ( queryParameters.isCallable() ) {
			throw new IllegalArgumentException("callable not yet supported for native queries");
		}

		queryParameters.processFilters( customQuery.getSQL(), sessionContract );

		Dialect dialect = session.getDialect();
		String sql = Parameters.processParameters(
				dialect.addSqlHintOrComment(
						queryParameters.getFilteredSQL(),
						queryParameters,
						session.getFactory().getSessionFactoryOptions().isCommentsEnabled()
				),
				dialect
		);

		PreparedStatementAdaptor statement = new PreparedStatementAdaptor();
		try {
			int col = 1;
			for ( ParameterBinder binder : customQuery.getParameterValueBinders() ) {
				col += binder.bind( statement, queryParameters, sessionContract, col );
			}
		}
		catch (SQLException e) {
			//can not happen
			throw new JDBCException("error binding parameters", e);
		}
//		RowSelection selection = queryParameters.getRowSelection();
//		if ( selection != null && selection.getTimeout() != null ) {
//			statement.setQueryTimeout( selection.getTimeout() );
//		}

		return session.getReactiveConnection().update( sql, statement.getParametersAsArray() );
	}
}
