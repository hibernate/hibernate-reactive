/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.query.spi.NativeSQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.param.ParameterBinder;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.session.ReactiveSession;

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

		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			int col = 1;
			for ( ParameterBinder binder : customQuery.getParameterValueBinders() ) {
				col += binder.bind( statement, queryParameters, sessionContract, col );
			}
		} );

//		RowSelection selection = queryParameters.getRowSelection();
//		if ( selection != null && selection.getTimeout() != null ) {
//			statement.setQueryTimeout( selection.getTimeout() );
//		}

		boolean commentsEnabled = session.getFactory().getSessionFactoryOptions().isCommentsEnabled();
		String sql = session.getDialect()
				.addSqlHintOrComment( queryParameters.getFilteredSQL(), queryParameters, commentsEnabled );

		return session.getReactiveConnection().update( sql, params );
	}
}
