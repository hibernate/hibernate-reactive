/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.engine.query.spi.NativeSQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.param.ParameterBinder;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveQueryExecutor;

import java.util.concurrent.CompletionStage;

public class ReactiveNativeSQLQueryPlan extends NativeSQLQueryPlan {

	private final String sourceQuery;
	private final CustomQuery customQuery;

	private Parameters parser;

	public ReactiveNativeSQLQueryPlan(String sourceQuery, CustomQuery customQuery) {
		super(sourceQuery, customQuery);
		this.sourceQuery = sourceQuery;
		this.customQuery = customQuery;
	}

	@Override
	public int performExecuteUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Use performExecuteReactiveUpdate instead" );
	}

	public CompletionStage<Integer> performExecuteReactiveUpdate(QueryParameters queryParameters,
																 ReactiveQueryExecutor session) {
		SharedSessionContractImplementor sessionContract = session.getSharedContract();

		session.addBulkCleanupAction( new BulkOperationCleanupAction(
				session.getSharedContract(),
				getCustomQuery().getQuerySpaces()
		) );

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

		sql = process( session, sql, params );
		return session.getReactiveConnection().update( sql, params );
	}

	private String process(ReactiveQueryExecutor session, String sql, Object[] params) {
		return Parameters.instance( session.getDialect() ).process( sql, params.length );
	}
}
