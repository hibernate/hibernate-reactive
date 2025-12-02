/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.mutation.internal.Handler;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

/**
 * @see org.hibernate.query.sqm.mutation.internal.Handler
 */
public interface ReactiveHandler extends Handler {
	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default int execute(DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	@Override
	default int execute(JdbcParameterBindings jdbcParameterBindings, DomainQueryExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	/**
	 * Execute the multi-table update or delete indicated by the SQM AST
	 * passed in when this Handler was created.
	 *
	 * @param executionContext Contextual information needed for execution
	 *
	 * @return The "number of rows affected" count
	 */
	default CompletionStage<Integer> reactiveExecute(DomainQueryExecutionContext executionContext){
		return reactiveExecute( createJdbcParameterBindings( executionContext ), executionContext );
	}

	/**
	 * Execute the multi-table update or delete indicated by the SQM AST
	 * passed in when this Handler was created.
	 *
	 * @param jdbcParameterBindings The parameter bindings for JDBC parameters
	 * @param executionContext Contextual information needed for execution
	 * @return The "number of rows affected" count
	 */
	CompletionStage<Integer> reactiveExecute(JdbcParameterBindings jdbcParameterBindings, DomainQueryExecutionContext executionContext);
}
