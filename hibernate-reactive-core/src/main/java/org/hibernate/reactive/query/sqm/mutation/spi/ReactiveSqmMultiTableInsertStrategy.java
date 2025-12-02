/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.spi;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandlerBuildResult;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;

public interface ReactiveSqmMultiTableInsertStrategy extends SqmMultiTableInsertStrategy {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default int executeInsert(
			SqmInsertStatement<?> sqmInsertStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteInsert" );
	}

	/**
	 * Execute the multi-table insert indicated by the passed SqmInsertStatement
	 *
	 * @return The number of rows affected
	 * @deprecated Uses {@link #buildHandler(SqmInsertStatement, DomainParameterXref, DomainQueryExecutionContext)} instead
	 */
	@Deprecated(forRemoval = true, since = "3.1")
	default CompletionStage<Integer> reactiveExecuteInsert(
			SqmInsertStatement<?> sqmInsertStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context){
		final MultiTableHandlerBuildResult buildResult = buildHandler( sqmInsertStatement, domainParameterXref, context );
		return ((ReactiveHandler)buildResult.multiTableHandler()).reactiveExecute( buildResult.firstJdbcParameterBindings(), context );
	}
}
