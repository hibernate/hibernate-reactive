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
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.tree.SqmDeleteOrUpdateStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.sqm.mutation.internal.ReactiveHandler;

public interface ReactiveSqmMultiTableMutationStrategy extends SqmMultiTableMutationStrategy {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default int executeUpdate(
			SqmUpdateStatement<?> sqmUpdateStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteUpdate" );
	}

	/**
	 * Execute the multi-table update indicated by the passed SqmUpdateStatement
	 *
	 * @return The number of rows affected
	 * @deprecated Use {@link #buildHandler(SqmDeleteOrUpdateStatement, DomainParameterXref, DomainQueryExecutionContext)} instead
	 */
	@Deprecated(forRemoval = true, since = "7.1")
	default CompletionStage<Integer> reactiveExecuteUpdate(
			SqmUpdateStatement<?> sqmUpdateStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context){
		final MultiTableHandlerBuildResult buildResult = buildHandler( sqmUpdateStatement, domainParameterXref, context );
		return ((ReactiveHandler)buildResult.multiTableHandler()).reactiveExecute( buildResult.firstJdbcParameterBindings(), context );

	}

	@Override
	default int executeDelete(
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteDelete" );
	}

	/**
	 * Execute the multi-table update indicated by the passed SqmUpdateStatement
	 *
	 * @return The number of rows affected
	 * @deprecated Use {@link #buildHandler(SqmDeleteOrUpdateStatement, DomainParameterXref, DomainQueryExecutionContext)} instead
	 */
	@Deprecated(forRemoval = true, since = "3.1")
	default CompletionStage<Integer> reactiveExecuteDelete(
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context){
		final MultiTableHandlerBuildResult buildResult = buildHandler( sqmDeleteStatement, domainParameterXref, context );
		return ((ReactiveHandler)buildResult.multiTableHandler()).reactiveExecute( buildResult.firstJdbcParameterBindings(), context  );
	}
}
