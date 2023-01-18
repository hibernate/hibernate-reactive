/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.spi;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

public interface ReactiveSqmMultiTableMutationStrategy extends SqmMultiTableMutationStrategy {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default int executeUpdate(
			SqmUpdateStatement<?> sqmUpdateStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteUpdate" );
	}

	@Override
	CompletionStage<Integer> reactiveExecuteUpdate(
			SqmUpdateStatement<?> sqmUpdateStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context);

	@Override
	default int executeDelete(
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteDelete" );
	}

	CompletionStage<Integer> reactiveExecuteDelete(
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context);
}
