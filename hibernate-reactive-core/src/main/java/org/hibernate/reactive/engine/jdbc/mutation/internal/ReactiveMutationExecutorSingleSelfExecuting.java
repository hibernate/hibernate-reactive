/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;


import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorSingleSelfExecuting;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.model.ReactiveSelfExecutingUpdateOperation;
import org.hibernate.sql.model.SelfExecutingUpdateOperation;
import org.hibernate.sql.model.ValuesAnalysis;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveMutationExecutorSingleSelfExecuting extends MutationExecutorSingleSelfExecuting
		implements ReactiveMutationExecutor {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final SelfExecutingUpdateOperation operation;

	public ReactiveMutationExecutorSingleSelfExecuting(
			SelfExecutingUpdateOperation operation,
			SharedSessionContractImplementor session) {
		super( operation, session );
		this.operation = operation;
	}

	@Override
	protected void performSelfExecutingOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "performReactiveSelfExecutingOperation" );
	}

	@Override
	public CompletionStage<Void> performReactiveSelfExecutingOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			SharedSessionContractImplementor session) {
		if ( inclusionChecker.include( operation.getTableDetails() ) && operation instanceof ReactiveSelfExecutingUpdateOperation ) {
			return ( (ReactiveSelfExecutingUpdateOperation) operation )
					.performReactiveMutation( getJdbcValueBindings(), valuesAnalysis, session );
		}
		return voidFuture();
	}
}
