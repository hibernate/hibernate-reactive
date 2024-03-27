/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.env.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;

import static org.hibernate.reactive.engine.jdbc.ResultsCheckerUtil.checkResults;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.AbstractMutationExecutor
 */
public interface ReactiveMutationExecutor extends MutationExecutor {

	@Override
	default GeneratedValues execute(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		throw make( Log.class, MethodHandles.lookup() ).nonReactiveMethodCall( "executeReactive" );
	}

	default CompletionStage<GeneratedValues> executeReactive(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return performReactiveNonBatchedOperations( modelReference, valuesAnalysis, inclusionChecker, resultChecker, session )
				.thenCompose( generatedValues -> performReactiveSelfExecutingOperations( valuesAnalysis, inclusionChecker, session )
									  .thenCompose( v -> performReactiveBatchedOperations( valuesAnalysis, inclusionChecker, resultChecker, session ) )
									  .thenApply( v -> generatedValues )
				);
	}

	default CompletionStage<GeneratedValues> performReactiveNonBatchedOperations(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return nullFuture();
	}

	default CompletionStage<Void> performReactiveSelfExecutingOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			SharedSessionContractImplementor session) {
		return voidFuture();
	}

	default CompletionStage<Void> performReactiveBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker, OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return voidFuture();
	}

	/**
	 * Perform a non-batched mutation
	 */
	default CompletionStage<Void> performReactiveNonBatchedMutation(
			PreparedStatementDetails statementDetails,
			Object id,
			JdbcValueBindings valueBindings,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		if ( statementDetails == null ) {
			return nullFuture();
		}

		final TableMapping tableDetails = statementDetails.getMutatingTableDetails();
		if ( inclusionChecker != null && !inclusionChecker.include( tableDetails ) ) {
			if ( MODEL_MUTATION_LOGGER.isTraceEnabled() ) {
				MODEL_MUTATION_LOGGER.tracef( "Skipping execution of secondary insert : %s", tableDetails.getTableName() );
			}
			return voidFuture();
		}

		if ( id != null ) {
			assert !tableDetails.isIdentifierTable() : "Unsupported identifier table with generated id";
			( (EntityTableMapping) tableDetails ).getKeyMapping().breakDownKeyJdbcValues(
					id,
					(jdbcValue, columnMapping) -> valueBindings.bindValue(
							jdbcValue,
							tableDetails.getTableName(),
							columnMapping.getColumnName(),
							ParameterUsage.SET
					),
					session
			);
		}

		// If we get here the statement is needed - make sure it is resolved
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			valueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sqlString = statementDetails.getSqlString();
		return reactiveConnection
				.update( sqlString, params )
				.thenCompose( affectedRowCount -> {
					if ( affectedRowCount == 0 && tableDetails.isOptional() ) {
						// the optional table did not have a row
						return voidFuture();
					}
					checkResults( session, statementDetails, resultChecker, affectedRowCount, -1 );
					return voidFuture();
				} )
				.whenComplete( (o, throwable) -> {
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( tableDetails );
				} );
	}
}
