/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import static org.hibernate.reactive.engine.jdbc.ResultsCheckerUtil.checkResults;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_TRACE_ENABLED;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorPostInsert;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.id.insert.ReactiveInsertGeneratedIdentifierDelegate;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.ValuesAnalysis;

public class ReactiveMutationExecutorPostInsert extends MutationExecutorPostInsert implements ReactiveMutationExecutor {

	public ReactiveMutationExecutorPostInsert(
			MutationOperationGroup mutationOperationGroup,
			SharedSessionContractImplementor session) {
		super( mutationOperationGroup, session );
	}

	@Override
	public CompletionStage<Object> executeReactive(Object modelReference, ValuesAnalysis valuesAnalysis,
												   TableInclusionChecker inclusionChecker,
												   OperationResultChecker resultChecker,
												   SharedSessionContractImplementor session) {
		return ( (ReactiveInsertGeneratedIdentifierDelegate) mutationTarget.getIdentityInsertDelegate() )
				.reactivePerformInsert(
						identityInsertStatementDetails,
						getJdbcValueBindings(),
						modelReference,
						session
				)
				.thenApply( this::logId )
				.thenCompose(id -> {
					if (secondaryTablesStatementGroup == null) {
						return completedFuture(id);
					}
					AtomicReference<CompletionStage<Object>>  res = new AtomicReference<>(completedFuture(id));
					secondaryTablesStatementGroup.forEachStatement((tableName, statementDetails) -> {
						res.set(res.get().thenCompose(i -> reactiveExecuteWithId(i, tableName, statementDetails, inclusionChecker, resultChecker, session)));
					});
					return res.get();
				});
	}

	private Object logId(Object identifier) {
		if ( MODEL_MUTATION_LOGGER_TRACE_ENABLED ) {
			MODEL_MUTATION_LOGGER
					.tracef( "Post-insert generated value : `%s` (%s)", identifier, mutationTarget.getNavigableRole().getFullPath() );
		}
		return identifier;
	}

	private CompletionStage<Object> reactiveExecuteWithId(
			Object id,
			String tableName,
			PreparedStatementDetails statementDetails,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {

		if ( statementDetails == null ) {
			return completedFuture(id);
		}

		final EntityTableMapping tableDetails = (EntityTableMapping) statementDetails.getMutatingTableDetails();
		assert !tableDetails.isIdentifierTable();

		if ( inclusionChecker != null && !inclusionChecker.include( tableDetails ) ) {
			if ( MODEL_MUTATION_LOGGER_TRACE_ENABLED ) {
				MODEL_MUTATION_LOGGER.tracef(
						"Skipping execution of secondary insert : %s",
						tableDetails.getTableName()
				);
			}
			return completedFuture(id);
		}


		tableDetails.getKeyMapping().breakDownKeyJdbcValues(
				id,
				(jdbcValue, columnMapping) -> {
					valueBindings.bindValue(
							jdbcValue,
							tableName,
							columnMapping.getColumnName(),
							ParameterUsage.SET
					);
				},
				session
		);

		session.getJdbcServices().getSqlStatementLogger().logStatement( statementDetails.getSqlString() );

		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			//noinspection resource
			details.resolveStatement();
			valueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sqlString = statementDetails.getSqlString();
		return reactiveConnection
				.update( sqlString, params )
				.thenApply( affectedRowCount -> {
					if ( affectedRowCount == 0 && tableDetails.isOptional() ) {
						// the optional table did not have a row
						return completedFuture(id);
					}
					checkResults( session, statementDetails, resultChecker, affectedRowCount, -1);
					return id;
				} ).whenComplete( (unused, throwable) -> {
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( tableDetails );
				} );
	}


}
