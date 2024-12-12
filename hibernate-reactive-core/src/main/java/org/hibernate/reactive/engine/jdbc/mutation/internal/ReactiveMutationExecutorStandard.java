/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.batch.spi.Batch;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementGroup;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorStandard;
import org.hibernate.engine.jdbc.mutation.spi.BatchKeyAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.persister.entity.mutation.EntityMutationTarget;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.generator.values.ReactiveGeneratedValuesMutationDelegate;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.model.EntityMutationOperationGroup;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;

import static org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper.checkResults;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.MutationExecutorStandard
 */
public class ReactiveMutationExecutorStandard extends MutationExecutorStandard implements ReactiveMutationExecutor {

	private static final Log LOG = make( Log.class, MethodHandles.lookup() );

	private final GeneratedValuesMutationDelegate generatedValuesDelegate;
	private final MutationOperationGroup mutationOperationGroup;

	public ReactiveMutationExecutorStandard(
			MutationOperationGroup mutationOperationGroup,
			BatchKeyAccess batchKeySupplier,
			int batchSize,
			SharedSessionContractImplementor session) {
		super( mutationOperationGroup, batchKeySupplier, batchSize, session );
		this.generatedValuesDelegate = mutationOperationGroup.asEntityMutationOperationGroup() != null
				? mutationOperationGroup.asEntityMutationOperationGroup().getMutationDelegate()
				: null;
		this.mutationOperationGroup = mutationOperationGroup;
	}

	private ReactiveConnection connection(SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
	}

	@Override
	public CompletionStage<Void> performReactiveBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker, OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return ReactiveMutationExecutor.super
				.performReactiveBatchedOperations( valuesAnalysis, inclusionChecker, resultChecker, session);
	}

	@Override
	protected GeneratedValues performNonBatchedOperations(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "performReactiveNonBatchedOperations" );
	}

	@Override
	protected void performSelfExecutingOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "performReactiveSelfExecutingOperations" );
	}

	@Override
	protected void performBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			Batch.StaleStateMapper staleStateMapper) {
		throw LOG.nonReactiveMethodCall( "performReactiveBatchedOperations" );
	}

	@Override
	public CompletionStage<GeneratedValues> performReactiveNonBatchedOperations(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session,
			boolean isIndentityInsert,
			String[] identifiersColumnsNames) {

		if ( getNonBatchedStatementGroup() == null || getNonBatchedStatementGroup().getNumberOfStatements() <= 0 ) {
			return nullFuture();
		}

		PreparedStatementGroup nonBatchedStatementGroup = getNonBatchedStatementGroup();
		if ( generatedValuesDelegate != null ) {
			final EntityMutationOperationGroup entityGroup = mutationOperationGroup.asEntityMutationOperationGroup();
			final EntityMutationTarget entityTarget = entityGroup.getMutationTarget();
			final PreparedStatementDetails details = nonBatchedStatementGroup.getPreparedStatementDetails(
					entityTarget.getIdentifierTableName()
			);
			return ( (ReactiveGeneratedValuesMutationDelegate) generatedValuesDelegate )
					.reactivePerformMutation( details, getJdbcValueBindings(), modelReference, session )
					.thenCompose( generatedValues -> {
						Object id = entityGroup.getMutationType() == MutationType.INSERT && details.getMutatingTableDetails().isIdentifierTable()
								? generatedValues.getGeneratedValue( entityTarget.getTargetPart().getIdentifierMapping() )
								: null;
						OperationsForEach forEach = new OperationsForEach(
								id,
								inclusionChecker,
								resultChecker,
								session,
								getJdbcValueBindings(),
								true
						);
						nonBatchedStatementGroup.forEachStatement( forEach::add );
						return forEach.buildLoop().thenApply( v -> generatedValues );
					} );

		}
		else {
			OperationsForEach forEach = new OperationsForEach(
					null,
					inclusionChecker,
					resultChecker,
					session,
					getJdbcValueBindings(),
					false
			);
			nonBatchedStatementGroup.forEachStatement( forEach::add );
			return forEach.buildLoop().thenCompose( CompletionStages::nullFuture );
		}
	}

	private class OperationsForEach {

		private final Object id;
		private final TableInclusionChecker inclusionChecker;
		private final OperationResultChecker resultChecker;
		private final SharedSessionContractImplementor session;
		private final boolean requiresCheck;
		private final JdbcValueBindings jdbcValueBindings;

		private CompletionStage<Void> loop = voidFuture();

		public OperationsForEach(
				Object id,
				TableInclusionChecker inclusionChecker,
				OperationResultChecker resultChecker,
				SharedSessionContractImplementor session,
				JdbcValueBindings jdbcValueBindings,
				boolean requiresCheck) {
			this.id = id;
			this.inclusionChecker = inclusionChecker;
			this.resultChecker = resultChecker;
			this.session = session;
			this.jdbcValueBindings = jdbcValueBindings;
			this.requiresCheck = requiresCheck;
		}

		public void add(String tableName, PreparedStatementDetails statementDetails) {
			if ( requiresCheck ) {
				loop = loop.thenCompose( v -> !statementDetails
						.getMutatingTableDetails().isIdentifierTable()
						? performReactiveNonBatchedMutation( statementDetails, id, jdbcValueBindings, inclusionChecker, resultChecker, session, null )
						: voidFuture()
				);
			}
			else {
				loop = loop.thenCompose( v -> performReactiveNonBatchedMutation(
						statementDetails,
						null,
						jdbcValueBindings,
						inclusionChecker,
						resultChecker,
						session,
						null
				) );
			}
		}

		public CompletionStage<Void> buildLoop() {
			return loop;
		}
	}
	@Override
	public CompletionStage<Void> performReactiveNonBatchedMutation(
			PreparedStatementDetails statementDetails,
			Object id,
			JdbcValueBindings valueBindings,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session,
			String[] identifierColumnsNames) {
		if ( statementDetails == null ) {
			return voidFuture();
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

		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			valueBindings.beforeStatement( details );
		} );

		return connection( session )
				.update( statementDetails.getSqlString(), params )
				.thenCompose( affectedRowCount -> checkResult( session, statementDetails, resultChecker, tableDetails, affectedRowCount ) )
				.whenComplete( (unused, throwable) -> {
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( tableDetails );
				} );
	}

	private static CompletionStage<Void> checkResult(
			SharedSessionContractImplementor session,
			PreparedStatementDetails statementDetails,
			OperationResultChecker resultChecker,
			TableMapping tableDetails,
			Integer affectedRowCount) {
		if ( affectedRowCount == 0 && tableDetails.isOptional() ) {
			// the optional table did not have a row
			return voidFuture();
		}
		try {
			checkResults( resultChecker, statementDetails, affectedRowCount, -1 );
			return voidFuture();
		}
		catch (SQLException e) {
			Throwable exception = session.getJdbcServices().getSqlExceptionHelper().convert(
					e,
					String.format(
							"Unable to execute mutation PreparedStatement against table `%s`",
							tableDetails.getTableName()
					),
					statementDetails.getSqlString()
			);
			return failedFuture( exception );
		}
	}

	@Override
	public void release() {
	}
}
