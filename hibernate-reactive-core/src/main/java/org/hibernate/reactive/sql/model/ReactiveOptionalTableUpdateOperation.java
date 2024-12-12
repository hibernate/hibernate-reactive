/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.model;

import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.PreparedStatementGroupSingleTable;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.mutation.UpdateValuesAnalysis;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.model.MutationTarget;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
import org.hibernate.sql.model.jdbc.JdbcDeleteMutation;
import org.hibernate.sql.model.jdbc.JdbcInsertMutation;
import org.hibernate.sql.model.jdbc.JdbcMutationOperation;
import org.hibernate.sql.model.jdbc.OptionalTableUpdateOperation;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

public class ReactiveOptionalTableUpdateOperation extends OptionalTableUpdateOperation
		implements ReactiveSelfExecutingUpdateOperation {
	private static final Log LOG = make( Log.class, lookup() );
	private final OptionalTableUpdate upsert;

	public ReactiveOptionalTableUpdateOperation(
			MutationTarget<?> mutationTarget,
			OptionalTableUpdate upsert,
			SessionFactoryImplementor factory) {
		super( mutationTarget, upsert, factory );
		this.upsert = upsert;
	}

	@Override
	public void performMutation(
			JdbcValueBindings jdbcValueBindings,
			ValuesAnalysis valuesAnalysis,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "performReactiveMutation" );
	}

	@Override
	public CompletionStage<Void> performReactiveMutation(
			JdbcValueBindings jdbcValueBindings,
			ValuesAnalysis incomingValuesAnalysis,
			SharedSessionContractImplementor session) {
		final UpdateValuesAnalysis valuesAnalysis = (UpdateValuesAnalysis) incomingValuesAnalysis;
		if ( !valuesAnalysis.getTablesNeedingUpdate().contains( getTableDetails() ) ) {
			return voidFuture();
		}

		return doReactiveMutation( getTableDetails(), jdbcValueBindings, valuesAnalysis, session );
	}

	private CompletionStage<Void> doReactiveMutation(
			TableMapping tableMapping,
			JdbcValueBindings jdbcValueBindings,
			UpdateValuesAnalysis valuesAnalysis,
			SharedSessionContractImplementor session) {

		return voidFuture()
				.thenCompose( v -> {
					if ( shouldDelete( valuesAnalysis, tableMapping ) ) {
						return performReactiveDelete( jdbcValueBindings, session );
					}
					else {
						return performReactiveUpdate( jdbcValueBindings, session )
								.thenCompose( wasUpdated -> {
									if ( !wasUpdated ) {
										MODEL_MUTATION_LOGGER.debugf( "Upsert update altered no rows - inserting : %s", tableMapping.getTableName() );
										return performReactiveInsert( jdbcValueBindings, session );
									}
									return voidFuture();
								} );
					}
				} )
				.whenComplete( (o, throwable) -> jdbcValueBindings.afterStatement( tableMapping ) );
	}

	private boolean shouldDelete(UpdateValuesAnalysis valuesAnalysis, TableMapping tableMapping) {
		return !valuesAnalysis.getTablesWithNonNullValues().contains( tableMapping )
				// all the new values for this table were null - possibly delete the row
				&& valuesAnalysis.getTablesWithPreviousNonNullValues().contains( tableMapping );
	}

	/**
	 * @see org.hibernate.sql.model.jdbc.OptionalTableUpdateOperation#performDelete(JdbcValueBindings, SharedSessionContractImplementor)
	 */
	private CompletionStage<Void> performReactiveDelete(
			JdbcValueBindings jdbcValueBindings,
			SharedSessionContractImplementor session) {
		final JdbcDeleteMutation jdbcDelete = createJdbcDelete( session );

		final PreparedStatementGroupSingleTable statementGroup = new PreparedStatementGroupSingleTable(
				jdbcDelete,
				session
		);
		final PreparedStatementDetails statementDetails = statementGroup.resolvePreparedStatementDetails(
				getTableDetails().getTableName() );

		session.getJdbcServices().getSqlStatementLogger().logStatement( jdbcDelete.getSqlString() );

		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor(
					statementDetails,
					statement,
					session.getJdbcServices()
			);
			jdbcValueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		return reactiveConnection.update( statementDetails.getSqlString(), params ).thenCompose( CompletionStages::voidFuture);
	}

	/**
	 * @see org.hibernate.sql.model.jdbc.OptionalTableUpdateOperation#performUpdate(JdbcValueBindings, SharedSessionContractImplementor)
	 */
	private CompletionStage<Boolean> performReactiveUpdate(
			JdbcValueBindings jdbcValueBindings,
			SharedSessionContractImplementor session) {
		MODEL_MUTATION_LOGGER.tracef( "#performReactiveUpdate(%s)", getTableDetails().getTableName() );

		final JdbcMutationOperation jdbcUpdate = createJdbcUpdate( session );
		final PreparedStatementGroupSingleTable statementGroup = new PreparedStatementGroupSingleTable( jdbcUpdate, session );
		final PreparedStatementDetails statementDetails = statementGroup
				.resolvePreparedStatementDetails( getTableDetails().getTableName() );

		// If we get here the statement is needed - make sure it is resolved
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor(
					statementDetails,
					statement,
					session.getJdbcServices()
			);
			jdbcValueBindings.beforeStatement( details );
		} );

		session.getJdbcServices().getSqlStatementLogger().logStatement( statementDetails.getSqlString() );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		return reactiveConnection
				.update( statementDetails.getSqlString(), params )
				.thenApply( rowCount -> {
					if ( rowCount == 0 ) {
						return false;
					}
					try {
						upsert.getExpectation()
								.verifyOutcome(
										rowCount,
										statementDetails.getStatement(),
										-1,
										statementDetails.getSqlString()
								);
						return true;
					}
					catch (SQLException e) {
						throw session.getJdbcServices().getSqlExceptionHelper().convert(
								e,
								"Unable to execute mutation PreparedStatement against table `" + getTableDetails().getTableName() + "`",
								statementDetails.getSqlString()
						);
					}
				} );
	}

	private CompletionStage<Void> performReactiveInsert(
			JdbcValueBindings jdbcValueBindings,
			SharedSessionContractImplementor session) {
		final JdbcInsertMutation jdbcInsert = createJdbcInsert( session );

		final PreparedStatementGroupSingleTable statementGroup = new PreparedStatementGroupSingleTable( jdbcInsert, session );
		final PreparedStatementDetails statementDetails = statementGroup.resolvePreparedStatementDetails( getTableDetails().getTableName() );
		// If we get here the statement is needed - make sure it is resolved
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			jdbcValueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		return reactiveConnection.update( statementDetails.getSqlString(), params ).thenCompose(CompletionStages::voidFuture);
	}
}
