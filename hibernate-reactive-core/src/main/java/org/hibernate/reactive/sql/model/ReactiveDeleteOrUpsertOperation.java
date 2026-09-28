/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.model;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.PreparedStatementGroupSingleTable;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.mutation.UpdateValuesAnalysis;
import org.hibernate.reactive.adaptor.internal.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.internal.PreparedStatementAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.spi.mutation.ValuesAnalysis;
import org.hibernate.sql.spi.mutation.jdbc.DeleteOrUpsertOperation;

import static org.hibernate.reactive.util.internal.CompletionStages.supplyStage;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

public class ReactiveDeleteOrUpsertOperation extends DeleteOrUpsertOperation
		implements ReactiveSelfExecutingUpdateOperation {

	public ReactiveDeleteOrUpsertOperation(DeleteOrUpsertOperation operation) {
		super( operation.getUpsertOperation(), operation.getOptionalTableUpdate() );
	}

	@Override
	public CompletionStage<Void> performReactiveMutation(
			JdbcValueBindings jdbcValueBindings,
			ValuesAnalysis incomingValuesAnalysis,
			SharedSessionContractImplementor session) {
		final var tableMapping = getTableDetails();
		final UpdateValuesAnalysis valuesAnalysis = (UpdateValuesAnalysis) incomingValuesAnalysis;
		return supplyStage( () -> valuesAnalysis.getTablesWithNonNullValues().contains( tableMapping )
				? performReactiveUpsert( jdbcValueBindings, session )
				: performReactiveDelete( jdbcValueBindings, session )
		).whenComplete( (o, throwable) -> jdbcValueBindings.afterStatement( tableMapping ) );
	}

	private CompletionStage<Void> performReactiveUpsert(
			JdbcValueBindings jdbcValueBindings,
			SharedSessionContractImplementor session) {
		final String tableName = getTableDetails().getTableName();
		MODEL_MUTATION_LOGGER.performingUpsert( tableName );

		final PreparedStatementGroupSingleTable statementGroup = new PreparedStatementGroupSingleTable( getUpsertOperation(), session );
		final PreparedStatementDetails statementDetails = statementGroup.resolvePreparedStatementDetails( tableName );

		session.getJdbcServices().getSqlStatementLogger().logStatement( statementDetails.getSqlString() );
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			jdbcValueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		return reactiveConnection
				.update( statementDetails.getSqlString(), params )
				.thenAccept( rowCount -> MODEL_MUTATION_LOGGER
						.tracef( "`%s` rows upserted into `%s`", rowCount, getTableDetails().getTableName() )
				);
	}

	private CompletionStage<Void> performReactiveDelete(
			JdbcValueBindings jdbcValueBindings,
			SharedSessionContractImplementor session) {
		final var tableMapping = getTableDetails();
		final var statementDetails = createDeleteStatementDetails( session, tableMapping );
		session.getJdbcServices().getSqlStatementLogger().logStatement( statementDetails.getSqlString() );
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			jdbcValueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sqlString = statementDetails.getSqlString();
		return reactiveConnection
				.update( sqlString, params )
				.thenAccept( rowCount -> MODEL_MUTATION_LOGGER.tracef( "`%s` rows upsert-deleted from `%s`", rowCount, tableMapping.getTableName() ) );
	}
}
