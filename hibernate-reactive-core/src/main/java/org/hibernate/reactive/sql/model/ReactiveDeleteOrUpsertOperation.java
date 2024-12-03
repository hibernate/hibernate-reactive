/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.model;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.MutationQueryOptions;
import org.hibernate.engine.jdbc.mutation.internal.PreparedStatementGroupSingleTable;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.mutation.EntityMutationTarget;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.persister.entity.mutation.UpdateValuesAnalysis;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
import org.hibernate.sql.model.internal.TableDeleteStandard;
import org.hibernate.sql.model.jdbc.DeleteOrUpsertOperation;
import org.hibernate.sql.model.jdbc.JdbcDeleteMutation;
import org.hibernate.sql.model.jdbc.UpsertOperation;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Collections.emptyList;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

public class ReactiveDeleteOrUpsertOperation extends DeleteOrUpsertOperation
		implements ReactiveSelfExecutingUpdateOperation {
	private static final Log LOG = make( Log.class, lookup() );

	public ReactiveDeleteOrUpsertOperation(
			EntityMutationTarget mutationTarget,
			EntityTableMapping tableMapping,
			UpsertOperation upsertOperation,
			OptionalTableUpdate optionalTableUpdate) {
		super( mutationTarget, tableMapping, upsertOperation, optionalTableUpdate );
	}

	public ReactiveDeleteOrUpsertOperation(DeleteOrUpsertOperation original) {
		super( original );
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
				.thenCompose( v -> !valuesAnalysis.getTablesWithNonNullValues().contains( tableMapping )
						? performReactiveDelete( jdbcValueBindings, session )
						: performReactiveUpsert( jdbcValueBindings, session )
				)
				.whenComplete( (o, throwable) -> jdbcValueBindings.afterStatement( tableMapping ) );
	}

	private CompletionStage<Void> performReactiveUpsert(
			JdbcValueBindings jdbcValueBindings,
			SharedSessionContractImplementor session) {
		final String tableName = getTableDetails().getTableName();
		MODEL_MUTATION_LOGGER.tracef( "#performReactiveUpsert(%s)", tableName );

		final PreparedStatementGroupSingleTable statementGroup = new PreparedStatementGroupSingleTable( getUpsertOperation(), session );
		final PreparedStatementDetails statementDetails = statementGroup.resolvePreparedStatementDetails( tableName );

		session.getJdbcServices().getSqlStatementLogger().logStatement( statementDetails.getSqlString() );
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor(
					statementDetails,
					statement,
					session.getJdbcServices()
			);
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
		final String tableName = getTableDetails().getTableName();
		MODEL_MUTATION_LOGGER.tracef( "#performReactiveDelete(%s)", tableName );

		final TableDeleteStandard upsertDeleteAst = new TableDeleteStandard(
				getOptionalTableUpdate().getMutatingTable(),
				getMutationTarget(),
				"upsert delete",
				getOptionalTableUpdate().getKeyBindings(),
				emptyList(),
				emptyList()
		);

		final SqlAstTranslator<JdbcDeleteMutation> translator = session
				.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildModelMutationTranslator( upsertDeleteAst, session.getFactory() );
		final JdbcDeleteMutation upsertDelete = translator.translate( null, MutationQueryOptions.INSTANCE );

		final PreparedStatementGroupSingleTable statementGroup = new PreparedStatementGroupSingleTable( upsertDelete, session );
		final PreparedStatementDetails statementDetails = statementGroup.resolvePreparedStatementDetails( tableName );

		session.getJdbcServices().getSqlStatementLogger().logStatement( statementDetails.getSqlString() );
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			jdbcValueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sqlString = statementDetails.getSqlString();
		return reactiveConnection
				.update( sqlString, params )
				.thenAccept( rowCount -> MODEL_MUTATION_LOGGER.tracef( "`%s` rows upsert-deleted from `%s`", rowCount, tableName ) );
	}
}
