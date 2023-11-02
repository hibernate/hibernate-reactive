/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
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

import org.jboss.logging.Logger;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

public class ReactiveDeleteOrUpsertOperation extends DeleteOrUpsertOperation implements ReactiveSelfExecutingUpdateOperation {
	private static final Log LOG = make( Log.class, lookup() );
	private final OptionalTableUpdate upsert;
	private final UpsertOperation upsertOperation;
	private final UpsertStatementInfo upsertStatementInfo;
	public ReactiveDeleteOrUpsertOperation(
			EntityMutationTarget mutationTarget,
			EntityTableMapping tableMapping,
			UpsertOperation upsertOperation,
			OptionalTableUpdate optionalTableUpdate) {
		super( mutationTarget, tableMapping, upsertOperation, optionalTableUpdate );
		this.upsert = optionalTableUpdate;
		this.upsertOperation = upsertOperation;
		this.upsertStatementInfo = new UpsertStatementInfo( );
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

	/**
	 *
	 * @see DeleteOrUpsertOperation#performMutation(JdbcValueBindings, ValuesAnalysis, SharedSessionContractImplementor)
	 * @param tableMapping
	 * @param jdbcValueBindings
	 * @param valuesAnalysis
	 * @param session
	 * @return
	 */
	private CompletionStage<Void> doReactiveMutation(
			TableMapping tableMapping,
			JdbcValueBindings jdbcValueBindings,
			UpdateValuesAnalysis valuesAnalysis,
			SharedSessionContractImplementor session) {

		return voidFuture()
				.thenCompose( v -> {
					if ( !valuesAnalysis.getTablesWithNonNullValues().contains( tableMapping ) ) {
						return performReactiveDelete( jdbcValueBindings, session );
					}
					else {
						return performReactiveUpsert( jdbcValueBindings,session );
					}
				} )
				.whenComplete( (o, throwable) -> jdbcValueBindings.afterStatement( tableMapping ) );
	}

	private CompletionStage<Void> performReactiveUpsert(JdbcValueBindings jdbcValueBindings, SharedSessionContractImplementor session) {
		final PreparedStatementGroupSingleTable statementGroup = new PreparedStatementGroupSingleTable( upsertOperation, session );
		final PreparedStatementDetails statementDetails = statementGroup.resolvePreparedStatementDetails( getTableDetails().getTableName() );
		upsertStatementInfo.setStatementDetails( statementDetails );

		// If we get here the statement is needed - make sure it is resolved
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor(
					statementDetails,
					statement,
					session.getJdbcServices()
			);
			jdbcValueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sqlString = statementDetails.getSqlString();
		return reactiveConnection
				.update( sqlString, params ).thenCompose(this::checkUpsertResults);
	}

	private CompletionStage<Void> checkUpsertResults( Integer rowCount ) {
		if ( rowCount > 0 ) {
			try {
				upsert.getExpectation()
						.verifyOutcome(
								rowCount,
								upsertStatementInfo.getStatementDetails(),
								-1,
								upsertStatementInfo.getStatementSqlString()
						);
				return voidFuture();
			}
			catch (SQLException e) {
				LOG.log( Logger.Level.ERROR, e );
			}
		}
		return voidFuture();
	}

	private CompletionStage<Void> performReactiveDelete(JdbcValueBindings jdbcValueBindings, SharedSessionContractImplementor session) {
		MODEL_MUTATION_LOGGER.tracef( "#performDelete(%s)", getTableDetails().getTableName() );

		final TableDeleteStandard upsertDeleteAst = new TableDeleteStandard(
				upsert.getMutatingTable(),
				getMutationTarget(),
				"upsert delete",
				upsert.getKeyBindings(),
				Collections.emptyList(),
				Collections.emptyList()
		);

		final SqlAstTranslator<JdbcDeleteMutation> translator = session
				.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildModelMutationTranslator( upsertDeleteAst, session.getFactory() );
		final JdbcDeleteMutation upsertDelete = translator.translate( null, MutationQueryOptions.INSTANCE );

		final PreparedStatementGroupSingleTable statementGroup = new PreparedStatementGroupSingleTable( upsertDelete, session );
		final PreparedStatementDetails statementDetails = statementGroup.resolvePreparedStatementDetails( getTableDetails().getTableName() );
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			jdbcValueBindings.beforeStatement( details );
		} );
		session.getJdbcServices().getSqlStatementLogger().logStatement( statementDetails.getSqlString() );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sqlString = statementDetails.getSqlString();
		return reactiveConnection
				.update( sqlString, params ).thenCompose(this::checkUpsertResults);
	}

	static class UpsertStatementInfo {

		PreparedStatementDetails statementDetails;

		public void setStatementDetails(PreparedStatementDetails statementDetails) {
			this.statementDetails = statementDetails;
		}

		public PreparedStatement getStatementDetails() {
			return statementDetails.getStatement();
		}

		public String getStatementSqlString() {
			return statementDetails.getSqlString();
		}
	}
}
