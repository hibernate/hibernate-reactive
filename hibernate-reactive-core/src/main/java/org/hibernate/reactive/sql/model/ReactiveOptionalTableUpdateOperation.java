/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.model;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.MutationQueryOptions;
import org.hibernate.engine.jdbc.mutation.internal.PreparedStatementGroupSingleTable;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.CollectionHelper;
import org.hibernate.persister.entity.mutation.UpdateValuesAnalysis;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.model.MutationTarget;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;
import org.hibernate.sql.model.ast.MutatingTableReference;
import org.hibernate.sql.model.ast.TableDelete;
import org.hibernate.sql.model.ast.TableInsert;
import org.hibernate.sql.model.ast.TableUpdate;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
import org.hibernate.sql.model.internal.TableDeleteCustomSql;
import org.hibernate.sql.model.internal.TableDeleteStandard;
import org.hibernate.sql.model.internal.TableInsertCustomSql;
import org.hibernate.sql.model.internal.TableInsertStandard;
import org.hibernate.sql.model.internal.TableUpdateCustomSql;
import org.hibernate.sql.model.internal.TableUpdateStandard;
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

	// FIXME: Adding this to ORM will save us some duplicated code (similar to createJdbcInsert and createJdbcDelete)
	private JdbcMutationOperation createJdbcUpdate(SharedSessionContractImplementor session) {
		MutationTarget<?> mutationTarget = super.getMutationTarget();
		TableUpdate<JdbcMutationOperation> tableUpdate;
		if ( getTableDetails().getUpdateDetails() != null
				&& getTableDetails().getUpdateDetails().getCustomSql() != null ) {
			tableUpdate = new TableUpdateCustomSql(
					new MutatingTableReference( getTableDetails() ),
					mutationTarget,
					"upsert update for " + mutationTarget.getRolePath(),
					upsert.getValueBindings(),
					upsert.getKeyBindings(),
					upsert.getOptimisticLockBindings(),
					upsert.getParameters()
			);
		}
		else {
			tableUpdate = new TableUpdateStandard(
					new MutatingTableReference( getTableDetails() ),
					mutationTarget,
					"upsert update for " + mutationTarget.getRolePath(),
					upsert.getValueBindings(),
					upsert.getKeyBindings(),
					upsert.getOptimisticLockBindings(),
					upsert.getParameters()
			);
		}

		final SqlAstTranslator<JdbcMutationOperation> translator = session
				.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildModelMutationTranslator( tableUpdate, session.getFactory() );

		return translator.translate( null, MutationQueryOptions.INSTANCE );
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

	/**
	 * @see org.hibernate.sql.model.jdbc.OptionalTableUpdateOperation#createJdbcInsert(SharedSessionContractImplementor)
	 */
	// FIXME: change visibility to protected in ORM and remove this method
	private JdbcInsertMutation createJdbcInsert(SharedSessionContractImplementor session) {
		final TableInsert tableInsert;
		if ( getTableDetails().getInsertDetails() != null
				&& getTableDetails().getInsertDetails().getCustomSql() != null ) {
			tableInsert = new TableInsertCustomSql(
					new MutatingTableReference( getTableDetails() ),
					getMutationTarget(),
					CollectionHelper.combine( upsert.getValueBindings(), upsert.getKeyBindings() ),
					upsert.getParameters()
			);
		}
		else {
			tableInsert = new TableInsertStandard(
					new MutatingTableReference( getTableDetails() ),
					getMutationTarget(),
					CollectionHelper.combine( upsert.getValueBindings(), upsert.getKeyBindings() ),
					Collections.emptyList(),
					upsert.getParameters()
			);
		}

		final SessionFactoryImplementor factory = session.getSessionFactory();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = factory
				.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory();

		final SqlAstTranslator<JdbcInsertMutation> translator = sqlAstTranslatorFactory.buildModelMutationTranslator(
				tableInsert,
				factory
		);

		return translator.translate( null, MutationQueryOptions.INSTANCE );
	}

	/**
	 * @see org.hibernate.sql.model.jdbc.OptionalTableUpdateOperation#createJdbcDelete(SharedSessionContractImplementor)
	 */
	// FIXME: change visibility to protected in ORM and remove this method
	private JdbcDeleteMutation createJdbcDelete(SharedSessionContractImplementor session) {
		final TableDelete tableDelete;
		if ( getTableDetails().getDeleteDetails() != null
				&& getTableDetails().getDeleteDetails().getCustomSql() != null ) {
			tableDelete = new TableDeleteCustomSql(
					new MutatingTableReference( getTableDetails() ),
					getMutationTarget(),
					"upsert delete for " + upsert.getMutationTarget().getRolePath(),
					upsert.getKeyBindings(),
					upsert.getOptimisticLockBindings(),
					upsert.getParameters()
			);
		}
		else {
			tableDelete = new TableDeleteStandard(
					new MutatingTableReference( getTableDetails() ),
					getMutationTarget(),
					"upsert delete for " + getMutationTarget().getRolePath(),
					upsert.getKeyBindings(),
					upsert.getOptimisticLockBindings(),
					upsert.getParameters()
			);
		}

		final SessionFactoryImplementor factory = session.getSessionFactory();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = factory
				.getJdbcServices()
				.getJdbcEnvironment()
				.getSqlAstTranslatorFactory();

		final SqlAstTranslator<JdbcDeleteMutation> translator = sqlAstTranslatorFactory.buildModelMutationTranslator(
				tableDelete,
				factory
		);

		return translator.translate( null, MutationQueryOptions.INSTANCE );
	}
}
