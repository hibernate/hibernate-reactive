/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityRowIdMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationOperationGroup;

import static org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper.identifiedResultsCheck;

public class ReactiveDeleteCoordinator extends DeleteCoordinator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	// We expect this to only be assigned in coordinateReactiveDelete
	private CompletionStage<Void> stage;

	public ReactiveDeleteCoordinator(AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public void coordinateDelete(Object entity, Object id, Object version, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "coordinateReactiveDelete" );
	}

	public CompletionStage<Void> coordinateReactiveDelete(Object entity, Object id, Object version, SharedSessionContractImplementor session) {
		stage = new CompletableFuture<>();
		try {
			super.coordinateDelete( entity, id, version, session );
		}
		catch (Throwable t) {
			stage.toCompletableFuture().completeExceptionally( t );
		}
		return stage;
	}

	@Override
	protected void doDynamicDelete(Object entity, Object id, Object rowId, Object[] loadedState, SharedSessionContractImplementor session) {
		final MutationOperationGroup operationGroup = generateOperationGroup( loadedState, true, session );
		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, operationGroup );

		operationGroup.forEachOperation( (position, mutation) -> {
			if ( mutation != null ) {
				final String tableName = mutation.getTableDetails().getTableName();
				mutationExecutor.getPreparedStatementDetails( tableName );
			}
		} );

		applyLocking( null, loadedState, mutationExecutor, session );
		applyId( id, null, mutationExecutor, getStaticDeleteGroup(), session );

		mutationExecutor.executeReactive(
						entity,
						null,
						null,
						(statementDetails, affectedRowCount, batchPosition) -> identifiedResultsCheck(
								statementDetails,
								affectedRowCount,
								batchPosition,
								entityPersister(),
								id,
								factory()
						),
						session
				)
				.whenComplete( (o, t) -> mutationExecutor.release() )
				.whenComplete( this::complete );
	}

	@Override
	protected void applyId(
			Object id,
			Object rowId,
			MutationExecutor mutationExecutor,
			MutationOperationGroup operationGroup,
			SharedSessionContractImplementor session) {
		final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();
		final EntityRowIdMapping rowIdMapping = entityPersister().getRowIdMapping();

		operationGroup.forEachOperation( (position, jdbcMutation) -> {
			final EntityTableMapping tableDetails = (EntityTableMapping) jdbcMutation.getTableDetails();
			breakDownIdJdbcValues( id, rowId, session, jdbcValueBindings, rowIdMapping, tableDetails );
			final PreparedStatementDetails statementDetails = mutationExecutor.getPreparedStatementDetails( tableDetails.getTableName() );
			if ( statementDetails != null ) {
				PreparedStatementAdaptor.bind( statement -> {
					PrepareStatementDetailsAdaptor detailsAdaptor = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
					// force creation of the PreparedStatement
					//noinspection resource
					detailsAdaptor.resolveStatement();
				} );
			}
		} );
	}

	@Override
	protected void doStaticDelete(Object entity, Object id, Object[] loadedState, Object version, SharedSessionContractImplementor session) {
		final boolean applyVersion = entity != null;
		final MutationOperationGroup operationGroupToUse = entity == null
				? resolveNoVersionDeleteGroup( session )
				: getStaticDeleteGroup();

		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, operationGroupToUse );
		getStaticDeleteGroup().forEachOperation( (position, mutation) -> {
			if ( mutation != null ) {
				mutationExecutor.getPreparedStatementDetails( mutation.getTableDetails().getTableName() );
			}
		} );

		if ( applyVersion ) {
			applyLocking( version, null, mutationExecutor, session );
		}
		final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();
		bindPartitionColumnValueBindings( loadedState, session, jdbcValueBindings );
		applyId( id, null, mutationExecutor, getStaticDeleteGroup(), session );
		mutationExecutor.executeReactive(
						entity,
						null,
						null,
						(statementDetails, affectedRowCount, batchPosition) -> identifiedResultsCheck(
								statementDetails,
								affectedRowCount,
								batchPosition,
								entityPersister(),
								id,
								factory()
						),
						session
				)
				.thenAccept( v -> mutationExecutor.release() )
				.whenComplete( this::complete );
	}

	/**
	 * Copy and paste of the on in ORM
	 */
	private static void breakDownIdJdbcValues(
			Object id,
			Object rowId,
			SharedSessionContractImplementor session,
			JdbcValueBindings jdbcValueBindings,
			EntityRowIdMapping rowIdMapping,
			EntityTableMapping tableDetails) {
		if ( rowId != null && rowIdMapping != null && tableDetails.isIdentifierTable() ) {
			jdbcValueBindings.bindValue(
					rowId,
					tableDetails.getTableName(),
					rowIdMapping.getRowIdName(),
					ParameterUsage.RESTRICT,
					session
			);
		}
		else {
			tableDetails.getKeyMapping().breakDownKeyJdbcValues(
					id,
					(jdbcValue, columnMapping) -> jdbcValueBindings.bindValue(
							jdbcValue,
							tableDetails.getTableName(),
							columnMapping.getColumnName(),
							ParameterUsage.RESTRICT,
							session
					),
					session
			);
		}
	}

	private void complete(Object o, Throwable throwable) {
		if ( throwable != null ) {
			stage.toCompletableFuture().completeExceptionally( throwable );
		}
		else {
			stage.toCompletableFuture().complete( null );
		}
	}

	private ReactiveMutationExecutor mutationExecutor(
			SharedSessionContractImplementor session,
			MutationOperationGroup operationGroup) {
		final MutationExecutorService mutationExecutorService = session
				.getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		return (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( this::getBatchKey, operationGroup, session );
	}
}
