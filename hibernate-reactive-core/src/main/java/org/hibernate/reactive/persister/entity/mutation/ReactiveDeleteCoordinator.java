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
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinatorStandard;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.MutationOperationGroup;

import static org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper.identifiedResultsCheck;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveDeleteCoordinator extends DeleteCoordinatorStandard {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private CompletionStage<Void> stage;

	public ReactiveDeleteCoordinator(AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public void delete(Object entity, Object id, Object version, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "coordinateReactiveDelete" );
	}

	public CompletionStage<Void> reactiveDelete(Object entity, Object id, Object version, SharedSessionContractImplementor session) {
		try {
			super.delete( entity, id, version, session );
			return stage != null ? stage : voidFuture();
		}
		catch (Throwable t) {
			if ( stage == null ) {
				return failedFuture( t );
			}
			stage.toCompletableFuture().completeExceptionally( t );
			return stage;
		}
	}

	@Override
	protected void doDynamicDelete(Object entity, Object id, Object rowId, Object[] loadedState, SharedSessionContractImplementor session) {
		stage = new CompletableFuture<>();
		final MutationOperationGroup operationGroup = generateOperationGroup( null, loadedState, true, session );
		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, operationGroup );

		for ( int i = 0; i < operationGroup.getNumberOfOperations(); i++ ) {
			final MutationOperation mutation = operationGroup.getOperation( i );
			if ( mutation != null ) {
				final String tableName = mutation.getTableDetails().getTableName();
				mutationExecutor.getPreparedStatementDetails( tableName );
			}
		}
		applyDynamicDeleteTableDetails( id, rowId, loadedState, mutationExecutor, operationGroup, session );
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

		for ( int position = 0; position < operationGroup.getNumberOfOperations(); position++ ) {
			final MutationOperation jdbcMutation = operationGroup.getOperation( position );
			final EntityTableMapping tableDetails = (EntityTableMapping) jdbcMutation.getTableDetails();
			breakDownKeyJdbcValues( id, rowId, session, jdbcValueBindings, tableDetails );
			final PreparedStatementDetails statementDetails = mutationExecutor.getPreparedStatementDetails( tableDetails.getTableName() );
			if ( statementDetails != null ) {
				PreparedStatementAdaptor.bind( statement -> {
					PrepareStatementDetailsAdaptor detailsAdaptor = new PrepareStatementDetailsAdaptor(
							statementDetails,
							statement,
							session.getJdbcServices()
					);
					// force creation of the PreparedStatement
					//noinspection resource
					detailsAdaptor.resolveStatement();
				} );
			}
		}
	}

	@Override
	protected void doStaticDelete(Object entity, Object id, Object rowId, Object[] loadedState, Object version, SharedSessionContractImplementor session) {
		stage = new CompletableFuture<>();
		final boolean applyVersion = entity != null;
		final MutationOperationGroup operationGroupToUse = entity == null
				? resolveNoVersionDeleteGroup( session )
				: getStaticMutationOperationGroup();

		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, operationGroupToUse );
		for ( int position = 0; position < getStaticMutationOperationGroup().getNumberOfOperations(); position++ ) {
			final MutationOperation mutation = getStaticMutationOperationGroup().getOperation( position );
			if ( mutation != null ) {
				mutationExecutor.getPreparedStatementDetails( mutation.getTableDetails().getTableName() );
			}
		}

		if ( applyVersion ) {
			applyLocking( version, null, mutationExecutor, session );
		}
		final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();
		bindPartitionColumnValueBindings( loadedState, session, jdbcValueBindings );
		applyId( id, rowId, mutationExecutor, getStaticMutationOperationGroup(), session );
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
