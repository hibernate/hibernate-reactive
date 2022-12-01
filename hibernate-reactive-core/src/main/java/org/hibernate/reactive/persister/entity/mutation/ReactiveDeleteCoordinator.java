/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationOperationGroup;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveDeleteCoordinator extends DeleteCoordinator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private CompletionStage<Void> stage = null;

	public ReactiveDeleteCoordinator(AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public void coordinateDelete(Object entity, Object id, Object version, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "coordinateReactiveDelete" );
	}

	public CompletionStage<Void> coordinateReactiveDelete(Object entity, Object id, Object version, SharedSessionContractImplementor session) {
		try {
			super.coordinateDelete( entity, id, version, session );
			if ( stage == null ) {
				return voidFuture();
			}
			return stage;
		}
		catch (Throwable t) {
			if ( stage == null ) {
				stage = new CompletableFuture<>();
			}
			stage.toCompletableFuture().completeExceptionally( t );
			return stage;
		}
	}

	@Override
	protected void doDynamicDelete(Object entity, Object id, Object rowId, Object[] loadedState, SharedSessionContractImplementor session) {
		stage = new CompletableFuture<>();
		final MutationOperationGroup operationGroup = generateOperationGroup( loadedState, true, session );
		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, operationGroup );

		operationGroup.forEachOperation( (position, mutation) -> {
			if ( mutation != null ) {
				final String tableName = mutation.getTableDetails().getTableName();
				mutationExecutor.getPreparedStatementDetails( tableName );
			}
		} );

		applyLocking( null, loadedState, mutationExecutor, session );

		applyId( id, rowId, mutationExecutor, operationGroup, session );


		mutationExecutor.execute(
						entity,
						null,
						null,
						(statementDetails, affectedRowCount, batchPosition) -> ModelMutationHelper.identifiedResultsCheck(
								statementDetails,
								affectedRowCount,
								batchPosition,
								entityPersister(),
								id,
								factory()
						),
						session
				)
				.whenComplete( (o, throwable) -> mutationExecutor.release() )
				.whenComplete(this::complete);
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

	@Override
	protected void doStaticDelete(Object entity, Object id, Object version, SharedSessionContractImplementor session) {
		final MutationExecutorService mutationExecutorService = session
				.getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		final boolean applyVersion = entity != null;
		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( entity, session, mutationExecutorService );

		getStaticDeleteGroup().forEachOperation( (position, mutation) -> {
			if ( mutation != null ) {
				final String tableName = mutation.getTableDetails().getTableName();
				mutationExecutor.getPreparedStatementDetails( tableName );
			}
		} );

		if ( applyVersion ) {
			applyLocking( version, null, mutationExecutor, session );
		}

		applyId( id, null, mutationExecutor, getStaticDeleteGroup(), session );

		mutationExecutor.execute(
						entity,
						null,
						null,
						(statementDetails, affectedRowCount, batchPosition) -> ModelMutationHelper.identifiedResultsCheck(
								statementDetails,
								affectedRowCount,
								batchPosition,
								entityPersister(),
								id,
								factory()
						),
						session
				)
				.whenComplete( (o, throwable) -> mutationExecutor.release() )
				.whenComplete( this::complete );
	}

	private ReactiveMutationExecutor mutationExecutor(
			Object entity,
			SharedSessionContractImplementor session,
			MutationExecutorService mutationExecutorService) {
		final MutationOperationGroup operationGroupToUse = entity == null
				? resolveNoVersionDeleteGroup( session )
				: getStaticDeleteGroup();

		return (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( this::getBatchKey, operationGroupToUse, session );
	}
}
