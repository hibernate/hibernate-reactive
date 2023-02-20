/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityVersionMapping;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.AttributeAnalysis;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.persister.entity.mutation.UpdateCoordinatorStandard;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.model.MutationOperationGroup;

import static org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper.identifiedResultsCheck;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class ReactiveUpdateCoordinatorStandard extends UpdateCoordinatorStandard implements ReactiveUpdateCoordinator {

	private CompletionStage<Void> stage;


	public ReactiveUpdateCoordinatorStandard(AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	private void complete(Object o, Throwable throwable) {
		if ( throwable != null ) {
			stage.toCompletableFuture().completeExceptionally( throwable );
		}
		else {
			stage.toCompletableFuture().complete( null );
		}
	}

	@Override
	public CompletionStage<Void> coordinateReactiveUpdate(
			Object entity,
			Object id,
			Object rowId,
			Object[] values,
			Object oldVersion,
			Object[] incomingOldValues,
			int[] dirtyAttributeIndexes,
			boolean hasDirtyCollection,
			SharedSessionContractImplementor session) {
		stage = null;
		try {
			super.coordinateUpdate(
					entity,
					id,
					rowId,
					values,
					oldVersion,
					incomingOldValues,
					dirtyAttributeIndexes,
					hasDirtyCollection,
					session
			);
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
	protected void doVersionUpdate(
			Object entity,
			Object id,
			Object version,
			Object oldVersion,
			SharedSessionContractImplementor session) {
		assert getVersionUpdateGroup() != null;
		this.stage = new CompletableFuture<>();

		final EntityTableMapping mutatingTableDetails = (EntityTableMapping) getVersionUpdateGroup()
				.getSingleOperation().getTableDetails();
		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, getVersionUpdateGroup() );
		final EntityVersionMapping versionMapping = entityPersister().getVersionMapping();

		// set the new version
		mutationExecutor.getJdbcValueBindings().bindValue(
				version,
				mutatingTableDetails.getTableName(),
				versionMapping.getSelectionExpression(),
				ParameterUsage.SET
		);

		// restrict the key
		mutatingTableDetails.getKeyMapping().breakDownKeyJdbcValues(
				id,
				(jdbcValue, columnMapping) -> mutationExecutor.getJdbcValueBindings().bindValue(
						jdbcValue,
						mutatingTableDetails.getTableName(),
						columnMapping.getColumnName(),
						ParameterUsage.RESTRICT
				),
				session
		);

		// restrict the old-version
		mutationExecutor.getJdbcValueBindings().bindValue(
				oldVersion,
				mutatingTableDetails.getTableName(),
				versionMapping.getSelectionExpression(),
				ParameterUsage.RESTRICT
		);

		mutationExecutor.executeReactive(
						entity,
						null,
						(tableMapping) -> tableMapping.getTableName().equals( entityPersister().getIdentifierTableName() ),
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

	private ReactiveMutationExecutor mutationExecutor(
			SharedSessionContractImplementor session,
			MutationOperationGroup operationGroup) {
		final MutationExecutorService mutationExecutorService = session.getSessionFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		return (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( this::getBatchKey, operationGroup, session );
	}

	@Override
	protected void doDynamicUpdate(
			Object entity,
			Object id,
			Object rowId,
			Object[] values,
			Object[] oldValues,
			UpdateCoordinatorStandard.InclusionChecker dirtinessChecker,
			UpdateCoordinatorStandard.UpdateValuesAnalysisImpl valuesAnalysis,
			SharedSessionContractImplementor session) {
		this.stage = new CompletableFuture<>();
		// Create the JDBC operation descriptors
		final MutationOperationGroup dynamicUpdateGroup = generateDynamicUpdateGroup(
				id,
				rowId,
				oldValues,
				valuesAnalysis,
				session
		);

		// and then execute them
		final MutationExecutorService mutationExecutorService = session.getSessionFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, dynamicUpdateGroup );

		decomposeForUpdate(
				id,
				rowId,
				values,
				valuesAnalysis,
				mutationExecutor,
				dynamicUpdateGroup,
				(attributeIndex, attribute) -> dirtinessChecker.include( attributeIndex, (SingularAttributeMapping) attribute ) ? AttributeAnalysis.DirtynessStatus.CONSIDER_LIKE_DIRTY : AttributeAnalysis.DirtynessStatus.NOT_DIRTY,
				session
		);
		bindPartitionColumnValueBindings( oldValues, session, mutationExecutor.getJdbcValueBindings() );

		mutationExecutor.executeReactive(
						entity,
						valuesAnalysis,
						(tableMapping) -> {
							if ( tableMapping.isOptional()
									&& !valuesAnalysis.getTablesWithNonNullValues().contains( tableMapping ) ) {
								// the table is optional, and we have null values for all of its columns
								// todo (6.0) : technically we might need to delete row here
								return false;
							}

							//noinspection RedundantIfStatement
							if ( !valuesAnalysis.getTablesNeedingUpdate().contains( tableMapping ) ) {
								// nothing changed
								return false;
							}

							return true;
						},
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
				.whenComplete( (o, throwable) -> mutationExecutor.release() )
				.whenComplete( this::complete );
	}

	@Override
	protected void doStaticUpdate(
			Object entity,
			Object id,
			Object rowId,
			Object[] values,
			Object[] oldValues,
			UpdateValuesAnalysisImpl valuesAnalysis,
			SharedSessionContractImplementor session) {
		this.stage = new CompletableFuture<>();
		final MutationOperationGroup staticUpdateGroup = getStaticUpdateGroup();
		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, staticUpdateGroup );

		decomposeForUpdate(
				id,
				rowId,
				values,
				valuesAnalysis,
				mutationExecutor,
				staticUpdateGroup,
				(position, attribute) -> AttributeAnalysis.DirtynessStatus.CONSIDER_LIKE_DIRTY,
				session
		);
		bindPartitionColumnValueBindings( oldValues, session, mutationExecutor.getJdbcValueBindings() );

		mutationExecutor.executeReactive(
						entity,
						valuesAnalysis,
						valuesAnalysis.getTablesNeedingUpdate()::contains,
						(statementDetails, affectedRowCount, batchPosition) -> identifiedResultsCheck( statementDetails, affectedRowCount, batchPosition, entityPersister(), id, factory() ),
						session
				)
				.whenComplete( (o, throwable) -> mutationExecutor.release() )
				.whenComplete( this::complete );
	}
}
