/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import java.util.concurrent.atomic.AtomicInteger;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.metamodel.mapping.EntityVersionMapping;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.AttributeAnalysis;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.persister.entity.mutation.UpdateCoordinatorStandard;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.tuple.entity.EntityMetamodel;

import static org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper.identifiedResultsCheck;
import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.internal.util.collections.ArrayHelper.EMPTY_INT_ARRAY;
import static org.hibernate.internal.util.collections.ArrayHelper.trim;
import static org.hibernate.reactive.persister.entity.mutation.GeneratorValueUtil.generateValue;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
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
			int[] incomingDirtyAttributeIndexes,
			boolean hasDirtyCollection,
			SharedSessionContractImplementor session) {
		final EntityVersionMapping versionMapping = entityPersister().getVersionMapping();
		if ( versionMapping != null ) {
			final boolean isForcedVersionIncrement = handlePotentialImplicitForcedVersionIncrement(
					entity,
					id,
					values,
					oldVersion,
					incomingDirtyAttributeIndexes,
					session,
					versionMapping
			);
			if ( isForcedVersionIncrement ) {
				return voidFuture();
			}
		}

		final EntityEntry entry = session.getPersistenceContextInternal().getEntry( entity );

		// Ensure that an immutable or non-modifiable entity is not being updated unless it is
		// in the process of being deleted.
		if ( entry == null && !entityPersister().isMutable() ) {
			return CompletionStages.failedFuture(new IllegalStateException( "Updating immutable entity that is not in session yet" ));
		}

		CompletionStage<Void> s = voidFuture();
		return s.thenCompose( v -> reactivePreUpdateInMemoryValueGeneration(entity, values, session) )
				.thenCompose( preUpdateGeneratedAttributeIndexes -> {
					final int[] dirtyAttributeIndexes = dirtyAttributeIndexes( incomingDirtyAttributeIndexes, preUpdateGeneratedAttributeIndexes );

					final boolean[] attributeUpdateability;
					boolean forceDynamicUpdate;

					if ( entityPersister().getEntityMetamodel().isDynamicUpdate() && dirtyAttributeIndexes != null ) {
						attributeUpdateability = getPropertiesToUpdate( dirtyAttributeIndexes, hasDirtyCollection );
						forceDynamicUpdate = true;
					}
					else if ( !isModifiableEntity( entry ) ) {
						// either the entity is mapped as immutable or has been marked as read-only within the Session
						attributeUpdateability = getPropertiesToUpdate(
								dirtyAttributeIndexes == null ? EMPTY_INT_ARRAY : dirtyAttributeIndexes,
								hasDirtyCollection
						);
						forceDynamicUpdate = true;
					}
					else if ( dirtyAttributeIndexes != null
							&& entityPersister().hasUninitializedLazyProperties( entity )
							&& entityPersister().hasLazyDirtyFields( dirtyAttributeIndexes ) ) {
						// we have an entity with dirty lazy attributes.  we need to use dynamic
						// delete and add the dirty, lazy attributes plus the non-lazy attributes
						forceDynamicUpdate = true;
						attributeUpdateability = getPropertiesToUpdate( dirtyAttributeIndexes, hasDirtyCollection );

						final boolean[] propertyLaziness = entityPersister().getPropertyLaziness();
						for ( int i = 0; i < propertyLaziness.length; i++ ) {
							// add also all the non-lazy properties because dynamic update is false
							if ( !propertyLaziness[i] ) {
								attributeUpdateability[i] = true;
							}
						}
					}
					else {
						attributeUpdateability = getPropertyUpdateability( entity );
						forceDynamicUpdate = entityPersister().hasUninitializedLazyProperties( entity );
					}

					performUpdate(
							entity,
							id,
							rowId,
							values,
							oldVersion,
							incomingOldValues,
							hasDirtyCollection,
							session,
							versionMapping,
							dirtyAttributeIndexes,
							attributeUpdateability,
							forceDynamicUpdate
					);

					// stage gets updated by doDynamicUpdate and doStaticUpdate which get called by performUpdate
					return stage != null ? stage : voidFuture();
				});
	}

	private CompletionStage<int[]> reactivePreUpdateInMemoryValueGeneration(
			Object entity,
			Object[] currentValues,
			SharedSessionContractImplementor session) {
		final EntityMetamodel entityMetamodel = entityPersister().getEntityMetamodel();
		if ( !entityMetamodel.hasPreUpdateGeneratedValues() ) {
			return completedFuture(EMPTY_INT_ARRAY);
		}

		CompletionStage<Void> result = voidFuture();

		final Generator[] generators = entityMetamodel.getGenerators();
		if ( generators.length != 0 ) {
			final int[] fieldsPreUpdateNeeded = new int[generators.length];

			AtomicInteger count = new AtomicInteger(0);
			for ( int i = 0; i < generators.length; i++ ) {
				final int index = i;
				final Generator generator = generators[i];
				if ( generator != null
						&& !generator.generatedOnExecution()
						&& generator.generatesOnUpdate() ) {
					final Object currentValue = currentValues[i];
					final BeforeExecutionGenerator beforeGenerator = (BeforeExecutionGenerator) generator;
					result = result.thenCompose( v -> generateValue( session, entity, currentValue, beforeGenerator, INSERT )
							.thenAccept( generatedValue -> {
								currentValues[index] = generatedValue;
								entityPersister().setPropertyValue( entity, index, generatedValue );
								fieldsPreUpdateNeeded[count.getAndIncrement()] = index;
							} ) );
				}
			}

			return result.thenApply(v -> {
				if (count.get() > 0) {
					return trim( fieldsPreUpdateNeeded, count.get() );
				} else {
					return EMPTY_INT_ARRAY;
				}
			});
		}

		return completedFuture(EMPTY_INT_ARRAY);
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
		return (ReactiveMutationExecutor) mutationExecutorService.createExecutor( this::getBatchKey, operationGroup, session );
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

		final ReactiveMutationExecutor mutationExecutor = mutationExecutor( session, dynamicUpdateGroup );

		decomposeForUpdate(
				id,
				rowId,
				values,
				valuesAnalysis,
				mutationExecutor,
				dynamicUpdateGroup,
				(attributeIndex, attribute) -> dirtinessChecker.include( attributeIndex, (SingularAttributeMapping) attribute )
						? AttributeAnalysis.DirtynessStatus.CONSIDER_LIKE_DIRTY
						: AttributeAnalysis.DirtynessStatus.NOT_DIRTY,
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
						(statementDetails, affectedRowCount, batchPosition)
								-> identifiedResultsCheck( statementDetails, affectedRowCount, batchPosition, entityPersister(), id, factory() ),
						session
				)
				.whenComplete( (o, throwable) -> mutationExecutor.release() )
				.whenComplete( this::complete );
	}
}
