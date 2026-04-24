/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.Internal;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.internal.NoBatchKeyAccess;
import org.hibernate.engine.jdbc.mutation.spi.BatchKeyAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.persister.entity.mutation.InsertCoordinatorStandard;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.tuple.entity.EntityMetamodel;

import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.reactive.persister.entity.mutation.GeneratorValueUtil.generateValue;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.supplyStage;

/**
 * @see InsertCoordinatorStandard
 */
@Internal
public class ReactiveInsertCoordinatorStandard extends InsertCoordinatorStandard implements ReactiveInsertCoordinator, InsertCoordinator {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveInsertCoordinatorStandard(EntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public GeneratedValues insert(Object entity, Object[] values, SharedSessionContractImplementor session) {
		return insert( entity, null, values, session );
	}

	@Override
	public GeneratedValues insert(Object entity, Object id, Object[] values, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveInsert" );
	}

	@Override
	public CompletionStage<GeneratedValues> reactiveInsert(Object entity, Object[] values, SharedSessionContractImplementor session) {
		return reactiveInsert( entity, null, values, session );
	}

	@Override
	public CompletionStage<GeneratedValues> reactiveInsert(Object entity, Object id, Object[] values, SharedSessionContractImplementor session) {
		return coordinateReactiveInsert( entity, id, values, session, true );
	}

	public CompletionStage<GeneratedValues> coordinateReactiveInsert(
			Object entity,
			Object id,
			Object[] values,
			SharedSessionContractImplementor session,
			boolean isIdentityInsert) {
		return reactivePreInsertInMemoryValueGeneration( values, entity, session )
				.thenCompose( needsDynamicInsert -> {
					final boolean forceIdentifierBinding = entityPersister().getGenerator().generatedOnExecution() && id != null;
					return entityPersister().getEntityMetamodel().isDynamicInsert() || needsDynamicInsert || forceIdentifierBinding
						? doDynamicInserts( id, values, entity, session, forceIdentifierBinding, isIdentityInsert )
						: doStaticInserts( id, values, entity, session, isIdentityInsert );
				} );
	}

	private CompletionStage<Boolean> reactivePreInsertInMemoryValueGeneration(Object[] currentValues, Object entity, SharedSessionContractImplementor session) {
		final EntityMetamodel entityMetamodel = entityPersister().getEntityMetamodel();
		CompletionStage<Boolean> stage = falseFuture();
		if ( entityMetamodel.hasPreInsertGeneratedValues() ) {
			final Generator[] generators = entityMetamodel.getGenerators();
			for ( int i = 0; i < generators.length; i++ ) {
				final int index = i;
				final Generator generator = generators[i];
				if ( generator != null
						&& !generator.generatedOnExecution()
						&& generator.generatesOnInsert() ) {
					final Object currentValue = currentValues[i];
					final BeforeExecutionGenerator beforeGenerator = (BeforeExecutionGenerator) generator;
					stage = stage
							.thenCompose( foundStateDependentGenerator -> generateValue(
												  session,
												  entity,
												  currentValue,
												  beforeGenerator,
												  INSERT
										  )
												  .thenApply( generatedValue -> {
													  currentValues[index] = generatedValue;
													  entityPersister().setValue( entity, index, generatedValue );
													  return foundStateDependentGenerator || beforeGenerator.generatedOnExecution();
												  } )
							);
				}
			}
		}

		return stage;
	}

	protected CompletionStage<GeneratedValues> doDynamicInserts(
			Object id,
			Object[] values,
			Object object,
			SharedSessionContractImplementor session,
			boolean forceIdentifierBinding,
			boolean isIdentityInsert) {
		final boolean[] propertiesToInsert = getPropertiesToInsert( entityPersister(), values );
		final MutationOperationGroup insertGroup = generateDynamicInsertSqlGroup( propertiesToInsert, object, session, forceIdentifierBinding );
		final ReactiveMutationExecutor mutationExecutor = getReactiveMutationExecutor( session, insertGroup, true );

		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );
		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );
		decomposeForInsert( mutationExecutor, id, values, object, insertGroup, propertiesToInsert, tableInclusionChecker, session );
		return supplyStage( () -> mutationExecutor.executeReactive(
								object,
								insertValuesAnalysis,
								tableInclusionChecker,
								(statementDetails, affectedRowCount, batchPosition) -> {
									statementDetails.getExpectation()
											.verifyOutcome(
													affectedRowCount,
													statementDetails.getStatement(),
													batchPosition,
													statementDetails.getSqlString()
											);
									return true;
								},
								session,
								isIdentityInsert,
								entityPersister().getIdentifierColumnNames()
						)
						.whenComplete( (o, t) -> mutationExecutor.release() ) );
	}

	protected CompletionStage<GeneratedValues> doStaticInserts(Object id, Object[] values, Object object, SharedSessionContractImplementor session, boolean isIdentityInsert) {
		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );
		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );
		final ReactiveMutationExecutor mutationExecutor = getReactiveMutationExecutor( session, getStaticMutationOperationGroup(), false );

		decomposeForInsert( mutationExecutor, id, values, object, getStaticMutationOperationGroup(), entityPersister().getPropertyInsertability(), tableInclusionChecker, session );
		return supplyStage( () -> mutationExecutor.executeReactive(
				object,
				insertValuesAnalysis,
				tableInclusionChecker,
				(statementDetails, affectedRowCount, batchPosition) -> {
					statementDetails.getExpectation().verifyOutcome(
							affectedRowCount,
							statementDetails.getStatement(),
							batchPosition,
							statementDetails.getSqlString()
					);
					return true;
				},
				session,
				isIdentityInsert,
				entityPersister().getIdentifierColumnNames()
		) ).whenComplete( (generatedValues, throwable) -> mutationExecutor.release() );
	}


	protected static TableInclusionChecker getTableInclusionChecker(InsertValuesAnalysis insertValuesAnalysis) {
		return tableMapping -> !tableMapping.isOptional() || insertValuesAnalysis.hasNonNullBindings( tableMapping );
	}

	private ReactiveMutationExecutor getReactiveMutationExecutor(SharedSessionContractImplementor session, MutationOperationGroup operationGroup, boolean dynamicUpdate) {
		return (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( resolveBatchKeyAccess( dynamicUpdate, session ), operationGroup, session );
	}

	@Override
	protected BatchKeyAccess resolveBatchKeyAccess(boolean dynamicUpdate, SharedSessionContractImplementor session) {
		if ( !dynamicUpdate
				&& !entityPersister().optimisticLockStyle().isAllOrDirty()
				&& session.getTransactionCoordinator() != null
//				&& session.getTransactionCoordinator().isTransactionActive()
		) {
			return this::getBatchKey;
		}

		return NoBatchKeyAccess.INSTANCE;
	}
}
