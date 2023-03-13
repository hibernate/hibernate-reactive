/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.Internal;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.BeforeExecutionGenerator;
import org.hibernate.generator.Generator;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.AttributeMappingsList;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.tuple.entity.EntityMetamodel;

import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.reactive.persister.entity.mutation.GeneratorValueUtil.generateValue;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

@Internal
public class ReactiveInsertCoordinator extends InsertCoordinator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveInsertCoordinator(AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public Object coordinateInsert(Object id, Object[] values, Object entity, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "coordinateReactiveInsert" );
	}

	public CompletionStage<Object> coordinateReactiveInsert(Object id, Object[] currentValues, Object entity, SharedSessionContractImplementor session) {
		return reactivePreInsertInMemoryValueGeneration(currentValues, entity, session)
				.thenCompose( v -> {
					return entityPersister().getEntityMetamodel().isDynamicInsert()
				? doDynamicInserts( id, currentValues, entity, session )
				: doStaticInserts( id, currentValues, entity, session );
				});
	}

	private CompletionStage<Void> reactivePreInsertInMemoryValueGeneration(Object[] currentValues, Object entity, SharedSessionContractImplementor session) {
		CompletionStage<Void> stage = voidFuture();

		final EntityMetamodel entityMetamodel = entityPersister().getEntityMetamodel();
		if ( entityMetamodel.hasPreInsertGeneratedValues() ) {
			final Generator[] generators = entityMetamodel.getGenerators();
			for ( int i = 0; i < generators.length; i++ ) {
				final int index = i;
				final Generator generator = generators[i];
				if ( generator != null
						&& !generator.generatedOnExecution()
						&& generator.generatesOnInsert() ) {
					final Object currentValue = currentValues[i];
					stage = stage.thenCompose( v -> generateValue( session, entity, currentValue,
							(BeforeExecutionGenerator) generator, INSERT)
							.thenAccept( generatedValue -> {
								currentValues[index] = generatedValue;
								entityPersister().setPropertyValue( entity, index, generatedValue );
							} ) );
				}
			}
		}

		return stage;
	}

	@Override
	protected void decomposeForInsert(
			MutationExecutor mutationExecutor,
			Object id,
			Object[] values,
			MutationOperationGroup mutationGroup,
			boolean[] propertyInclusions,
			TableInclusionChecker tableInclusionChecker,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "decomposeForReactiveInsert" );
	}

	protected CompletionStage<Void> decomposeForReactiveInsert(
			MutationExecutor mutationExecutor,
			Object id,
			Object[] values,
			MutationOperationGroup mutationGroup,
			boolean[] propertyInclusions,
			TableInclusionChecker tableInclusionChecker,
			SharedSessionContractImplementor session) {
		final JdbcValueBindings jdbcValueBindings = mutationExecutor.getJdbcValueBindings();
		final AttributeMappingsList attributeMappings = entityPersister().getAttributeMappings();
		mutationGroup.forEachOperation( (position, operation) -> {
			final EntityTableMapping tableDetails = (EntityTableMapping) operation.getTableDetails();
			if ( tableInclusionChecker.include( tableDetails ) ) {
				final int[] attributeIndexes = tableDetails.getAttributeIndexes();
				for ( final int attributeIndex : attributeIndexes ) {
					if ( propertyInclusions[attributeIndex] ) {
						final AttributeMapping mapping = entityPersister().getAttributeMappings().get( attributeIndex );
						decomposeAttribute( values[attributeIndex], session, jdbcValueBindings, mapping );
					}
				}
			}
		} );

		mutationGroup.forEachOperation( (position, jdbcOperation) -> {
			if ( id == null )  {
				assert entityPersister().getIdentityInsertDelegate() != null;
			}
			else {
				final EntityTableMapping tableDetails = (EntityTableMapping) jdbcOperation.getTableDetails();
				breakDownJdbcValue( id, session, jdbcValueBindings, tableDetails );
			}
		} );
		return voidFuture();
	}

	@Override
	protected CompletionStage<Object> doDynamicInserts(Object id, Object[] values, Object object, SharedSessionContractImplementor session) {
		final boolean[] insertability = getPropertiesToInsert( values );
		final MutationOperationGroup insertGroup = generateDynamicInsertSqlGroup( insertability );
		final ReactiveMutationExecutor mutationExecutor = getReactiveMutationExecutor( session, insertGroup );

		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );
		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );
		return decomposeForReactiveInsert( mutationExecutor, id, values, insertGroup, insertability, tableInclusionChecker, session )
				.thenCompose( v -> mutationExecutor.executeReactive(
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
								session
						)
						.whenComplete( (o, t) -> mutationExecutor.release() ) );
	}

	@Override
	protected CompletionStage<Object> doStaticInserts(Object id, Object[] values, Object object, SharedSessionContractImplementor session) {
		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );
		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );
		final ReactiveMutationExecutor mutationExecutor = getReactiveMutationExecutor( session, getStaticInsertGroup() );

		return decomposeForReactiveInsert( mutationExecutor, id, values, getStaticInsertGroup(), entityPersister().getPropertyInsertability(), tableInclusionChecker, session )
				.thenCompose( v -> mutationExecutor.executeReactive(
					object,
					insertValuesAnalysis,
					tableInclusionChecker,
					(statementDetails, affectedRowCount, batchPosition) -> {
						statementDetails
								.getExpectation()
								.verifyOutcome( affectedRowCount, statementDetails.getStatement(), batchPosition, statementDetails.getSqlString() );
						return true;
					},
					session
			) );
	}

	private ReactiveMutationExecutor getReactiveMutationExecutor(SharedSessionContractImplementor session, MutationOperationGroup operationGroup) {
		final MutationExecutorService mutationExecutorService = session
				.getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		MutationExecutor executor = mutationExecutorService
				.createExecutor( this::getInsertBatchKey, operationGroup, session );
		return  (ReactiveMutationExecutor) executor;
	}
}
