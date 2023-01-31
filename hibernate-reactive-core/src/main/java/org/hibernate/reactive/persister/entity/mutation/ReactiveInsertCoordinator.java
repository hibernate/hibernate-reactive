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
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.AttributeMappingsList;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationOperationGroup;

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

	public CompletionStage<Object> coordinateReactiveInsert(Object id, Object[] values, Object entity, SharedSessionContractImplementor session) {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( values, entity, session );

		return entityPersister().getEntityMetamodel().isDynamicInsert()
				? doDynamicInserts( id, values, entity, session )
				: doStaticInserts( id, values, entity, session );
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

	// Copy and paste from ORM: InsertCoordinator#decomposeAttribute
	private void decomposeAttribute(
			Object value,
			SharedSessionContractImplementor session,
			JdbcValueBindings jdbcValueBindings,
			AttributeMapping mapping) {
		if ( !(mapping instanceof PluralAttributeMapping) ) {
			mapping.decompose(
					value,
					(jdbcValue, selectableMapping) -> {
						if ( selectableMapping.isInsertable() ) {
							jdbcValueBindings.bindValue(
									jdbcValue,
									entityPersister().physicalTableNameForMutation( selectableMapping ),
									selectableMapping.getSelectionExpression(),
									ParameterUsage.SET
							);
						}
					},
					session
			);
		}
	}

	// Copy and paste from ORM: InsertCoordinator#breakDownJdbcValue
	private static void breakDownJdbcValue(
			Object id,
			SharedSessionContractImplementor session,
			JdbcValueBindings jdbcValueBindings,
			EntityTableMapping tableDetails) {
		final String tableName = tableDetails.getTableName();
		tableDetails.getKeyMapping().breakDownKeyJdbcValues(
				id,
				(jdbcValue, columnMapping) -> jdbcValueBindings.bindValue(
						jdbcValue,
						tableName,
						columnMapping.getColumnName(),
						ParameterUsage.SET
				),
				session
		);
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
