/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.concurrent.CompletionStage;

import org.hibernate.Internal;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.model.MutationOperationGroup;

@Internal
public class ReactiveInsertCoordinator extends InsertCoordinator {

	public ReactiveInsertCoordinator(AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public Object coordinateInsert(Object id, Object[] values, Object entity, SharedSessionContractImplementor session) {
		return super.coordinateInsert( id, values, entity, session );
	}

	@Override
	protected Object doDynamicInserts(Object id, Object[] values, Object object, SharedSessionContractImplementor session) {
		final boolean[] insertability = getPropertiesToInsert( values );
		final MutationOperationGroup insertGroup = generateDynamicInsertSqlGroup( insertability );
		final ReactiveMutationExecutor mutationExecutor = getReactiveMutationExecutor( session, insertGroup );

		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );
		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );
		decomposeForInsert( mutationExecutor, id, values, insertGroup, insertability, tableInclusionChecker, session );

		return mutationExecutor.execute(
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
				.whenComplete( (o, throwable) -> mutationExecutor.release() );
	}

	@Override
	protected CompletionStage<Object> doStaticInserts(Object id, Object[] values, Object object, SharedSessionContractImplementor session) {
		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );
		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );
		final ReactiveMutationExecutor mutationExecutor = getReactiveMutationExecutor( session, getStaticInsertGroup() );

		decomposeForInsert( mutationExecutor, id, values, getStaticInsertGroup(), entityPersister().getPropertyInsertability(), tableInclusionChecker, session );

		return mutationExecutor.execute(
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
				session
		);
	}

	private ReactiveMutationExecutor getReactiveMutationExecutor(SharedSessionContractImplementor session, MutationOperationGroup operationGroup) {
		final MutationExecutorService mutationExecutorService = session
				.getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		return  (ReactiveMutationExecutor) mutationExecutorService
				.createExecutor( this::getInsertBatchKey, operationGroup, session );
	}
}
