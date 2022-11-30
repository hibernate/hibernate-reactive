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
import org.hibernate.reactive.sql.exec.spi.ReactiveJdbcMutationExecutor;
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
	protected Object doDynamicInserts(
			Object id,
			Object[] values,
			Object object,
			SharedSessionContractImplementor session) {
		final boolean[] insertability = getPropertiesToInsert( values );
		final MutationOperationGroup insertGroup = generateDynamicInsertSqlGroup( insertability );

		final MutationExecutorService mutationExecutorService = session
				.getFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		final ReactiveJdbcMutationExecutor mutationExecutor = (ReactiveJdbcMutationExecutor) mutationExecutorService
				.createExecutor( this::getInsertBatchKey, insertGroup, session );

		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );

		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );

		decomposeForInsert( mutationExecutor, id, values, insertGroup, insertability, tableInclusionChecker, session );

		try {
			return mutationExecutor.executeReactive(
					object,
					insertValuesAnalysis,
					tableInclusionChecker,
					(statementDetails, affectedRowCount, batchPosition) -> {
						statementDetails.getExpectation()
								.verifyOutcome( affectedRowCount,
								statementDetails.getStatement(),
								batchPosition,
								statementDetails.getSqlString()
						);
						return true;
					},
					session
			);
		}
		finally {
			mutationExecutor.release();
		}
	}

	@Override
	protected CompletionStage<Object> doStaticInserts(Object id, Object[] values, Object object, SharedSessionContractImplementor session) {
		final InsertValuesAnalysis insertValuesAnalysis = new InsertValuesAnalysis( entityPersister(), values );

		final TableInclusionChecker tableInclusionChecker = getTableInclusionChecker( insertValuesAnalysis );

		final MutationExecutorService mutationExecutorService = session.getSessionFactory()
				.getServiceRegistry()
				.getService( MutationExecutorService.class );

		final ReactiveJdbcMutationExecutor mutationExecutor = (ReactiveJdbcMutationExecutor) mutationExecutorService
				.createExecutor( this::getInsertBatchKey, getStaticInsertGroup(), session );

		decomposeForInsert(
				mutationExecutor,
				id,
				values,
				getStaticInsertGroup(),
				entityPersister().getPropertyInsertability(),
				tableInclusionChecker,
				session
		);

		try {
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
		finally {
			mutationExecutor.release();
		}
	}
}
