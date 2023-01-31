/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal;

import java.util.Map;
import java.util.function.Supplier;

import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.persister.entity.mutation.EntityMutationTarget;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorPostInsert;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorPostInsertSingleTable;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorSingleBatched;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorSingleNonBatched;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorSingleSelfExecuting;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorStandard;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.MutationTarget;
import org.hibernate.sql.model.MutationType;
import org.hibernate.sql.model.PreparableMutationOperation;
import org.hibernate.sql.model.SelfExecutingUpdateOperation;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.StandardMutationExecutorService
 */
public class ReactiveStandardMutationExecutorService implements MutationExecutorService {

	private final int globalBatchSize;

	public ReactiveStandardMutationExecutorService(Map<String, Object> configurationValues) {
		this( ConfigurationHelper.getInt( Environment.STATEMENT_BATCH_SIZE, configurationValues, 1 ) );
	}

	public ReactiveStandardMutationExecutorService(int globalBatchSize) {
		this.globalBatchSize = globalBatchSize;
	}

	//FIXME: It would be nice to have a factory to pass to the ORM method
	@Override
	public MutationExecutor createExecutor(
			Supplier<BatchKey> batchKeySupplier,
			MutationOperationGroup operationGroup,
			SharedSessionContractImplementor session) {
		// decide whether to use batching - any number > one means to batch
		final Integer sessionBatchSize = session.getJdbcCoordinator()
				.getJdbcSessionOwner()
				.getJdbcBatchSize();
		final int batchSizeToUse = sessionBatchSize == null
				? globalBatchSize
				: sessionBatchSize;

		final int numberOfOperations = operationGroup.getNumberOfOperations();
		final MutationType mutationType = operationGroup.getMutationType();
		final MutationTarget<?> mutationTarget = operationGroup.getMutationTarget();

		if ( mutationType == MutationType.INSERT
				&& mutationTarget instanceof EntityMutationTarget
				&& ( (EntityMutationTarget) mutationTarget ).getIdentityInsertDelegate() != null ) {
			assert mutationTarget instanceof EntityMappingType;

			if ( numberOfOperations > 1 ) {
				return new ReactiveMutationExecutorPostInsert( operationGroup, session );
			}

			return new ReactiveMutationExecutorPostInsertSingleTable( operationGroup, session );
		}

		if ( numberOfOperations == 1 ) {
			final MutationOperation singleOperation = operationGroup.getSingleOperation();
			if ( singleOperation instanceof SelfExecutingUpdateOperation ) {
				return new ReactiveMutationExecutorSingleSelfExecuting( (SelfExecutingUpdateOperation) singleOperation, session );
			}

			final PreparableMutationOperation jdbcOperation = (PreparableMutationOperation) singleOperation;
			final BatchKey batchKey = batchKeySupplier.get();
			if ( jdbcOperation.canBeBatched( batchKey, batchSizeToUse ) ) {
				return new ReactiveMutationExecutorSingleBatched( jdbcOperation, batchKey, batchSizeToUse, session );
			}

			return new ReactiveMutationExecutorSingleNonBatched( jdbcOperation, session );
		}

		return new ReactiveMutationExecutorStandard( operationGroup, batchKeySupplier, batchSizeToUse, session );
	}
}
