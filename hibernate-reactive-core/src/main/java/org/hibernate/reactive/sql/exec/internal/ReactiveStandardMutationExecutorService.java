/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal;

import java.util.Map;

import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.spi.BatchKeyAccess;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorSingleBatched;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorSingleNonBatched;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorSingleSelfExecuting;
import org.hibernate.reactive.engine.jdbc.mutation.internal.ReactiveMutationExecutorStandard;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.MutationOperationGroup;
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
			BatchKeyAccess batchKeySupplier,
			MutationOperationGroup operationGroup,
			SharedSessionContractImplementor session) {
		// decide whether to use batching - any number > one means to batch
		final Integer sessionBatchSize = session.getJdbcCoordinator()
				.getJdbcSessionOwner()
				.getJdbcBatchSize();
		final int batchSizeToUse = sessionBatchSize == null
				? globalBatchSize
				: sessionBatchSize;

		if ( operationGroup.getNumberOfOperations() == 1 ) {
			final MutationOperation singleOperation = operationGroup.getSingleOperation();
			if ( singleOperation instanceof SelfExecutingUpdateOperation ) {
				return new ReactiveMutationExecutorSingleSelfExecuting( (SelfExecutingUpdateOperation) singleOperation, session );
			}

			final PreparableMutationOperation jdbcOperation = (PreparableMutationOperation) singleOperation;
			final BatchKey batchKey = batchKeySupplier.getBatchKey();
			if ( jdbcOperation.canBeBatched( batchKey, batchSizeToUse ) ) {
				return new ReactiveMutationExecutorSingleBatched( jdbcOperation, batchKey, batchSizeToUse, session );
			}

			return new ReactiveMutationExecutorSingleNonBatched( jdbcOperation, generatedValuesDelegate( operationGroup ), session );
		}

		return new ReactiveMutationExecutorStandard( operationGroup, batchKeySupplier, batchSizeToUse, session );
	}

	private static GeneratedValuesMutationDelegate generatedValuesDelegate(MutationOperationGroup operationGroup) {
		return operationGroup.asEntityMutationOperationGroup() != null
				? operationGroup.asEntityMutationOperationGroup().getMutationDelegate()
				: null;
	}
}
