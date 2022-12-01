/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.env.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.sql.model.ValuesAnalysis;

/**
 * @see org.hibernate.engine.jdbc.mutation.MutationExecutor
 */
public interface ReactiveMutationExecutor extends MutationExecutor {

	@Override
	CompletionStage<Object> execute(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session);
}
