/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorPostInsert;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveMutationExecutor;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.ValuesAnalysis;

public class ReactiveMutationExecutorPostInsert extends MutationExecutorPostInsert implements ReactiveMutationExecutor {

	public ReactiveMutationExecutorPostInsert(
			MutationOperationGroup mutationOperationGroup,
			SharedSessionContractImplementor session) {
		super( mutationOperationGroup, session );
	}

	public Object execute(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "executeReactive" );
	}
}
