/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorPostInsertSingleTable;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.sql.model.MutationOperationGroup;

public class ReactiveMutationExecutorPostInsertSingleTable extends MutationExecutorPostInsertSingleTable implements MutationExecutor {

	public ReactiveMutationExecutorPostInsertSingleTable(
			MutationOperationGroup mutationOperationGroup,
			SharedSessionContractImplementor session) {
		super( mutationOperationGroup, session );
	}
}
