/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.model;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.sql.model.SelfExecutingUpdateOperation;
import org.hibernate.sql.model.ValuesAnalysis;

/**
 * @see org.hibernate.sql.model.SelfExecutingUpdateOperation
 */
public interface ReactiveSelfExecutingUpdateOperation extends SelfExecutingUpdateOperation {

	CompletionStage<Void> performReactiveMutation(
			JdbcValueBindings jdbcValueBindings,
			ValuesAnalysis valuesAnalysis,
			SharedSessionContractImplementor session);
}
