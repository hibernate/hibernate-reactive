/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.sql.PreparedStatement;
import org.hibernate.reactive.engine.impl.InternalStage;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

/**
 * @see org.hibernate.sql.exec.spi.JdbcMutationExecutor
 */
public interface ReactiveJdbcMutationExecutor {

	InternalStage<Integer> executeReactive(
			JdbcOperationQueryMutation jdbcMutation,
			JdbcParameterBindings jdbcParameterBindings,
			Function<String, PreparedStatement> statementCreator,
			BiConsumer<Integer, PreparedStatement> expectationCheck,
			ExecutionContext executionContext);
}
