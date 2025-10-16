/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.exec.spi.StatementAccess;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.concurrent.CompletionStage;

/**
 * Reactive version of {@link JdbcSelect}
 */
public interface ReactiveJdbcSelect extends JdbcSelect {
	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default void performPreActions(
			StatementAccess jdbcStatementAccess,
			Connection jdbcConnection,
			ExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactivePerformPreActions()" );

	}

	@Override
	default void performPostAction(
			boolean succeeded,
			StatementAccess jdbcStatementAccess,
			Connection jdbcConnection,
			ExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactivePerformPostActions()" );
	}


	CompletionStage<Void> reactivePerformPreActions(ReactiveConnection connection, ExecutionContext executionContext);

	CompletionStage<Void> reactivePerformPostActions(
			boolean succeeded,
			ReactiveConnection connection,
			ExecutionContext executionContext);
}
