/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.LoadedValuesCollector;

import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

/**
 * The pre-actions and post-actions of one execution of a {@link ReactiveJdbcSelect}.
 * <p>
 * Reactive version of {@link org.hibernate.sql.exec.spi.JdbcActionExecution}: the same instance must be used
 * for both phases of an execution, and never for another execution.
 */
public interface ReactiveJdbcActionExecution {

	/**
	 * An execution without actions
	 */
	ReactiveJdbcActionExecution NONE = new ReactiveJdbcActionExecution() {
		@Override
		public CompletionStage<Void> reactivePerformPreActions(ReactiveConnection connection, ExecutionContext executionContext) {
			return voidFuture();
		}

		@Override
		public CompletionStage<Void> reactivePerformPostActions(
				boolean succeeded,
				ReactiveConnection connection,
				ExecutionContext executionContext,
				LoadedValuesCollector loadedValuesCollector) {
			return voidFuture();
		}
	};

	CompletionStage<Void> reactivePerformPreActions(ReactiveConnection connection, ExecutionContext executionContext);

	CompletionStage<Void> reactivePerformPostActions(
			boolean succeeded,
			ReactiveConnection connection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector);

	/**
	 * Runs the post-actions that must run after a failure, for example to restore the lock timeout of the connection.
	 * A failure of the cleanup is added to the original failure, which the caller remains responsible for propagating.
	 *
	 * @param failure the original failure
	 * @param loadedValuesCollector the collector, or {@code null} if the execution failed before creating it
	 */
	default CompletionStage<Void> reactiveCleanupAfterFailure(
			Throwable failure,
			ReactiveConnection connection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector) {
		return reactivePerformPostActions( false, connection, executionContext, loadedValuesCollector )
				.handle( (v, cleanupFailure) -> {
					if ( cleanupFailure != null && cleanupFailure != failure ) {
						failure.addSuppressed( cleanupFailure );
					}
					return null;
				} );
	}
}
