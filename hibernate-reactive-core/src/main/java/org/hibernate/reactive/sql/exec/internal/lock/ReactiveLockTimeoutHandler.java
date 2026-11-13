/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.sql.exec.spi.ReactivePostAction;
import org.hibernate.reactive.sql.exec.spi.ReactivePreAction;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.LoadedValuesCollector;
import org.hibernate.sql.exec.spi.SecondaryAction;
import org.hibernate.sql.exec.spi.StatementAccess;

import jakarta.persistence.Timeout;

import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

/**
 * Reactive version of {@link org.hibernate.sql.exec.internal.LockTimeoutHandler}
 * <p>
 * The same instance is shared by all the executions of a query, for example through the query plan cache:
 * the state of an execution is in the action that {@link #createExecutionAction()} returns.
 */
public class ReactiveLockTimeoutHandler implements ReactivePreAction, ReactivePostAction {
	public static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveConnectionLockTimeoutStrategy lockTimeoutStrategy;
	private final Timeout timeout;

	public ReactiveLockTimeoutHandler(Timeout timeout, ReactiveConnectionLockTimeoutStrategy lockTimeoutStrategy) {
		this.timeout = timeout;
		this.lockTimeoutStrategy = lockTimeoutStrategy;
	}

	@Override
	public SecondaryAction createExecutionAction() {
		return new Execution();
	}

	@Override
	public void performPreAction(
			StatementAccess jdbcStatementAccess,
			Connection jdbcConnection,
			ExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactivePerformPreAction()" );
	}

	@Override
	public void performPostAction(
			StatementAccess jdbcStatementAccess,
			Connection jdbcConnection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector) {
		throw LOG.nonReactiveMethodCall( "reactivePerformPostAction()" );
	}

	@Override
	public CompletionStage<Void> reactivePerformPreAction(
			ReactiveConnection connection,
			ExecutionContext executionContext) {
		throw executionActionRequired();
	}

	@Override
	public CompletionStage<Void> reactivePerformReactivePostAction(
			ReactiveConnection connection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector) {
		throw executionActionRequired();
	}

	@Override
	public boolean shouldRunAfterFail() {
		throw executionActionRequired();
	}

	private static IllegalStateException executionActionRequired() {
		return new IllegalStateException( "Lock timeout definitions require an execution action; use createExecutionAction()" );
	}

	/**
	 * Sets the lock timeout of the connection for one execution, and restores the previous one afterward
	 */
	private class Execution implements ReactivePreAction, ReactivePostAction {
		private Timeout baseline;
		private boolean restorationRequired;

		@Override
		public void performPreAction(
				StatementAccess jdbcStatementAccess,
				Connection jdbcConnection,
				ExecutionContext executionContext) {
			throw LOG.nonReactiveMethodCall( "reactivePerformPreAction()" );
		}

		@Override
		public void performPostAction(
				StatementAccess jdbcStatementAccess,
				Connection jdbcConnection,
				ExecutionContext executionContext,
				LoadedValuesCollector loadedValuesCollector) {
			throw LOG.nonReactiveMethodCall( "reactivePerformPostAction()" );
		}

		@Override
		public CompletionStage<Void> reactivePerformPreAction(
				ReactiveConnection connection,
				ExecutionContext executionContext) {
			final var factory = executionContext.getSession().getFactory();
			return lockTimeoutStrategy.getReactiveLockTimeout( connection, factory )
					.thenCompose( currentTimeout -> {
						baseline = currentTimeout;
						// A failing setter may already have changed the connection setting
						restorationRequired = true;
						return lockTimeoutStrategy.setReactiveLockTimeout( timeout, connection, factory );
					} );
		}

		@Override
		public CompletionStage<Void> reactivePerformReactivePostAction(
				ReactiveConnection connection,
				ExecutionContext executionContext,
				LoadedValuesCollector loadedValuesCollector) {
			if ( restorationRequired ) {
				restorationRequired = false;
				return lockTimeoutStrategy
						.setReactiveLockTimeout( baseline, connection, executionContext.getSession().getFactory() );
			}
			return voidFuture();
		}

		@Override
		public boolean shouldRunAfterFail() {
			return restorationRequired;
		}
	}
}
