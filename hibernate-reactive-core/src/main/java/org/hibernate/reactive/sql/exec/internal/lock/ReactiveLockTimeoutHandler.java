/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.sql.exec.spi.ReactivePostAction;
import org.hibernate.reactive.sql.exec.spi.ReactivePreAction;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.StatementAccess;

import jakarta.persistence.Timeout;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.concurrent.CompletionStage;

/**
 * Reactive version of {@link org.hibernate.sql.exec.internal.LockTimeoutHandler}
 */
public class ReactiveLockTimeoutHandler implements ReactivePreAction, ReactivePostAction {
	public static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );


	private final ReactiveConnectionLockTimeoutStrategy lockTimeoutStrategy;
	private final Timeout timeout;

	private Timeout baseline;
	private boolean setTimeout;

	public ReactiveLockTimeoutHandler(Timeout timeout, ReactiveConnectionLockTimeoutStrategy lockTimeoutStrategy) {
		this.timeout = timeout;
		this.lockTimeoutStrategy = lockTimeoutStrategy;
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
			ExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactivePerformPostAction()" );
	}

	@Override
	public CompletionStage<Void> reactivePerformPreAction(
			ReactiveConnection connection,
			ExecutionContext executionContext) {
		final var factory = executionContext.getSession().getFactory();

		// first, get the baseline (for post-action)
		return lockTimeoutStrategy.getReactiveLockTimeout( connection, factory )
				.thenCompose( baseline -> {
					this.baseline = baseline;
					// now set the timeout
					return lockTimeoutStrategy.setReactiveLockTimeout( timeout, connection, factory );
				} )
				.thenAccept( unused -> setTimeout = true );

	}

	@Override
	public CompletionStage<Void> reactivePerformReactivePostAction(
			ReactiveConnection connection,
			ExecutionContext executionContext) {
		final var factory = executionContext.getSession().getFactory();

		// reset the timeout
		return lockTimeoutStrategy.setReactiveLockTimeout(baseline, connection,factory );
	}



	@Override
	public boolean shouldRunAfterFail() {
		// if we set the timeout in the pre-action, we should always reset it in post-action
		return setTimeout;
	}
}
