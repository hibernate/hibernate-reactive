/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * When a CompletableFuture needs to be executed asynchronously
 * via e.g. CompletableFuture.supplyAsync or .thenApplyAsync(task, Executor)
 * one needs to be careful that subsequent stages aren't guaranteed to be run
 * on the same executor, as execution is scheduled immediately, and
 * there's a change for the task to be completed before the chain
 * of downstream events has been enqueued.
 * The simplest solution for this is to ensure the chain is fully defined
 * before passing it to the Executor; sometimes this requires substantial
 * refactoring, in which case this helper class can be useful:
 * this Executor implementation delegates to an underlying Executor, but
 * holds the single task until it's ready to be fired via {@link #runHeldTasks()}.
 *
 * This instance is to be used once, essentially to decorate a single use of
 * asynchronous scheduling of a CompletableFuture, and is not threadsafe.
 */
public final class OneOffDelegatingExecutor implements Executor {
	private final Executor delegateExecutor;
	private Runnable deferredTask;

	OneOffDelegatingExecutor(Executor delegate) {
		Objects.requireNonNull(delegate);
		this.delegateExecutor = delegate;
	}

	@Override
	public void execute(Runnable runnable) {
		Objects.requireNonNull(runnable);
		this.deferredTask = runnable;
	}

	public void runHeldTasks() {
		if ( this.deferredTask != null )
			this.delegateExecutor.execute( this.deferredTask );
		else {
			throw new IllegalStateException( "No task has been scheduled yet" );
		}
	}

}
