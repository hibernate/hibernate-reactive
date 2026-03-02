/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool;

import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.hibernate.Incubating;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Coordinates transaction management for reactive connections, similar to
 * Hibernate ORM's {@link org.hibernate.resource.transaction.spi.TransactionCoordinator}.
 * <p>
 * This abstraction allows transactions to be managed either by Hibernate Reactive
 * itself (resource-local mode) or by an external framework (managed mode), such as
 * Quarkus managing transactions at a higher level.
 * <p>
 * When transactions are externally managed, Hibernate Reactive delegates the
 * transaction lifecycle management to the external coordinator.
 *
 * @see org.hibernate.reactive.pool.impl.ResourceLocalTransactionCoordinator
 */
@Incubating
public interface ReactiveTransactionCoordinator {

	/**
	 * Transaction status indicating the transaction has been committed.
	 * Matches {@code javax.transaction.Status.STATUS_COMMITTED}.
	 */
	int STATUS_COMMITTED = 3;

	/**
	 * Transaction status indicating the transaction has been rolled back.
	 * Matches {@code javax.transaction.Status.STATUS_ROLLEDBACK}.
	 */
	int STATUS_ROLLEDBACK = 4;

	/**
	 * Indicates whether transactions are being handled externally to Hibernate Reactive.
	 * <p>
	 * The external coordinator is responsible for:
	 * <ul>
	 *     <li>Beginning transactions before providing connections</li>
	 *     <li>Committing or rolling back transactions on session close</li>
	 *     <li>Properly closing physical connections</li>
	 * </ul>
	 *
	 * @return {@code true} if transactions are externally managed, {@code false} otherwise
	 */
	boolean isExternallyManaged();

	/**
	 * Returns the connection associated with the current transaction context.
	 * <p>
	 * When transactions are externally managed, this method provides access to the
	 * connection that was established by the external transaction manager. Sessions
	 * should use this connection instead of obtaining a new one from the pool.
	 * <p>
	 * For resource-local transaction coordinators, this method returns {@code null}
	 * since there is no externally-managed connection.
	 *
	 * @return a {@link CompletionStage} containing the current connection, or {@code null}
	 *         if no externally-managed connection is available
	 */
	default CompletionStage<ReactiveConnection> getConnection() {
		return null;
	}

	/**
	 * Registers an action to be executed before the transaction commits.
	 * Similar to JTA's {@code Synchronization.beforeCompletion()}.
	 * <p>
	 * This is used by sessions to defer their flush until commit time.
	 * These actions are NOT executed on rollback.
	 * <p>
	 * For resource-local transaction coordinators, this is a no-op since the
	 * session manages its own flush timing.
	 *
	 * @param action the action to execute before commit
	 */
	default void registerBeforeCommit(Supplier<CompletionStage<Void>> action) {
		// No-op for resource-local transactions
	}

	/**
	 * Registers an action to be executed after the transaction completes,
	 * whether by commit or rollback. Similar to JTA's {@code Synchronization.afterCompletion(int)},
	 * but reactive-friendly by returning a {@link CompletionStage}.
	 * <p>
	 * This is used for cleanup actions like closing sessions.
	 * <p>
	 * For resource-local transaction coordinators, this is a no-op since the
	 * session manages its own cleanup.
	 *
	 * @param action the action to execute after completion, receiving the transaction status
	 *               ({@link #STATUS_COMMITTED} or {@link #STATUS_ROLLEDBACK}) and returning
	 *               a {@link CompletionStage} that completes when the action is done
	 */
	default void registerAfterCompletion(IntFunction<CompletionStage<Void>> action) {
		// No-op for resource-local transactions
	}

	/**
	 * Executes all registered before-commit actions.
	 * Called by the connection before committing the transaction.
	 *
	 * @return a completion stage that completes when all actions have executed
	 */
	default CompletionStage<Void> beforeCompletion() {
		// No-op for resource-local transactions
		return voidFuture();
	}

	/**
	 * Executes all registered after-completion actions.
	 * Called by the connection after commit or rollback.
	 *
	 * @param status the transaction status ({@link #STATUS_COMMITTED} or {@link #STATUS_ROLLEDBACK})
	 * @return a completion stage that completes when all actions have executed
	 */
	default CompletionStage<Void> afterCompletion(int status) {
		// No-op for resource-local transactions
		return voidFuture();
	}
}
