/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.internal;

import org.hibernate.reactive.pool.ReactiveConnection;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Holds the current transaction state for a session, tracking both internally-opened
 * transactions (via {@code withTransaction()}) and externally-opened transactions
 * (started on the underlying connection by a framework-level pool).
 *
 * @param <T> the transaction interface type ({@link org.hibernate.reactive.mutiny.Mutiny.Transaction}
 *            or {@link org.hibernate.reactive.stage.Stage.Transaction})
 */
public class CurrentTransaction<T> {

	private final ReactiveConnection connection;
	private T internalTransaction;

	/**
	 * Cached {@link ExternalTransaction} wrapper. The wrapper captures
	 * the opaque transaction identity at creation time (see
	 * {@link ExternalTransaction#transactionId()}), so repeated calls
	 * return the same instance within a single external transaction
	 * while still detecting when the connection has cycled to a new
	 * transaction and creating a fresh wrapper.
	 *
	 * @see ReactiveConnection#currentTransactionId()
	 */
	private ExternalTransaction externalTransaction;

	public CurrentTransaction(ReactiveConnection connection) {
		this.connection = connection;
	}

	/**
	 * Returns the active transaction, if any. Checks the internally-opened
	 * transaction first, then falls back to detecting an externally-opened
	 * transaction on the underlying connection.
	 * <p>
	 * The {@link ExternalTransaction} instance is cached so that repeated
	 * calls return the same object while the external transaction is active.
	 * If the underlying transaction identity has changed (the connection
	 * committed or rolled back and started a new transaction), a fresh
	 * wrapper is created to avoid carrying stale state such as
	 * {@code markedForRollback} from the previous transaction.
	 *
	 * @see ReactiveConnection#currentTransactionId()
	 */
	@SuppressWarnings("unchecked")
	public T get() {
		if ( internalTransaction != null ) {
			return internalTransaction;
		}
		if ( connection.isTransactionInProgress() ) {
			// Compare the current transaction identity with the one cached
			// in the wrapper. If they differ, the connection has moved to a
			// new transaction and we must create a fresh wrapper to avoid
			// carrying stale markedForRollback state.
			if ( externalTransaction == null
					|| connection.currentTransactionId() != externalTransaction.transactionId() ) {
				externalTransaction = new ExternalTransaction( connection );
			}
			return (T) externalTransaction;
		}
		// No transaction in progress — clear the cache
		externalTransaction = null;
		return null;
	}

	/**
	 * If a transaction is already active (internal or external), joins it by
	 * applying the work directly. Otherwise, creates a new internal transaction
	 * via the factory and delegates to its {@link ExecutableTransaction#execute}
	 * method, passing a cleanup callback to invoke when the transaction completes.
	 *
	 * @param work    the transactional work; applied directly when joining
	 * @param factory creates the new {@link ExecutableTransaction}
	 */
	@SuppressWarnings("unchecked")
	public <R> R execute(
			Function<T, R> work,
			Supplier<? extends ExecutableTransaction<T, R>> factory) {
		T existing = get();
		if ( existing != null ) {
			return work.apply( existing );
		}
		ExecutableTransaction<T, R> tx = factory.get();
		internalTransaction = (T) tx;
		return tx.execute( work, this::clear );
	}

	private void clear() {
		internalTransaction = null;
	}
}
