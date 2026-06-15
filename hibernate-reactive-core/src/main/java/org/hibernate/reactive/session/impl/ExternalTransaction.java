/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.stage.Stage;

/**
 * Represents a transaction that was started externally (e.g., by a framework-level
 * connection pool) rather than through {@link Mutiny.Session#withTransaction} or
 * {@link Stage.Session#withTransaction}. Returned by {@code currentTransaction()}
 * when the underlying connection has an active transaction that was not initiated
 * by this session.
 * <p>
 * The {@link #markForRollback()} flag is informational: the external transaction
 * owner is responsible for the actual commit/rollback lifecycle.
 */
public class ExternalTransaction implements Mutiny.Transaction, Stage.Transaction {

	private final ReactiveConnection connection;
	private final Object transactionId;
	private boolean markedForRollback;

	public ExternalTransaction(ReactiveConnection connection) {
		this.connection = connection;
		this.transactionId = connection.currentTransactionId();
	}

	public boolean isTransactionInProgress() {
		return connection.isTransactionInProgress();
	}

	/**
	 * The opaque transaction identity captured when this wrapper was created.
	 * Used by {@link CurrentTransaction} to detect when the connection has
	 * cycled to a new transaction.
	 *
	 * @see ReactiveConnection#currentTransactionId()
	 */
	Object transactionId() {
		return transactionId;
	}

	@Override
	public void markForRollback() {
		markedForRollback = true;
	}

	@Override
	public boolean isMarkedForRollback() {
		return markedForRollback;
	}
}
