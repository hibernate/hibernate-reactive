/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.hibernate.Incubating;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveTransactionCoordinator;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Transaction coordinator implementation for externally-managed transactions.
 * <p>
 * This coordinator is used when an external framework (like Quarkus with JTA)
 * manages transaction boundaries. It:
 * <ul>
 *     <li>Tracks registered before-commit and after-completion actions</li>
 *     <li>Executes before-commit actions when {@link #beforeCompletion()} is called</li>
 *     <li>Executes after-completion actions when {@link #afterCompletion(int)} is called</li>
 *     <li>Provides access to the externally-managed connection</li>
 * </ul>
 * <p>
 * The connection supplier should return the connection associated with the
 * current transaction context, typically stored in the Vert.x context.
 *
 * @see ReactiveTransactionCoordinator
 * @see ResourceLocalTransactionCoordinator
 */
@Incubating
public class ExternalTransactionCoordinator implements ReactiveTransactionCoordinator {

	private final Supplier<CompletionStage<ReactiveConnection>> connectionSupplier;
	private final List<Supplier<CompletionStage<Void>>> beforeCommitActions = new ArrayList<>();
	private final List<IntFunction<CompletionStage<Void>>> afterCompletionActions = new ArrayList<>();

	/**
	 * Creates an external transaction coordinator with the given connection supplier.
	 *
	 * @param connectionSupplier supplies the connection associated with the current
	 *                           transaction context, returning {@code null} if no
	 *                           connection is available
	 */
	public ExternalTransactionCoordinator(Supplier<CompletionStage<ReactiveConnection>> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	@Override
	public boolean isExternallyManaged() {
		return true;
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection() {
		return connectionSupplier.get();
	}

	@Override
	public void registerBeforeCommit(Supplier<CompletionStage<Void>> action) {
		beforeCommitActions.add( action );
	}

	@Override
	public void registerAfterCompletion(IntFunction<CompletionStage<Void>> action) {
		afterCompletionActions.add( action );
	}

	@Override
	public CompletionStage<Void> beforeCompletion() {
		if ( beforeCommitActions.isEmpty() ) {
			return voidFuture();
		}
		CompletionStage<Void> result = voidFuture();
		for ( Supplier<CompletionStage<Void>> action : beforeCommitActions ) {
			result = result.thenCompose( v -> action.get() );
		}
		return result.whenComplete( (v, t) -> beforeCommitActions.clear() );
	}

	@Override
	public CompletionStage<Void> afterCompletion(int status) {
		// Clear before-commit actions on rollback
		beforeCommitActions.clear();
		if ( afterCompletionActions.isEmpty() ) {
			return voidFuture();
		}
		CompletionStage<Void> result = voidFuture();
		for ( IntFunction<CompletionStage<Void>> action : afterCompletionActions ) {
			result = result.thenCompose( v -> action.apply( status ) );
		}
		return result.whenComplete( (v, t) -> afterCompletionActions.clear() );
	}
}
