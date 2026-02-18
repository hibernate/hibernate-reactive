/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.action.internal;

import java.util.Set;

import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.sqm.tree.SqmDmlStatement;
import org.hibernate.reactive.session.ReactiveSession;

/**
 * Helper class to schedule bulk operation cleanup actions in the reactive action queue.
 * <p>
 * This delegates to Hibernate ORM's {@link BulkOperationCleanupAction} for the actual
 * cleanup logic, but ensures the action is added to {@link org.hibernate.reactive.engine.ReactiveActionQueue}
 * instead of the standard {@link org.hibernate.engine.spi.ActionQueue}.
 */
public class ReactiveBulkOperationCleanupAction {

	/**
	 * Schedules a bulk operation cleanup action for a DML statement.
	 * This is the reactive equivalent of {@code BulkOperationCleanupAction.schedule()}.
	 *
	 * @param session The session
	 * @param statement The DML statement (UPDATE, DELETE, etc.)
	 */
	public static void schedule(SharedSessionContractImplementor session, SqmDmlStatement<?> statement) {
		final var metamodel = session.getFactory().getMappingMetamodel();
		final var persister = metamodel.getEntityDescriptor( statement.getTarget().getEntityName() );
		schedule( session, new BulkOperationCleanupAction( session, persister ) );
	}

	/**
	 * Schedules a bulk operation cleanup action for a set of affected table names.
	 * This is the reactive equivalent of {@code BulkOperationCleanupAction.schedule()}.
	 *
	 * @param session The session
	 * @param affectedTableNames The set of affected table names
	 */
	public static void schedule(SharedSessionContractImplementor session, Set<String> affectedTableNames) {
		schedule( session, new BulkOperationCleanupAction( session, affectedTableNames ) );
	}

	private static void schedule(SharedSessionContractImplementor session, BulkOperationCleanupAction action) {
		if ( session.isEventSource() ) {
			// Regular session - add action to the reactive action queue
			( (ReactiveSession) session ).getReactiveActionQueue().addAction( action );
		}
		else {
			// Stateless session - execute cleanup immediately
			action.getAfterTransactionCompletionProcess().doAfterTransactionCompletion( true, session );
		}
	}
}
