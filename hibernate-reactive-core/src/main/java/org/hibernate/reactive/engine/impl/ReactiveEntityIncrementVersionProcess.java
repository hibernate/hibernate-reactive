/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.reactive.engine.ReactiveBeforeTransactionCompletionProcess;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.util.concurrent.CompletionStage;

/**
 * A BeforeTransactionCompletionProcess impl to verify and increment an entity version as party
 * of before-transaction-completion processing
 *
 * @author Scott Marlow
 * @author Gavin King
 */
public class ReactiveEntityIncrementVersionProcess implements ReactiveBeforeTransactionCompletionProcess {
	private final Object object;

	/**
	 * Constructs an EntityIncrementVersionProcess for the given entity.
	 *
	 * @param object The entity instance
	 */
	public ReactiveEntityIncrementVersionProcess(Object object) {
		this.object = object;
	}

	/**
	 * Perform whatever processing is encapsulated here before completion of the transaction.
	 *
	 * @param session The session on which the transaction is preparing to complete.
	 */
	@Override
	public CompletionStage<Void> doBeforeTransactionCompletion(SessionImplementor session) {
		final EntityEntry entry = session.getPersistenceContext().getEntry( object );
		// Don't increment version for an entity that is not in the PersistenceContext;
		if ( entry == null ) {
			return CompletionStages.voidFuture();
		}

		return ( (ReactiveEntityPersister) entry.getPersister() )
				.lockReactive(
						entry.getId(),
						entry.getVersion(),
						object,
						new LockOptions(LockMode.PESSIMISTIC_FORCE_INCREMENT),
						session
				);
	}
}
