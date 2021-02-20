/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.dialect.lock.OptimisticEntityLockException;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.reactive.engine.ReactiveBeforeTransactionCompletionProcess;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.session.ReactiveSession;

import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.infoString;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A BeforeTransactionCompletionProcess impl to verify an entity version as part of
 * before-transaction-completion processing
 *
 * @author Scott Marlow
 * @author Gavin King
 */
public class ReactiveEntityVerifyVersionProcess implements ReactiveBeforeTransactionCompletionProcess {
	private final Object object;

	/**
	 * Constructs an EntityVerifyVersionProcess
	 *
	 * @param object The entity instance
	 */
	public ReactiveEntityVerifyVersionProcess(Object object) {
		this.object = object;
	}

	@Override
	public CompletionStage<Void> doBeforeTransactionCompletion(ReactiveSession session) {
		final EntityEntry entry = session.getPersistenceContext().getEntry( object );
		// Don't check version for an entity that is not in the PersistenceContext;
		if ( entry == null ) {
			return voidFuture();
		}

		return ( (ReactiveEntityPersister) entry.getPersister() )
				.reactiveGetCurrentVersion( entry.getId(), session.getSharedContract() )
				.thenAccept( latestVersion -> {
					if ( !entry.getVersion().equals( latestVersion ) ) {
						throw new OptimisticEntityLockException(
								object,
								"Newer version ["
										+ latestVersion
										+ "] of entity ["
										+ infoString( entry.getEntityName(), entry.getId() )
										+ "] found in database"
						);
					}
				} );
	}
}
