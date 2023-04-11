/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityDeleteAction;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive {@link EntityDeleteAction}.
 */
public class ReactiveEntityDeleteAction extends EntityDeleteAction implements ReactiveExecutable {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveEntityDeleteAction(
			Object id,
			Object[] state,
			Object version,
			Object instance,
			EntityPersister persister,
			boolean isCascadeDeleteEnabled,
			EventSource session) {
		super( id, state, version, instance, persister, isCascadeDeleteEnabled, session );
	}

	public ReactiveEntityDeleteAction(Object id, EntityPersister persister, EventSource session) {
		super( id, persister, session );
	}

	@Override
	public void execute() throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveExecute" );
	}

	private boolean isInstanceLoaded() {
		// A null instance signals that we're deleting an unloaded proxy.
		return getInstance() != null;
	}

	@Override
	public CompletionStage<Void> reactiveExecute() throws HibernateException {
		final Object id = getId();
		final Object version = getCurrentVersion();
		final EntityPersister persister = getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();

		final boolean veto = isInstanceLoaded() && preDelete();

		final Object ck = lockCacheItem();

		final CompletionStage<Void> deleteStep = !isCascadeDeleteEnabled() && !veto
				? ( (ReactiveEntityPersister) persister ).deleteReactive( id, version, instance, session )
				: voidFuture();

		return deleteStep.thenAccept( v -> {
			if ( isInstanceLoaded() ) {
				postDeleteLoaded( id, persister, session, instance, ck );
			}
			else {
				// we're deleting an unloaded proxy
				postDeleteUnloaded( id, persister, session, ck );
			}

			final StatisticsImplementor statistics = getSession().getFactory().getStatistics();
			if ( statistics.isStatisticsEnabled() && !veto ) {
				statistics.deleteEntity( getPersister().getEntityName() );
			}
		} );
	}
}
