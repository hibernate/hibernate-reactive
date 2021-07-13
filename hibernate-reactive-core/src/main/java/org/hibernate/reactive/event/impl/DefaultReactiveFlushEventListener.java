/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.reactive.event.ReactiveFlushEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultFlushEventListener}.
 */
public class DefaultReactiveFlushEventListener extends AbstractReactiveFlushingEventListener
		implements ReactiveFlushEventListener, FlushEventListener {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public CompletionStage<Void> reactiveOnFlush(FlushEvent event) throws HibernateException {
		final EventSource source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		if ( persistenceContext.getNumberOfManagedEntities() > 0 ||
				persistenceContext.getCollectionEntriesSize() > 0 ) {

			source.getEventListenerManager().flushStart();

			return flushEverythingToExecutions(event)
					.thenCompose( v -> performExecutions(source) )
					.thenRun( () -> postFlush( source ) )
					.whenComplete( (v, x) ->
							source.getEventListenerManager().flushEnd(
								event.getNumberOfEntitiesProcessed(),
								event.getNumberOfCollectionsProcessed()
					))
					.thenRun( () -> {
						postPostFlush( source );

						final StatisticsImplementor statistics = source.getFactory().getStatistics();
						if ( statistics.isStatisticsEnabled() ) {
							statistics.flush();
						}
					} );
		}
		return voidFuture();
	}

	@Override
	public void onFlush(FlushEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}
}
