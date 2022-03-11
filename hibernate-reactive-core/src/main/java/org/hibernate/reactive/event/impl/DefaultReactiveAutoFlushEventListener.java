/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionEventListenerManager;
import org.hibernate.event.spi.AutoFlushEvent;
import org.hibernate.event.spi.AutoFlushEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.event.ReactiveAutoFlushEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class DefaultReactiveAutoFlushEventListener extends AbstractReactiveFlushingEventListener
		implements ReactiveAutoFlushEventListener, AutoFlushEventListener {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public CompletionStage<Void> reactiveOnAutoFlush(AutoFlushEvent event) throws HibernateException {
		final EventSource source = event.getSession();
		final SessionEventListenerManager eventListenerManager = source.getEventListenerManager();

		eventListenerManager.partialFlushStart();
		CompletionStage<Void> autoFlushStage = voidFuture();
		if ( flushMightBeNeeded( source ) ) {
			// Need to get the number of collection removals before flushing to executions
			// (because flushing to executions can add collection removal actions to the action queue).
			final ReactiveActionQueue actionQueue = reactiveActionQueue( event );
			final int oldSize = actionQueue.numberOfCollectionRemovals();

			autoFlushStage = flushEverythingToExecutions( event )
					.thenCompose( v -> {
						if ( flushIsReallyNeeded( event, source ) ) {
							LOG.trace( "Need to execute flush" );
							event.setFlushRequired( true );

							return performExecutions( source )
									.thenRun( () -> postFlush( source ) )
									.thenRun( () -> postPostFlush( source ) )
									.thenRun( () -> {
										final StatisticsImplementor statistics = source.getFactory().getStatistics();
										if ( statistics.isStatisticsEnabled() ) {
											statistics.flush();
										}
									} );
						}
						else {
							LOG.trace( "Don't need to execute flush" );
							event.setFlushRequired( false );
							actionQueue.clearFromFlushNeededCheck( oldSize );
							return voidFuture();
						}
					} );
		}
		return autoFlushStage.whenComplete( (v, x) -> source.getEventListenerManager()
				.partialFlushEnd(
						event.getNumberOfEntitiesProcessed(),
						event.getNumberOfCollectionsProcessed()
				) );
	}

	private ReactiveActionQueue reactiveActionQueue(AutoFlushEvent event) {
		return event.getSession()
				.unwrap( ReactiveSession.class )
				.getReactiveActionQueue();
	}

	private boolean flushIsReallyNeeded(AutoFlushEvent event, final EventSource source) {
		return source.getHibernateFlushMode() == FlushMode.ALWAYS
				|| reactiveActionQueue( source ).areTablesToBeUpdated( event.getQuerySpaces() );
	}

	private ReactiveActionQueue reactiveActionQueue(EventSource source) {
		return source.unwrap( ReactiveSession.class ).getReactiveActionQueue();
	}

	private boolean flushMightBeNeeded(final EventSource source) {
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		return !source.getHibernateFlushMode().lessThan( FlushMode.AUTO )
				&& source.getDontFlushFromFind() == 0
				&& ( persistenceContext.getNumberOfManagedEntities() > 0 ||
				persistenceContext.getCollectionEntriesSize() > 0 );
	}

	@Override
	public void onAutoFlush(AutoFlushEvent event) throws HibernateException {
		throw new UnsupportedOperationException("use reactiveOnAutoFlush instead");
	}
}
