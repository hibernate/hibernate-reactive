package org.hibernate.reactive.event.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.*;
import org.hibernate.event.spi.*;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.reactive.event.spi.ReactiveFlushEventListener;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletionStage;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultFlushEventListener}.
 */
public class DefaultReactiveFlushEventListener extends AbstractReactiveFlushingEventListener implements ReactiveFlushEventListener, FlushEventListener {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			DefaultReactiveFlushEventListener.class.getName()
	);

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
		return CompletionStages.nullFuture();
	}

	@Override
	public void onFlush(FlushEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}
}
