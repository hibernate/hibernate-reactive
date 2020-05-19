package org.hibernate.reactive.event.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.ActionQueue;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionEventListenerManager;
import org.hibernate.event.spi.AutoFlushEvent;
import org.hibernate.event.spi.AutoFlushEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.reactive.internal.RxSessionInternal;
import org.hibernate.reactive.engine.spi.RxActionQueue;
import org.hibernate.reactive.event.spi.RxAutoFlushEventListener;
import org.hibernate.reactive.util.impl.RxUtil;
import org.hibernate.stat.spi.StatisticsImplementor;

import org.jboss.logging.Logger;

public class DefaultRxAutoFlushEventListener extends AbstractRxFlushingEventListener implements RxAutoFlushEventListener, AutoFlushEventListener {

	private static final CoreMessageLogger LOG = Logger.getMessageLogger( CoreMessageLogger.class, DefaultRxAutoFlushEventListener.class.getName() );

	@Override
	public CompletionStage<Void> rxOnAutoFlush(AutoFlushEvent event) throws HibernateException {
		final EventSource source = event.getSession();
		final SessionEventListenerManager eventListenerManager = source.getEventListenerManager();

		eventListenerManager.partialFlushStart();
		CompletionStage<Void> autoFlushStage = RxUtil.nullFuture();
		if ( flushMightBeNeeded( source ) ) {
			// Need to get the number of collection removals before flushing to executions
			// (because flushing to executions can add collection removal actions to the action queue).
			final ActionQueue actionQueue = source.getActionQueue();
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
							return RxUtil.nullFuture();
						}
					} );
		}
		autoFlushStage.whenComplete( (v, x) -> {
			source.getEventListenerManager().flushEnd( event.getNumberOfEntitiesProcessed(), event.getNumberOfCollectionsProcessed() );
			RxUtil.rethrowIfNotNull( x );
		} );

		return autoFlushStage;
	}

	private boolean flushIsReallyNeeded(AutoFlushEvent event, final EventSource source) {
		return source.getHibernateFlushMode() == FlushMode.ALWAYS
				|| rxActionQueue( source ).areTablesToBeUpdated( event.getQuerySpaces() );
	}

	private RxActionQueue rxActionQueue(EventSource source) {
		return source.unwrap( RxSessionInternal.class ).getRxActionQueue();
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
		throw new UnsupportedOperationException("use rxOnAutoFlush instead");
	}
}
