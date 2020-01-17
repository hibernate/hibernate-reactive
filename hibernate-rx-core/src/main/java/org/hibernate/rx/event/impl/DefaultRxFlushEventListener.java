package org.hibernate.rx.event.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.event.internal.DefaultFlushEventListener;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.engine.spi.RxActionQueue;
import org.hibernate.rx.event.spi.RxFlushEventListener;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletionStage;

public class DefaultRxFlushEventListener extends DefaultFlushEventListener implements RxFlushEventListener {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			DefaultRxFlushEventListener.class.getName()
	);

	@Override
	public CompletionStage<Void> rxOnFlush(FlushEvent event) throws HibernateException {
		final EventSource source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		CompletionStage<Void> ret = RxUtil.nullFuture();
		if ( persistenceContext.getNumberOfManagedEntities() > 0 ||
				persistenceContext.getCollectionEntries().size() > 0 ) {

			return ret.thenCompose( v -> {
				source.getEventListenerManager().flushStart();

				flushEverythingToExecutions( event );
				return rxPerformExecutions( source );
			} ).thenRun( () -> postFlush( source ) )
					.whenComplete( (v, x) -> {
						source.getEventListenerManager().flushEnd(
								event.getNumberOfEntitiesProcessed(),
								event.getNumberOfCollectionsProcessed()
						);

					} ).thenRun( () -> {
						postPostFlush( source );

						final StatisticsImplementor statistics = source.getFactory().getStatistics();
						if ( statistics.isStatisticsEnabled() ) {
							statistics.flush();
						}

					} );
		}
		return ret;
	}

	//	@Override
	protected CompletionStage<Void> rxPerformExecutions(EventSource session) {
		LOG.trace( "Executing flush" );

		// IMPL NOTE : here we alter the flushing flag of the persistence context to allow
		//		during-flush callbacks more leniency in regards to initializing proxies and
		//		lazy collections during their processing.
		// For more information, see HHH-2763
		CompletionStage<Void> ret = RxUtil.nullFuture();
		return ret.thenCompose( v -> {
			session.getJdbcCoordinator().flushBeginning();
			session.getPersistenceContext().setFlushing( true );
			// we need to lock the collection caches before executing entity inserts/updates in order to
			// account for bi-directional associations
			actionQueue( session ).prepareActions();
			return actionQueue( session ).executeActions();
		} ).whenComplete( (v, x) -> {
			session.getPersistenceContext().setFlushing( false );
			session.getJdbcCoordinator().flushEnding();
		} );
	}

	private RxActionQueue actionQueue(EventSource session) {
		return session.unwrap( RxHibernateSession.class ).getRxActionQueue();
	}

	public static class EventContextManagingFlushEventListenerDuplicationStrategy implements DuplicationStrategy {

		public static final DuplicationStrategy INSTANCE = new DefaultRxFlushEventListener.EventContextManagingFlushEventListenerDuplicationStrategy();

		private EventContextManagingFlushEventListenerDuplicationStrategy() {
		}

		@Override
		public boolean areMatch(Object listener, Object original) {
			if ( listener instanceof DefaultRxFlushEventListener && original instanceof FlushEventListener ) {
				return true;
			}

			return false;
		}

		@Override
		public Action getAction() {
			return Action.REPLACE_ORIGINAL;
		}
	}
}
