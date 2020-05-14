package org.hibernate.rx.event.impl;

import org.hibernate.HibernateException;
import org.hibernate.Interceptor;
import org.hibernate.action.internal.CollectionRecreateAction;
import org.hibernate.action.internal.CollectionRemoveAction;
import org.hibernate.action.internal.CollectionUpdateAction;
import org.hibernate.action.internal.QueuedOperationCollectionAction;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.internal.Collections;
import org.hibernate.engine.spi.*;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.*;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.RxSessionInternal;
import org.hibernate.rx.engine.impl.Cascade;
import org.hibernate.rx.engine.impl.CascadingActions;
import org.hibernate.rx.engine.spi.RxActionQueue;
import org.hibernate.rx.event.spi.RxFlushEventListener;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultFlushEventListener}.
 */
public class DefaultRxFlushEventListener extends AbstractRxFlushingEventListener implements RxFlushEventListener, FlushEventListener {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			DefaultRxFlushEventListener.class.getName()
	);

	@Override
	public CompletionStage<Void> rxOnFlush(FlushEvent event) throws HibernateException {
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
		return RxUtil.nullFuture();
	}

	@Override
	public void onFlush(FlushEvent event) throws HibernateException {
		throw new UnsupportedOperationException();
	}
}
