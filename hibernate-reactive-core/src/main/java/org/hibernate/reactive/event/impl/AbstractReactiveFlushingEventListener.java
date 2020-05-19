package org.hibernate.reactive.event.impl;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.Interceptor;
import org.hibernate.action.internal.CollectionRecreateAction;
import org.hibernate.action.internal.CollectionRemoveAction;
import org.hibernate.action.internal.CollectionUpdateAction;
import org.hibernate.action.internal.QueuedOperationCollectionAction;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.internal.Collections;
import org.hibernate.engine.spi.ActionQueue;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.FlushEntityEvent;
import org.hibernate.event.spi.FlushEntityEventListener;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.spi.ReactiveActionQueue;
import org.hibernate.reactive.impl.ReactiveSessionInternal;
import org.hibernate.reactive.engine.impl.Cascade;
import org.hibernate.reactive.engine.impl.CascadingActions;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.jboss.logging.Logger;

/**
 * Collects commons methods needed during the management of flush events.
 *
 * @see org.hibernate.event.internal.AbstractFlushingEventListener
 */
public abstract class AbstractReactiveFlushingEventListener {

	private static final CoreMessageLogger LOG = Logger.getMessageLogger( CoreMessageLogger.class, AbstractReactiveFlushingEventListener.class.getName() );

	protected CompletionStage<Void> performExecutions(EventSource session) {
		LOG.trace( "Executing flush" );

		// IMPL NOTE : here we alter the flushing flag of the persistence context to allow
		//		during-flush callbacks more leniency in regards to initializing proxies and
		//		lazy collections during their processing.
		// For more information, see HHH-2763
		return CompletionStages.nullFuture()
				.thenCompose(v -> {
					session.getJdbcCoordinator().flushBeginning();
					session.getPersistenceContext().setFlushing( true );
					// we need to lock the collection caches before executing entity inserts/updates in order to
					// account for bi-directional associations
					actionQueue( session ).prepareActions();
					return actionQueue( session ).executeActions();
				} )
				.whenComplete( (v, x) -> {
					session.getPersistenceContext().setFlushing( false );
					session.getJdbcCoordinator().flushEnding();
				} );
	}

	private ReactiveActionQueue actionQueue(EventSource session) {
		return session.unwrap( ReactiveSessionInternal.class ).getReactiveActionQueue();
	}

	/**
	 * Coordinates the processing necessary to get things ready for executions
	 * as db calls by preping the session caches and moving the appropriate
	 * entities and collections to their respective execution queues.
	 *
	 * @param event The flush event.
	 * @throws HibernateException Error flushing caches to execution queues.
	 */
	protected CompletionStage<Void> flushEverythingToExecutions(FlushEvent event) throws HibernateException {

		LOG.trace( "Flushing session" );

		EventSource session = event.getSession();

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		session.getInterceptor().preFlush( persistenceContext.managedEntitiesIterator() );

		CompletionStage<Void> cascades = prepareEntityFlushes(session, persistenceContext);
		// we could move this inside if we wanted to
		// tolerate collection initializations during
		// collection dirty checking:
		prepareCollectionFlushes( persistenceContext );
		// now, any collections that are initialized
		// inside this block do not get updated - they
		// are ignored until the next flush

		return cascades.thenAccept( v -> {
			persistenceContext.setFlushing(true);
			try {
				int entityCount = flushEntities(event, persistenceContext);
				int collectionCount = flushCollections(session, persistenceContext);

				event.setNumberOfEntitiesProcessed(entityCount);
				event.setNumberOfCollectionsProcessed(collectionCount);
			}
			finally {
				persistenceContext.setFlushing(false);
			}
		});

		//some statistics
//		logFlushResults( event );
	}

	/**
	 * process cascade save/update at the start of a flush to discover
	 * any newly referenced entity that must be passed to saveOrUpdate(),
	 * and also apply orphan delete
	 */
	private CompletionStage<Void> prepareEntityFlushes(EventSource session, PersistenceContext persistenceContext) throws HibernateException {

		LOG.debug( "Processing flush-time cascades" );

		CompletionStage<Void> stage = CompletionStages.nullFuture();
		final IdentitySet copiedAlready = new IdentitySet( 10 );
		//safe from concurrent modification because of how concurrentEntries() is implemented on IdentityMap
		for ( Map.Entry<Object, EntityEntry> me : persistenceContext.reentrantSafeEntityEntries() ) {
			EntityEntry entry = me.getValue();
			Status status = entry.getStatus();
			if ( status == Status.MANAGED || status == Status.SAVING || status == Status.READ_ONLY ) {
				stage = stage.thenCompose( v -> cascadeOnFlush( session, entry.getPersister(), me.getKey(), copiedAlready ) );
			}
		}
		return stage;
	}

	/**
	 * Initialize the flags of the CollectionEntry, including the
	 * dirty check.
	 */
	private void prepareCollectionFlushes(PersistenceContext persistenceContext) throws HibernateException {

		// Initialize dirty flags for arrays + collections with composite elements
		// and reset reached, doupdate, etc.

		LOG.debug( "Dirty checking collections" );
		persistenceContext.forEachCollectionEntry( (pc,ce) -> ce.preFlush( pc ), true );
	}

	/**
	 * 1. detect any dirty entities
	 * 2. schedule any entity updates
	 * 3. search out any reachable collections
	 */
	private int flushEntities(final FlushEvent event, final PersistenceContext persistenceContext) throws HibernateException {

		LOG.trace( "Flushing entities and processing referenced collections" );

		final EventSource source = event.getSession();
		final Iterable<FlushEntityEventListener> flushListeners =
				source.getFactory().getServiceRegistry()
						.getService( EventListenerRegistry.class )
						.getEventListenerGroup( EventType.FLUSH_ENTITY )
						.listeners();

		// Among other things, updateReachables() will recursively load all
		// collections that are moving roles. This might cause entities to
		// be loaded.

		// So this needs to be safe from concurrent modification problems.

		final Map.Entry<Object,EntityEntry>[] entityEntries = persistenceContext.reentrantSafeEntityEntries();
		final int count = entityEntries.length;

		for ( Map.Entry<Object,EntityEntry> me : entityEntries ) {

			// Update the status of the object and if necessary, schedule an update

			EntityEntry entry = me.getValue();
			Status status = entry.getStatus();

			if ( status != Status.LOADING && status != Status.GONE ) {
				final FlushEntityEvent entityEvent = new FlushEntityEvent( source, me.getKey(), entry );
				for ( FlushEntityEventListener listener : flushListeners ) {
					listener.onFlushEntity( entityEvent );
				}
			}
		}

		source.getActionQueue().sortActions();

		return count;
	}

	/**
	 * process any unreferenced collections and then inspect all known collections,
	 * scheduling creates/removes/updates
	 */
	private int flushCollections(final EventSource session, final PersistenceContext persistenceContext) throws HibernateException {
		LOG.trace( "Processing unreferenced collections" );

		final int count = persistenceContext.getCollectionEntriesSize();

		persistenceContext.forEachCollectionEntry(
				(persistentCollection, collectionEntry) -> {
					if ( !collectionEntry.isReached() && !collectionEntry.isIgnore() ) {
						Collections.processUnreachableCollection( persistentCollection, session );
					}
				}, true );

		// Schedule updates to collections:

		LOG.trace( "Scheduling collection removes/(re)creates/updates" );

		final ActionQueue actionQueue = session.getActionQueue();
		final Interceptor interceptor = session.getInterceptor();
		persistenceContext.forEachCollectionEntry(
				(coll, ce) -> {
					if ( ce.isDorecreate() ) {
						interceptor.onCollectionRecreate( coll, ce.getCurrentKey() );
						actionQueue.addAction(
								new CollectionRecreateAction(
										coll,
										ce.getCurrentPersister(),
										ce.getCurrentKey(),
										session
								)
						);
					}
					if ( ce.isDoremove() ) {
						interceptor.onCollectionRemove( coll, ce.getLoadedKey() );
						actionQueue.addAction(
								new CollectionRemoveAction(
										coll,
										ce.getLoadedPersister(),
										ce.getLoadedKey(),
										ce.isSnapshotEmpty( coll ),
										session
								)
						);
					}
					if ( ce.isDoupdate() ) {
						interceptor.onCollectionUpdate( coll, ce.getLoadedKey() );
						actionQueue.addAction(
								new CollectionUpdateAction(
										coll,
										ce.getLoadedPersister(),
										ce.getLoadedKey(),
										ce.isSnapshotEmpty( coll ),
										session
								)
						);
					}
					// todo : I'm not sure the !wasInitialized part should really be part of this check
					if ( !coll.wasInitialized() && coll.hasQueuedOperations() ) {
						actionQueue.addAction(
								new QueuedOperationCollectionAction(
										coll,
										ce.getLoadedPersister(),
										ce.getLoadedKey(),
										session
								)
						);
					}
				}, true );

		actionQueue.sortCollectionActions();

		return count;
	}

	private CompletionStage<Void> cascadeOnFlush(
			EventSource session,
			EntityPersister persister,
			Object object,
			IdentitySet copiedAlready)
			throws HibernateException {
		return new Cascade<>(
				CascadingActions.PERSIST_ON_FLUSH,
				CascadePoint.BEFORE_FLUSH,
				persister, object, copiedAlready, session
		).cascade();
	}

	/**
	 * 1. Recreate the collection key to collection map
	 * 2. rebuild the collection entries
	 * 3. call Interceptor.postFlush()
	 */
	protected void postFlush(SessionImplementor session) throws HibernateException {

		LOG.trace( "Post flush" );

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		persistenceContext.clearCollectionsByKey();

		// the database has changed now, so the subselect results need to be invalidated
		// the batch fetching queues should also be cleared - especially the collection batch fetching one
		persistenceContext.getBatchFetchQueue().clear();

		persistenceContext.forEachCollectionEntry(
				(persistentCollection, collectionEntry) -> {
					collectionEntry.postFlush( persistentCollection );
					if ( collectionEntry.getLoadedPersister() == null ) {
						//if the collection is dereferenced, unset its session reference and remove from the session cache
						//iter.remove(); //does not work, since the entrySet is not backed by the set
						persistentCollection.unsetSession( session );
						persistenceContext.removeCollectionEntry( persistentCollection );
					}
					else {
						//otherwise recreate the mapping between the collection and its key
						CollectionKey collectionKey = new CollectionKey(
								collectionEntry.getLoadedPersister(),
								collectionEntry.getLoadedKey()
						);
						persistenceContext.addCollectionByKey( collectionKey, persistentCollection );
					}
				}, true
		);
	}

	protected void postPostFlush(SessionImplementor session) {
		session.getInterceptor().postFlush( session.getPersistenceContextInternal().managedEntitiesIterator() );
	}

}
