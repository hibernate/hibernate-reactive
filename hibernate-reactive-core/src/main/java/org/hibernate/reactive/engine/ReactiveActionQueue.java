/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.PropertyValueException;
import org.hibernate.action.internal.*;
import org.hibernate.action.spi.AfterTransactionCompletionProcess;
import org.hibernate.action.spi.BeforeTransactionCompletionProcess;
import org.hibernate.action.spi.Executable;
import org.hibernate.cache.CacheException;
import org.hibernate.engine.internal.NonNullableTransientDependencies;
import org.hibernate.engine.spi.*;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.reactive.engine.impl.*;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.type.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive counterpart to {@link ActionQueue}, where DML
 * operations are queued before execution during a flush.
 */
public class ReactiveActionQueue {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( ReactiveActionQueue.class );
	/**
	 * A LinkedHashMap containing providers for all the ExecutableLists, inserted in execution order
	 */
	private static final LinkedHashMap<Class<? extends ReactiveExecutable>, ListProvider<? extends ReactiveExecutable>> EXECUTABLE_LISTS_MAP;

	static {
		EXECUTABLE_LISTS_MAP = new LinkedHashMap<>( 8 );

		EXECUTABLE_LISTS_MAP.put(
				ReactiveOrphanRemovalAction.class,
				new ListProvider<ReactiveOrphanRemovalAction>() {
					ExecutableList<ReactiveOrphanRemovalAction> get(ReactiveActionQueue instance) {
						return instance.orphanRemovals;
					}
					ExecutableList<ReactiveOrphanRemovalAction> init(ReactiveActionQueue instance) {
						// OrphanRemovalAction executables never require sorting.
						return instance.orphanRemovals = new ExecutableList<>( false );
					}
				}
		);
		EXECUTABLE_LISTS_MAP.put(
				ReactiveEntityInsertAction.class,
				new ListProvider<ReactiveEntityInsertAction>() {
					ExecutableList<ReactiveEntityInsertAction> get(ReactiveActionQueue instance) {
						return instance.insertions;
					}

					ExecutableList<ReactiveEntityInsertAction> init(ReactiveActionQueue instance) {
						if ( instance.isOrderInsertsEnabled() ) {
							return instance.insertions = new ExecutableList<>(
									new InsertActionSorter()
							);
						}
						else {
							return instance.insertions = new ExecutableList<>(
									false
							);
						}
					}
				}
		);
		EXECUTABLE_LISTS_MAP.put(
				ReactiveEntityUpdateAction.class,
				new ListProvider<ReactiveEntityUpdateAction>() {
					ExecutableList<ReactiveEntityUpdateAction> get(ReactiveActionQueue instance) {
						return instance.updates;
					}

					ExecutableList<ReactiveEntityUpdateAction> init(ReactiveActionQueue instance) {
						return instance.updates = new ExecutableList<>( false );
					}
				}
		);
//		EXECUTABLE_LISTS_MAP.put(
//				QueuedOperationCollectionAction.class,
//				new ListProvider<QueuedOperationCollectionAction>() {
//					ExecutableList<QueuedOperationCollectionAction> get(ReactiveActionQueue instance) {
//						return instance.collectionQueuedOps;
//					}
//					ExecutableList<QueuedOperationCollectionAction> init(ReactiveActionQueue instance) {
//						return instance.collectionQueuedOps = new ExecutableList<>(
//								instance.isOrderUpdatesEnabled()
//						);
//					}
//				}
//		);
		EXECUTABLE_LISTS_MAP.put(
				ReactiveCollectionRemoveAction.class,
				new ListProvider<ReactiveCollectionRemoveAction>() {
					ExecutableList<ReactiveCollectionRemoveAction> get(ReactiveActionQueue instance) {
						return instance.collectionRemovals;
					}
					ExecutableList<ReactiveCollectionRemoveAction> init(ReactiveActionQueue instance) {
						return instance.collectionRemovals = new ExecutableList<>(
								instance.isOrderUpdatesEnabled()
						);
					}
				}
		);
		EXECUTABLE_LISTS_MAP.put(
				ReactiveCollectionUpdateAction.class,
				new ListProvider<ReactiveCollectionUpdateAction>() {
					ExecutableList<ReactiveCollectionUpdateAction> get(ReactiveActionQueue instance) {
						return instance.collectionUpdates;
					}
					ExecutableList<ReactiveCollectionUpdateAction> init(ReactiveActionQueue instance) {
						return instance.collectionUpdates = new ExecutableList<>(
								instance.isOrderUpdatesEnabled()
						);
					}
				}
		);
		EXECUTABLE_LISTS_MAP.put(
				ReactiveCollectionRecreateAction.class,
				new ListProvider<ReactiveCollectionRecreateAction>() {
					ExecutableList<ReactiveCollectionRecreateAction> get(ReactiveActionQueue instance) {
						return instance.collectionCreations;
					}
					ExecutableList<ReactiveCollectionRecreateAction> init(ReactiveActionQueue instance) {
						return instance.collectionCreations = new ExecutableList<>(
								instance.isOrderUpdatesEnabled()
						);
					}
				}
		);
		EXECUTABLE_LISTS_MAP.put(
				ReactiveEntityDeleteAction.class,
				new ListProvider<ReactiveEntityDeleteAction>() {
					ExecutableList<ReactiveEntityDeleteAction> get(ReactiveActionQueue instance) {
						return instance.deletions;
					}

					ExecutableList<ReactiveEntityDeleteAction> init(ReactiveActionQueue instance) {
						// EntityDeleteAction executables never require sorting.
						return instance.deletions = new ExecutableList<>( false );
					}
				}
		);
	}

	// NOTE: ExecutableList fields must be instantiated via ListProvider#init or #getOrInit
	//       to ensure that they are instantiated consistently.

	private final ReactiveSession session;
	private UnresolvedEntityInsertActions unresolvedInsertions;
	// Object insertions, updates, and deletions have list semantics because
	// they must happen in the right order so as to respect referential
	// integrity
	private ExecutableList<ReactiveEntityInsertAction> insertions;
	private ExecutableList<ReactiveEntityDeleteAction> deletions;
	private ExecutableList<ReactiveEntityUpdateAction> updates;
	// Actually the semantics of the next three are really "Bag"
	// Note that, unlike objects, collection insertions, updates,
	// deletions are not really remembered between flushes. We
	// just re-use the same Lists for convenience.
	private ExecutableList<ReactiveCollectionRecreateAction> collectionCreations;
	private ExecutableList<ReactiveCollectionUpdateAction> collectionUpdates;
	private ExecutableList<QueuedOperationCollectionAction> collectionQueuedOps;
	private ExecutableList<ReactiveCollectionRemoveAction> collectionRemovals;
	// TODO: The removeOrphan concept is a temporary "hack" for HHH-6484.  This should be removed once action/task
	// ordering is improved.
	private ExecutableList<ReactiveOrphanRemovalAction> orphanRemovals;
	private transient boolean isTransactionCoordinatorShared;
	private AfterTransactionCompletionProcessQueue afterTransactionProcesses;
	private BeforeTransactionCompletionProcessQueue beforeTransactionProcesses;

	/**
	 * Constructs an action queue bound to the given session.
	 *
	 * @param session The session "owning" this queue.
	 */
	public ReactiveActionQueue(ReactiveSession session) {
		this.session = session;
		isTransactionCoordinatorShared = false;
	}

	private static String[] convertTimestampSpaces(Serializable[] spaces) {
		return (String[]) spaces;
	}

	private static boolean areTablesToBeUpdated(
			ExecutableList<?> actions,
			@SuppressWarnings("rawtypes") Set tableSpaces) {
		if ( actions == null || actions.isEmpty() ) {
			return false;
		}

		for ( Serializable actionSpace : actions.getQuerySpaces() ) {
			if ( tableSpaces.contains( actionSpace ) ) {
				LOG.debugf( "Changes must be flushed to space: %s", actionSpace );
				return true;
			}
		}

		return false;
	}

	private static boolean areTablesToBeUpdated(
			UnresolvedEntityInsertActions actions,
			@SuppressWarnings("rawtypes") Set tableSpaces) {
		for ( Executable action : actions.getDependentEntityInsertActions() ) {
			for ( Serializable space : action.getPropertySpaces() ) {
				if ( tableSpaces.contains( space ) ) {
					LOG.debugf( "Changes must be flushed to space: %s", space );
					return true;
				}
			}
		}
		return false;
	}

	private static Serializable[] convertTimestampSpaces(Set<Serializable> spaces) {
		return spaces.toArray(new Serializable[0]);
	}

	private static String toString(ExecutableList<?> q) {
		return q == null ? "ExecutableList{size=0}" : q.toString();
	}

//	/**
//	 * Used by the owning session to explicitly control deserialization of the action queue.
//	 *
//	 * @param ois The stream from which to read the action queue
//	 * @param session The session to which the action queue belongs
//	 *
//	 * @return The deserialized action queue
//	 *
//	 * @throws IOException indicates a problem reading from the stream
//	 * @throws ClassNotFoundException Generally means we were unable to locate user classes.
//	 */
//	public static ReactiveActionQueue deserialize(ObjectInputStream ois,
//												  SessionImplementor session, ReactiveSession session)
//			throws IOException, ClassNotFoundException {
//		final boolean traceEnabled = LOG.isTraceEnabled();
//		if ( traceEnabled ) {
//			LOG.trace( "Deserializing action-queue" );
//		}
//		ReactiveActionQueue rtn = new ReactiveActionQueue( session, session );
//
//		rtn.unresolvedInsertions = UnresolvedEntityInsertActions.deserialize( ois, session );
//
//		for ( ListProvider<?> provider : EXECUTABLE_LISTS_MAP.values() ) {
//			ExecutableList<?> l = provider.get( rtn );
//			boolean notNull = ois.readBoolean();
//			if ( notNull ) {
//				if ( l == null ) {
//					l = provider.init( rtn );
//				}
//				l.readExternal( ois );
//
//				if ( traceEnabled ) {
//					LOG.tracev( "Deserialized [{0}] entries", l.size() );
//				}
//				l.afterDeserialize( session );
//			}
//		}
//
//		return rtn;
//	}

	public void clear() {
		for ( ListProvider<?> listProvider : EXECUTABLE_LISTS_MAP.values() ) {
			ExecutableList<?> l = listProvider.get( this );
			if ( l != null ) {
				l.clear();
			}
		}
		if ( unresolvedInsertions != null ) {
			unresolvedInsertions.clear();
		}
	}

	public CompletionStage<Void> addAction(ReactiveEntityRegularInsertAction action) {
		LOG.tracev( "Adding an EntityInsertAction for [{0}] object", action.getEntityName() );
		return addInsertAction( action );
	}

	private CompletionStage<Void> addInsertAction(ReactiveEntityInsertAction insert) {
		CompletionStage<Void> ret = voidFuture();
		if ( insert.isEarlyInsert() ) {
			// For early inserts, must execute inserts before finding non-nullable transient entities.
			// TODO: find out why this is necessary
			LOG.tracev(
					"Executing inserts before finding non-nullable transient entities for early insert: [{0}]",
					insert
			);
			ret = ret.thenCompose( v -> executeInserts() );
		}

		NonNullableTransientDependencies nonNullableTransientDependencies = insert.findNonNullableTransientEntities();
		if ( nonNullableTransientDependencies == null ) {
			LOG.tracev( "Adding insert with no non-nullable, transient entities: [{0}]", insert );
			ret = ret.thenCompose( v -> addResolvedEntityInsertAction( insert ) );
		}
		else {
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev( "Adding insert with non-nullable, transient entities; insert=[{0}], dependencies=[{1}]",
							insert,
							nonNullableTransientDependencies.toLoggableString( insert.getSession() )
				);
			}
			if ( unresolvedInsertions == null ) {
				unresolvedInsertions = new UnresolvedEntityInsertActions();
			}
			unresolvedInsertions.addUnresolvedEntityInsertAction( (AbstractEntityInsertAction) insert, nonNullableTransientDependencies );
		}
		return ret;
	}

	private CompletionStage<Void> addResolvedEntityInsertAction(ReactiveEntityInsertAction insert) {
		CompletionStage<Void> ret;
		if ( insert.isEarlyInsert() ) {
			LOG.trace( "Executing insertions before resolved early-insert" );
			ret = executeInserts();
			LOG.debug( "Executing identity-insert immediately" );
			ret = ret.thenCompose( v -> execute( insert ) );
		}
		else {
			LOG.trace( "Adding resolved non-early insert action." );
			addAction( ReactiveEntityInsertAction.class, insert );
			ret = voidFuture();
		}

		return ret.thenCompose( v -> {
			if ( !insert.isVeto() ) {
				CompletionStage<Void> comp = insert.reactiveMakeEntityManaged();
				if ( unresolvedInsertions == null ) {
					return comp;
				}
				else {
					return comp.thenCompose( vv -> CompletionStages.loop(
							unresolvedInsertions.resolveDependentActions( insert.getInstance(), session.getSharedContract() ),
							resolvedAction -> addResolvedEntityInsertAction( (ReactiveEntityRegularInsertAction) resolvedAction )
					) );
				}
			}
			else {
				throw new EntityActionVetoException(
						"The EntityInsertAction was vetoed.",
						(EntityAction) insert
				);
			}
		} );
	}

	private <T extends ReactiveExecutable> void addAction(Class<T> executableClass, T action) {
		getExecutableList(executableClass).getOrInit( this ).add( action );
	}

	@SuppressWarnings("unchecked")
	private <T extends ReactiveExecutable> ListProvider<T> getExecutableList(Class<T> executableClass) {
		return (ListProvider<T>) EXECUTABLE_LISTS_MAP.get( executableClass );
	}

	/**
	 * Adds an entity (IDENTITY) insert action
	 *
	 * @param action The action representing the entity insertion
	 */
	public CompletionStage<Void> addAction(ReactiveEntityIdentityInsertAction action) {
		LOG.tracev( "Adding an EntityIdentityInsertAction for [{0}] object", action.getEntityName() );
		return addInsertAction( action );
	}

	/**
	 * Adds an entity delete action
	 *
	 * @param action The action representing the entity deletion
	 */
	public void addAction(ReactiveEntityDeleteAction action) {
		addAction( ReactiveEntityDeleteAction.class, action );
	}

	/**
	 * Adds an orphan removal action
	 *
	 * @param action The action representing the orphan removal
	 */
	public void addAction(ReactiveOrphanRemovalAction action) {
		addAction( ReactiveOrphanRemovalAction.class, action );
//		throw new UnsupportedOperationException();
	}

	/**
	 * Adds an entity update action
	 *
	 * @param action The action representing the entity update
	 */
	public void addAction(ReactiveEntityUpdateAction action) {
		addAction( ReactiveEntityUpdateAction.class, action );
	}

	/**
	 * Adds a collection (re)create action
	 *
	 * @param action The action representing the (re)creation of a collection
	 */
	public void addAction(ReactiveCollectionRecreateAction action) {
		addAction( ReactiveCollectionRecreateAction.class, action );
	}

	/**
	 * Adds a collection remove action
	 *
	 * @param action The action representing the removal of a collection
	 */
	public void addAction(ReactiveCollectionRemoveAction action) {
		addAction( ReactiveCollectionRemoveAction.class, action );
	}

	/**
	 * Adds a collection update action
	 *
	 * @param action The action representing the update of a collection
	 */
	public void addAction(ReactiveCollectionUpdateAction action) {
		addAction( ReactiveCollectionUpdateAction.class, action );
	}

	/**
	 * Adds an action relating to a collection queued operation (extra lazy).
	 *
	 * @param action The action representing the queued operation
	 */
	public void addAction(QueuedOperationCollectionAction action) {
//		addAction( QueuedOperationCollectionAction.class, action );
		throw new UnsupportedOperationException();
	}

	/**
	 * Adds an action defining a cleanup relating to a bulk operation (HQL/JPQL or Criteria based update/delete)
	 *
	 * @param action The action representing the queued operation
	 */
	public void addAction(BulkOperationCleanupAction action) {
		registerCleanupActions( action );
	}

	private void registerCleanupActions(Executable executable) {
		if ( executable.getBeforeTransactionCompletionProcess() != null ) {
			beforeTransactionProcesses();
			beforeTransactionProcesses.register( executable.getBeforeTransactionCompletionProcess() );
		}
		if ( session.getFactory().getSessionFactoryOptions().isQueryCacheEnabled() ) {
			invalidateSpaces( convertTimestampSpaces( executable.getPropertySpaces() ) );
		}
		if ( executable.getAfterTransactionCompletionProcess() != null ) {
			afterTransactionProcesses();
			afterTransactionProcesses.register( executable.getAfterTransactionCompletionProcess() );
		}
	}

	/**
	 * Are there unresolved entity insert actions that depend on non-nullable associations with a transient entity?
	 *
	 * @return true, if there are unresolved entity insert actions that depend on non-nullable associations with a
	 * transient entity; false, otherwise
	 */
	public boolean hasUnresolvedEntityInsertActions() {
		return unresolvedInsertions != null && !unresolvedInsertions.isEmpty();
	}

	/**
	 * Throws {@link org.hibernate.PropertyValueException} if there are any unresolved entity insert actions that depend
	 * on non-nullable associations with a transient entity. This method should be called on completion of an operation
	 * (after all cascades are completed) that saves an entity.
	 *
	 * @throws org.hibernate.PropertyValueException if there are any unresolved entity insert actions;
	 * {@link org.hibernate.PropertyValueException#getEntityName()} and
	 * {@link org.hibernate.PropertyValueException#getPropertyName()} will return the entity name and property value for
	 * the first unresolved entity insert action.
	 */
	public void checkNoUnresolvedActionsAfterOperation() throws PropertyValueException {
		if ( unresolvedInsertions != null ) {
			unresolvedInsertions.checkNoUnresolvedActionsAfterOperation();
		}
	}

	public void registerProcess(AfterTransactionCompletionProcess process) {
		afterTransactionProcesses().register( process );
	}

	public void registerProcess(BeforeTransactionCompletionProcess process) {
		beforeTransactionProcesses().register( process );
	}

	public void registerProcess(ReactiveAfterTransactionCompletionProcess process) {
		afterTransactionProcesses().registerReactive( process );
	}

	public void registerProcess(ReactiveBeforeTransactionCompletionProcess process) {
		beforeTransactionProcesses().registerReactive( process );
	}

	private BeforeTransactionCompletionProcessQueue beforeTransactionProcesses() {
		if (beforeTransactionProcesses == null) {
			beforeTransactionProcesses = new BeforeTransactionCompletionProcessQueue( session );
		}
		return beforeTransactionProcesses;
	}

	private AfterTransactionCompletionProcessQueue afterTransactionProcesses() {
		if (afterTransactionProcesses == null) {
			afterTransactionProcesses = new AfterTransactionCompletionProcessQueue( session );
		}
		return afterTransactionProcesses;
	}

	/**
	 * Perform all currently queued entity-insertion actions.
	 *
	 * @throws HibernateException error executing queued insertion actions.
	 */
	public CompletionStage<Void> executeInserts() {
		if ( insertions != null && !insertions.isEmpty() ) {
			return executeActions( insertions );
		}
		return voidFuture();
	}

	/**
	 * Perform all currently queued actions.
	 *
	 * @throws HibernateException error executing queued actions.
	 */
	public CompletionStage<Void> executeActions() {
		if ( hasUnresolvedEntityInsertActions() ) {
			return failedFuture( new IllegalStateException(
					"About to execute actions, but there are unresolved entity insert actions." ) );
		}

		CompletionStage<Void> ret = voidFuture();
		for ( ListProvider<? extends ReactiveExecutable> listProvider : EXECUTABLE_LISTS_MAP.values() ) {
			ExecutableList<? extends ReactiveExecutable> l = listProvider.get( this );
			if ( l != null && !l.isEmpty() ) {
				ret = ret.thenCompose( v -> executeActions( l ) );
			}
		}
		return ret;
	}

	/**
	 * Prepares the internal action queues for execution.
	 *
	 * @throws HibernateException error preparing actions.
	 */
	public void prepareActions() throws HibernateException {
		prepareActions( collectionRemovals );
		prepareActions( collectionUpdates );
		prepareActions( collectionCreations );
		prepareActions( collectionQueuedOps );
	}

	private void prepareActions(ExecutableList<?> queue) throws HibernateException {
		if ( queue == null ) {
			return;
		}
		for ( Executable executable : queue ) {
			executable.beforeExecutions();
		}
	}

	/**
	 * Performs cleanup of any held cache softlocks.
	 *
	 * @param success Was the transaction successful.
	 */
	public CompletionStage<Void> afterTransactionCompletion(boolean success) {
		if ( !isTransactionCoordinatorShared ) {
			// Execute completion actions only in transaction owner (aka parent session).
			if ( afterTransactionProcesses != null ) {
				return afterTransactionProcesses.afterTransactionCompletion( success );
			}
		}
		return voidFuture();
	}

	/**
	 * Execute any registered {@link org.hibernate.action.spi.BeforeTransactionCompletionProcess}
	 */
	public CompletionStage<Void> beforeTransactionCompletion() {
		if ( !isTransactionCoordinatorShared ) {
			// Execute completion actions only in transaction owner (aka parent session).
			if ( beforeTransactionProcesses != null ) {
				return beforeTransactionProcesses.beforeTransactionCompletion();
			}
		}
		return voidFuture();
	}

	/**
	 * Check whether any insertion or deletion actions are currently queued.
	 *
	 * @return {@code true} if insertions or deletions are currently queued; {@code false} otherwise.
	 */
	public boolean areInsertionsOrDeletionsQueued() {
		return ( insertions != null && !insertions.isEmpty() )
				|| hasUnresolvedEntityInsertActions()
				|| ( deletions != null && !deletions.isEmpty() )
				|| ( orphanRemovals != null && !orphanRemovals.isEmpty() );
	}

	/**
	 * Check whether the given tables/query-spaces are to be executed against given the currently queued actions.
	 *
	 * @param tables The table/query-spaces to check.
	 *
	 * @return {@code true} if we contain pending actions against any of the given tables; {@code false} otherwise.
	 */
	public boolean areTablesToBeUpdated(@SuppressWarnings("rawtypes") Set tables) {
		if ( tables.isEmpty() ) {
			return false;
		}
		for ( ListProvider<?> listProvider : EXECUTABLE_LISTS_MAP.values() ) {
			ExecutableList<?> l = listProvider.get( this );
			if ( areTablesToBeUpdated( l, tables ) ) {
				return true;
			}
		}
		if ( unresolvedInsertions == null ) {
			return false;
		}
		return areTablesToBeUpdated( unresolvedInsertions, tables );
	}

	/**
	 * Perform {@link org.hibernate.action.spi.Executable#execute()} on each element of the list
	 *
	 * @param list The list of Executable elements to be performed
	 */
	private <E extends ReactiveExecutable> CompletionStage<Void> executeActions(
			ExecutableList<E> list) throws HibernateException {
		// todo : consider ways to improve the double iteration of Executables here:
		//		1) we explicitly iterate list here to perform Executable#execute()
		//		2) ExecutableList#getQuerySpaces also iterates the Executables to collect query spaces.
		return CompletionStages.loop(
				list,
				e -> e.reactiveExecute()
						.whenComplete( (v2, x1) -> {
							if ( e.getBeforeTransactionCompletionProcess() != null ) {
								beforeTransactionProcesses().register( e.getBeforeTransactionCompletionProcess() );
							}
							if ( e.getAfterTransactionCompletionProcess() != null ) {
								afterTransactionProcesses().register( e.getAfterTransactionCompletionProcess() );
							}
						} )
		)
		.whenComplete( (v, x) -> {
			if ( session.getFactory().getSessionFactoryOptions().isQueryCacheEnabled() ) {
				// Strictly speaking, only a subset of the list may have been processed if a RuntimeException occurs.
				// We still invalidate all spaces. I don't see this as a big deal - after all, RuntimeExceptions are
				// unexpected.
				invalidateSpaces( convertTimestampSpaces( list.getQuerySpaces() ) );
			}
		} )
		.thenRun(list::clear)
		// session.getJdbcCoordinator().executeBatch();
		.thenCompose( v -> session.getReactiveConnection().executeBatch() );
	}

	/**
	 * @param executable The action to execute
	 */
	public <E extends ReactiveExecutable> CompletionStage<Void> execute(E executable) {
		return executable.reactiveExecute()
				.whenComplete( (v, x) -> registerCleanupActions( executable ) );
	}

	/**
	 * This method is now called once per execution of an ExecutableList or once for execution of an Execution.
	 *
	 * @param spaces The spaces to invalidate
	 */
	private void invalidateSpaces(Serializable[] spaces) {
		if ( spaces != null && spaces.length > 0 ) {
			for ( Serializable s : spaces ) {
				afterTransactionProcesses().addSpaceToInvalidate( s );
			}
			// Performance win: If we are processing an ExecutableList, this will only be called once
			session.getFactory().getCache().getTimestampsCache().preInvalidate( spaces, session.getSharedContract() );
		}
	}

	/**
	 * Returns a string representation of the object.
	 *
	 * @return a string representation of the object.
	 */
	@Override
	public String toString() {
		return "ReactiveActionQueue[insertions=" + toString( insertions )
//				+ " updates=" + toString( updates )
//				+ " deletions=" + toString( deletions )
//				+ " orphanRemovals=" + toString( orphanRemovals )
//				+ " collectionCreations=" + toString( collectionCreations )
//				+ " collectionRemovals=" + toString( collectionRemovals )
//				+ " collectionUpdates=" + toString( collectionUpdates )
//				+ " collectionQueuedOps=" + toString( collectionQueuedOps )
				+ " unresolvedInsertDependencies=" + unresolvedInsertions
				+ "]";
	}

	public int numberOfCollectionRemovals() {
		if ( collectionRemovals == null ) {
			return 0;
		}
		return collectionRemovals.size();
	}

	public int numberOfCollectionUpdates() {
		if ( collectionUpdates == null ) {
			return 0;
		}
		return collectionUpdates.size();
	}

	public int numberOfCollectionCreations() {
		if ( collectionCreations == null ) {
			return 0;
		}
		return collectionCreations.size();
	}

	public int numberOfDeletions() {
		int del = deletions == null ? 0 : deletions.size();
		int orph = orphanRemovals == null ? 0 : orphanRemovals.size();
		return del + orph;
	}

	public int numberOfUpdates() {
		if ( updates == null ) {
			return 0;
		}
		return updates.size();
	}

	public int numberOfInsertions() {
		if ( insertions == null ) {
			return 0;
		}
		return insertions.size();
	}

//	public TransactionCompletionProcesses getTransactionCompletionProcesses() {
//		return new TransactionCompletionProcesses( beforeTransactionProcesses(), afterTransactionProcesses() );
//	}
//
//	/**
//	 * Bind transaction completion processes to make them shared between primary and secondary session.
//	 * Transaction completion processes are always executed by transaction owner (primary session),
//	 * but can be registered using secondary session too.
//	 *
//	 * @param processes Transaction completion processes.
//	 * @param isTransactionCoordinatorShared Flag indicating shared transaction context.
//	 */
//	public void setTransactionCompletionProcesses(
//			TransactionCompletionProcesses processes,
//			boolean isTransactionCoordinatorShared) {
//		this.isTransactionCoordinatorShared = isTransactionCoordinatorShared;
//		this.beforeTransactionProcesses = processes.beforeTransactionCompletionProcesses;
//		this.afterTransactionProcesses = processes.afterTransactionCompletionProcesses;
//	}

	public void sortCollectionActions() {
		if ( isOrderUpdatesEnabled() ) {
			// sort the updates by fk
			if ( collectionCreations != null ) {
				collectionCreations.sort();
			}
			if ( collectionUpdates != null ) {
				collectionUpdates.sort();
			}
			if ( collectionQueuedOps != null ) {
				collectionQueuedOps.sort();
			}
			if ( collectionRemovals != null ) {
				collectionRemovals.sort();
			}
		}
	}

	public void sortActions() {
		if ( isOrderUpdatesEnabled() && updates != null ) {
			// sort the updates by pk
			updates.sort();
		}
		if ( isOrderInsertsEnabled() && insertions != null ) {
			insertions.sort();
		}
	}

	private boolean isOrderUpdatesEnabled() {
		return session.getFactory().getSessionFactoryOptions().isOrderUpdatesEnabled();
	}

	private boolean isOrderInsertsEnabled() {
		return session.getFactory().getSessionFactoryOptions().isOrderInsertsEnabled();
	}

	public void clearFromFlushNeededCheck(int previousCollectionRemovalSize) {
		if ( collectionCreations != null ) {
			collectionCreations.clear();
		}
		if ( collectionUpdates != null ) {
			collectionUpdates.clear();
		}
		if ( collectionQueuedOps != null ) {
			collectionQueuedOps.clear();
		}
		if ( updates != null ) {
			updates.clear();
		}
		// collection deletions are a special case since update() can add
		// deletions of collections not loaded by the session.
		if ( collectionRemovals != null && collectionRemovals.size() > previousCollectionRemovalSize ) {
			collectionRemovals.removeLastN( collectionRemovals.size() - previousCollectionRemovalSize );
		}
	}

	@SuppressWarnings("SimplifiableConditionalExpression")
	public boolean hasAfterTransactionActions() {
		return isTransactionCoordinatorShared ?
				false :
				afterTransactionProcesses != null && afterTransactionProcesses.hasActions();
	}

	@SuppressWarnings("SimplifiableConditionalExpression")
	public boolean hasBeforeTransactionActions() {
		return isTransactionCoordinatorShared ?
				false :
				beforeTransactionProcesses != null && beforeTransactionProcesses.hasActions();
	}

	public boolean hasAnyQueuedActions() {
		return ( updates != null && !updates.isEmpty() ) || ( insertions != null && !insertions.isEmpty() ) || hasUnresolvedEntityInsertActions()
				|| ( deletions != null && !deletions.isEmpty() ) || ( collectionUpdates != null && !collectionUpdates.isEmpty() )
				|| ( collectionQueuedOps != null && !collectionQueuedOps.isEmpty() ) || ( collectionRemovals != null && !collectionRemovals
				.isEmpty() )
				|| ( collectionCreations != null && !collectionCreations.isEmpty() );
	}

	public void unScheduleDeletion(EntityEntry entry, Object rescuedEntity) {
		if ( rescuedEntity instanceof HibernateProxy ) {
			LazyInitializer initializer = ( (HibernateProxy) rescuedEntity ).getHibernateLazyInitializer();
			if ( !initializer.isUninitialized() ) {
				rescuedEntity = initializer.getImplementation( session.getSharedContract() );
			}
		}
		if ( deletions != null ) {
			for ( int i = 0; i < deletions.size(); i++ ) {
				EntityDeleteAction action = deletions.get( i );
				if ( action.getInstance() == rescuedEntity ) {
					deletions.remove( i );
					return;
				}
			}
		}
		if ( orphanRemovals != null ) {
			for ( int i = 0; i < orphanRemovals.size(); i++ ) {
				EntityDeleteAction action = orphanRemovals.get( i );
				if ( action.getInstance() == rescuedEntity ) {
					orphanRemovals.remove( i );
					return;
				}
			}
		}
		throw new AssertionFailure( "Unable to perform un-delete for instance " + entry.getEntityName() );
	}

//	/**
//	 * Used by the owning session to explicitly control serialization of the action queue
//	 *
//	 * @param oos The stream to which the action queue should get written
//	 *
//	 * @throws IOException Indicates an error writing to the stream
//	 */
//	public void serialize(ObjectOutputStream oos) throws IOException {
//		LOG.trace( "Serializing action-queue" );
//		if ( unresolvedInsertions == null ) {
//			unresolvedInsertions = new UnresolvedEntityInsertActions();
//		}
//		unresolvedInsertions.serialize( oos );
//
//		for ( ListProvider<?> p : EXECUTABLE_LISTS_MAP.values() ) {
//			ExecutableList<?> l = p.get( this );
//			if ( l == null ) {
//				oos.writeBoolean( false );
//			}
//			else {
//				oos.writeBoolean( true );
//				l.writeExternal( oos );
//			}
//		}
//	}

	private abstract static class AbstractTransactionCompletionProcessQueue<T,U> {
		final ReactiveSession session;

		// Concurrency handling required when transaction completion process is dynamically registered
		// inside event listener (HHH-7478).
		protected Queue<T> processes = new ConcurrentLinkedQueue<>();
		protected Queue<U> reactiveProcesses = new ConcurrentLinkedQueue<>();

		private AbstractTransactionCompletionProcessQueue(ReactiveSession session) {
			this.session = session;
		}

		public void register(T process) {
			if ( process == null ) {
				return;
			}
			processes.add( process );
		}

		public void registerReactive(U process) {
			if ( process == null ) {
				return;
			}
			reactiveProcesses.add( process );
		}

		public boolean hasActions() {
			return !processes.isEmpty() || !reactiveProcesses.isEmpty();
		}
	}

	/**
	 * Encapsulates behavior needed for before transaction processing
	 */
	private static class BeforeTransactionCompletionProcessQueue
			extends AbstractTransactionCompletionProcessQueue<BeforeTransactionCompletionProcess,
														ReactiveBeforeTransactionCompletionProcess> {
		private BeforeTransactionCompletionProcessQueue(ReactiveSession session) {
			super( session );
		}

		public CompletionStage<Void> beforeTransactionCompletion() {
			while ( !processes.isEmpty() ) {
				try {
					processes.poll().doBeforeTransactionCompletion( session.getSharedContract() );
				}
				catch (HibernateException he) {
					throw he;
				}
				catch (Exception e) {
					throw new AssertionFailure( "Unable to perform beforeTransactionCompletion callback", e );
				}
			}
			return CompletionStages.loop(
					reactiveProcesses,
					process -> process.doBeforeTransactionCompletion( session )
			).whenComplete( (v, e) -> reactiveProcesses.clear() );
		}
	}

	/**
	 * Encapsulates behavior needed for after transaction processing
	 */
	private static class AfterTransactionCompletionProcessQueue
			extends AbstractTransactionCompletionProcessQueue<AfterTransactionCompletionProcess,
															ReactiveAfterTransactionCompletionProcess> {
		private final Set<Serializable> querySpacesToInvalidate = new HashSet<>();

		private AfterTransactionCompletionProcessQueue(ReactiveSession session) {
			super( session );
		}

		public void addSpaceToInvalidate(Serializable space) {
			querySpacesToInvalidate.add( space );
		}

		public CompletionStage<Void> afterTransactionCompletion(boolean success) {
			while ( !processes.isEmpty() ) {
				try {
					processes.poll().doAfterTransactionCompletion( success, session.getSharedContract() );
				}
				catch (CacheException ce) {
					LOG.unableToReleaseCacheLock( ce );
					// continue loop
				}
				catch (Exception e) {
					throw new AssertionFailure( "Exception releasing cache locks", e );
				}
			}

			if ( session.getFactory().getSessionFactoryOptions().isQueryCacheEnabled() ) {
				session.getFactory().getCache().getTimestampsCache().invalidate(
						querySpacesToInvalidate.toArray(new Serializable[0]),
						session.getSharedContract()
				);
			}
			querySpacesToInvalidate.clear();

			return CompletionStages.loop(
					reactiveProcesses,
					process -> process.doAfterTransactionCompletion( success, session )
			).whenComplete( (v, e) -> reactiveProcesses.clear() );
		}
	}

//	/**
//	 * Wrapper class allowing to bind the same transaction completion process queues in different sessions.
//	 */
//	public static class TransactionCompletionProcesses {
//		private final BeforeTransactionCompletionProcessQueue beforeTransactionCompletionProcesses;
//		private final AfterTransactionCompletionProcessQueue afterTransactionCompletionProcesses;
//
//		private TransactionCompletionProcesses(
//				BeforeTransactionCompletionProcessQueue beforeTransactionCompletionProcessQueue,
//				AfterTransactionCompletionProcessQueue afterTransactionCompletionProcessQueue) {
//			this.beforeTransactionCompletionProcesses = beforeTransactionCompletionProcessQueue;
//			this.afterTransactionCompletionProcesses = afterTransactionCompletionProcessQueue;
//		}
//	}

	/**
	 * Order the {@link #insertions} queue such that we group inserts against the same entity together (without
	 * violating constraints). The original order is generated by cascade order, which in turn is based on the
	 * directionality of foreign-keys. So even though we will be changing the ordering here, we need to make absolutely
	 * certain that we do not circumvent this FK ordering to the extent of causing constraint violations.
	 * <p>
	 * Sorts the insert actions using more hashes.
	 * </p>
	 * NOTE: this class is not thread-safe.
	 *
	 * @author Jay Erb
	 */
	private static class InsertActionSorter implements ExecutableList.Sorter<ReactiveEntityInsertAction> {
		/**
		 * Singleton access
		 */
		public static final InsertActionSorter INSTANCE = new InsertActionSorter();
		// the map of batch numbers to EntityInsertAction lists
		private Map<BatchIdentifier, List<ReactiveEntityInsertAction>> actionBatches;

		public InsertActionSorter() {
		}

		/**
		 * Sort the insert actions.
		 */
		public void sort(List<ReactiveEntityInsertAction> insertions) {
			// optimize the hash size to eliminate a rehash.
			this.actionBatches = new HashMap<>();

			// the mapping of entity names to their latest batch numbers.
			final List<BatchIdentifier> latestBatches = new ArrayList<>();

			for ( ReactiveEntityInsertAction action : insertions ) {
				BatchIdentifier batchIdentifier = new BatchIdentifier(
						action.getEntityName(),
						action.getSession()
								.getFactory()
								.getMetamodel()
								.entityPersister( action.getEntityName() )
								.getRootEntityName()
				);

				int index = latestBatches.indexOf( batchIdentifier );

				if ( index != -1 ) {
					batchIdentifier = latestBatches.get( index );
				}
				else {
					latestBatches.add( batchIdentifier );
				}
				addParentChildEntityNames( action, batchIdentifier );
				addToBatch( batchIdentifier, action );
			}
			insertions.clear();

			// Examine each entry in the batch list, and build the dependency graph.
			for ( int i = 0; i < latestBatches.size(); i++ ) {
				BatchIdentifier batchIdentifier = latestBatches.get( i );

				for ( int j = i - 1; j >= 0; j-- ) {
					BatchIdentifier prevBatchIdentifier = latestBatches.get( j );
					if ( prevBatchIdentifier.hasAnyParentEntityNames( batchIdentifier ) ) {
						prevBatchIdentifier.parent = batchIdentifier;
					}
					if ( batchIdentifier.hasAnyChildEntityNames( prevBatchIdentifier ) ) {
						prevBatchIdentifier.parent = batchIdentifier;
					}
				}

				for ( int j = i + 1; j < latestBatches.size(); j++ ) {
					BatchIdentifier nextBatchIdentifier = latestBatches.get( j );

					if ( nextBatchIdentifier.hasAnyParentEntityNames( batchIdentifier ) ) {
						nextBatchIdentifier.parent = batchIdentifier;
						nextBatchIdentifier.getParentEntityNames().add( batchIdentifier.getEntityName() );
					}
					if ( batchIdentifier.hasAnyChildEntityNames( nextBatchIdentifier ) ) {
						nextBatchIdentifier.parent = batchIdentifier;
						nextBatchIdentifier.getParentEntityNames().add( batchIdentifier.getEntityName() );
					}
				}
			}

			boolean sorted = false;

			long maxIterations = latestBatches.size() * latestBatches.size();
			long iterations = 0;

			sort:
			do {
				// Examine each entry in the batch list, sorting them based on parent/child association
				// as depicted by the dependency graph.
				iterations++;

				for ( int i = 0; i < latestBatches.size(); i++ ) {
					BatchIdentifier batchIdentifier = latestBatches.get( i );

					// Iterate next batches and make sure that children types are after parents.
					// Since the outer loop looks at each batch entry individually and the prior loop will reorder
					// entries as well, we need to look and verify if the current batch is a child of the next
					// batch or if the current batch is seen as a parent or child of the next batch.
					for ( int j = i + 1; j < latestBatches.size(); j++ ) {
						BatchIdentifier nextBatchIdentifier = latestBatches.get( j );

						if ( batchIdentifier.hasParent( nextBatchIdentifier )
								&& !nextBatchIdentifier.hasParent( batchIdentifier ) ) {
							latestBatches.remove( batchIdentifier );
							latestBatches.add( j, batchIdentifier );

							continue sort;
						}
					}
				}
				sorted = true;
			}
			while ( !sorted && iterations <= maxIterations );

			if ( iterations > maxIterations ) {
				LOG.warn( "The batch containing "
						+ latestBatches.size()
						+ " statements could not be sorted after "
						+ maxIterations
						+ " iterations. "
						+ "This might indicate a circular entity relationship." );
			}

			// Now, rebuild the insertions list. There is a batch for each entry in the name list.
			for ( BatchIdentifier rootIdentifier : latestBatches ) {
				insertions.addAll( actionBatches.get( rootIdentifier ) );
			}
		}

		/**
		 * Add parent and child entity names so that we know how to rearrange dependencies
		 *
		 * @param action The action being sorted
		 * @param batchIdentifier The batch identifier of the entity affected by the action
		 */
		private void addParentChildEntityNames(ReactiveEntityInsertAction action, BatchIdentifier batchIdentifier) {
			Object[] propertyValues = action.getState();
			ClassMetadata classMetadata = action.getPersister().getClassMetadata();
			if ( classMetadata != null ) {
				Type[] propertyTypes = classMetadata.getPropertyTypes();
				Type identifierType = classMetadata.getIdentifierType();

				for ( int i = 0; i < propertyValues.length; i++ ) {
					Object value = propertyValues[i];
					if (value!=null) {
						Type type = propertyTypes[i];
						addParentChildEntityNameByPropertyAndValue( action, batchIdentifier, type, value );
					}
				}

				if ( identifierType.isComponentType() ) {
					CompositeType compositeType = (CompositeType) identifierType;
					Type[] compositeIdentifierTypes = compositeType.getSubtypes();

					for ( Type type : compositeIdentifierTypes ) {
						addParentChildEntityNameByPropertyAndValue( action, batchIdentifier, type, null );
					}
				}
			}
		}

		private void addParentChildEntityNameByPropertyAndValue(
				ReactiveEntityInsertAction action,
				BatchIdentifier batchIdentifier,
				Type type,
				Object value) {
			if ( type.isEntityType() ) {
				final EntityType entityType = (EntityType) type;
				final String entityName = entityType.getName();
				final String rootEntityName = action.getSession().getFactory().getMetamodel()
						.entityPersister( entityName ).getRootEntityName();

				if ( entityType.isOneToOne() &&
						entityType.getForeignKeyDirection() == ForeignKeyDirection.TO_PARENT ) {
					if ( !entityType.isReferenceToPrimaryKey() ) {
						batchIdentifier.getChildEntityNames().add( entityName );
					}
					if ( !rootEntityName.equals( entityName ) ) {
						batchIdentifier.getChildEntityNames().add( rootEntityName );
					}
				}
				else {
					if ( !batchIdentifier.getEntityName().equals( entityName ) ) {
						batchIdentifier.getParentEntityNames().add( entityName );
					}
					if ( value != null ) {
						String valueClass = value.getClass().getName();
						if ( !valueClass.equals( entityName ) ) {
							batchIdentifier.getParentEntityNames().add( valueClass );
						}
					}
					if ( !rootEntityName.equals( entityName ) ) {
						batchIdentifier.getParentEntityNames().add( rootEntityName );
					}
				}
			}
			else if ( type.isCollectionType() ) {
				CollectionType collectionType = (CollectionType) type;
				final SessionFactoryImplementor sessionFactory = ( (SessionImplementor) action.getSession() )
						.getSessionFactory();
				if ( collectionType.getElementType( sessionFactory ).isEntityType() &&
						!sessionFactory.getMetamodel()
								.collectionPersister( collectionType.getRole() )
								.isManyToMany() ) {
					String entityName = collectionType.getAssociatedEntityName( sessionFactory );
					String rootEntityName = action.getSession()
							.getFactory()
							.getMetamodel()
							.entityPersister( entityName )
							.getRootEntityName();
					batchIdentifier.getChildEntityNames().add( entityName );
					if ( !rootEntityName.equals( entityName ) ) {
						batchIdentifier.getChildEntityNames().add( rootEntityName );
					}
				}
			}
			else if ( type.isComponentType() && value != null ) {
				// Support recursive checks of composite type properties for associations and collections.
				CompositeType compositeType = (CompositeType) type;
				final SharedSessionContractImplementor session = action.getSession();
				Object[] componentValues = compositeType.getPropertyValues( value, session );
				for ( int j = 0; j < componentValues.length; ++j ) {
					Type componentValueType = compositeType.getSubtypes()[j];
					Object componentValue = componentValues[j];
					addParentChildEntityNameByPropertyAndValue(
							action,
							batchIdentifier,
							componentValueType,
							componentValue
					);
				}
			}
		}

		private void addToBatch(BatchIdentifier batchIdentifier, ReactiveEntityInsertAction action) {
			List<ReactiveEntityInsertAction> actions = actionBatches.get( batchIdentifier );

			if ( actions == null ) {
				actions = new LinkedList<>();
				actionBatches.put( batchIdentifier, actions );
			}
			actions.add( action );
		}

		private static class BatchIdentifier {

			private final String entityName;
			private final String rootEntityName;

			private Set<String> parentEntityNames = new HashSet<>();

			private Set<String> childEntityNames = new HashSet<>();

			private BatchIdentifier parent;

			BatchIdentifier(String entityName, String rootEntityName) {
				this.entityName = entityName;
				this.rootEntityName = rootEntityName;
			}

			public BatchIdentifier getParent() {
				return parent;
			}

			public void setParent(BatchIdentifier parent) {
				this.parent = parent;
			}

			@Override
			public boolean equals(Object o) {
				if ( this == o ) {
					return true;
				}
				if ( !( o instanceof BatchIdentifier ) ) {
					return false;
				}
				BatchIdentifier that = (BatchIdentifier) o;
				return Objects.equals( entityName, that.entityName );
			}

			@Override
			public int hashCode() {
				return Objects.hash( entityName );
			}

			String getEntityName() {
				return entityName;
			}

			String getRootEntityName() {
				return rootEntityName;
			}

			Set<String> getParentEntityNames() {
				return parentEntityNames;
			}

			Set<String> getChildEntityNames() {
				return childEntityNames;
			}

			boolean hasAnyParentEntityNames(BatchIdentifier batchIdentifier) {
				return parentEntityNames.contains( batchIdentifier.getEntityName() ) ||
						parentEntityNames.contains( batchIdentifier.getRootEntityName() );
			}

			boolean hasAnyChildEntityNames(BatchIdentifier batchIdentifier) {
				return childEntityNames.contains( batchIdentifier.getEntityName() );
			}

			/**
			 * Check if this {@link BatchIdentifier} has a parent or grand parent
			 * matching the given {@link BatchIdentifier reference.
			 *
			 * @param batchIdentifier {@link BatchIdentifier} reference
			 *
			 * @return this {@link BatchIdentifier} has a parent matching the given {@link BatchIdentifier reference
			 */
			boolean hasParent(BatchIdentifier batchIdentifier) {
				return (
						parent == batchIdentifier
								|| ( parentEntityNames.contains( batchIdentifier.getEntityName() ) )
								|| parent != null && parent.hasParent( batchIdentifier, new ArrayList<>() )
				);
			}

			private boolean hasParent(BatchIdentifier batchIdentifier, List<BatchIdentifier> stack) {
				if ( !stack.contains( this ) && parent != null ) {
					stack.add( this );
					return parent.hasParent( batchIdentifier, stack );
				}
				return (
						parent == batchIdentifier
								|| parentEntityNames.contains( batchIdentifier.getEntityName() )
				);
			}
		}

	}

	private abstract static class ListProvider<T extends ReactiveExecutable> {
		abstract ExecutableList<T> get(ReactiveActionQueue instance);

		abstract ExecutableList<T> init(ReactiveActionQueue instance);

		ExecutableList<T> getOrInit(ReactiveActionQueue instance) {
			ExecutableList<T> list = get( instance );
			if ( list == null ) {
				list = init( instance );
			}
			return list;
		}
	}
}
