package org.hibernate.rx.impl;

import org.hibernate.*;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.internal.StatefulPersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.*;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionImpl;
import org.hibernate.jpa.QueryHints;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.RxSessionInternal;
import org.hibernate.rx.engine.spi.RxActionQueue;
import org.hibernate.rx.event.spi.*;
import org.hibernate.rx.util.impl.RxUtil;

import javax.persistence.EntityNotFoundException;
import javax.persistence.LockModeType;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An {@link RxSessionInternal} implemented by extension of
 * the {@link SessionImpl} in Hibernate core. Extension was
 * preferred to delegation because there are places where
 * Hibernate core compares the identity of session instances.
 */
public class RxSessionInternalImpl extends SessionImpl implements RxSessionInternal, EventSource {

	private transient RxActionQueue rxActionQueue = new RxActionQueue( this );

	public RxSessionInternalImpl(SessionFactoryImpl delegate, SessionCreationOptions options) {
		super( delegate, options );
	}

	@Override
	public RxActionQueue getRxActionQueue() {
		return rxActionQueue;
	}

	@Override
	public RxSession reactive() {
		return new RxSessionImpl( this );
	}

	@Override
	public Object immediateLoad(String entityName, Serializable id) throws HibernateException {
		throw new LazyInitializationException("reactive sessions do not support transparent lazy fetching"
				+ " - use RxSession.fetch() (entity '" + entityName + "' with id '" + id + "' was not loaded)");
	}

	@Override
	public <T> CompletionStage<Optional<T>> rxFetch(T association) {
		checkOpen();
		if ( association instanceof HibernateProxy ) {
			LazyInitializer initializer = ((HibernateProxy) association).getHibernateLazyInitializer();
			//TODO: is this correct?
			// SessionImpl doesn't use IdentifierLoadAccessImpl for initializing proxies
			return new RxIdentifierLoadAccessImpl<T>( initializer.getEntityName() )
					.fetch( initializer.getIdentifier() )
					.thenApply(Optional::get)
					.thenApply( result -> {
						initializer.setSession( this );
						initializer.setImplementation(result);
						return Optional.ofNullable(result);
					} );
		}
		if ( association instanceof PersistentCollection ) {
			//TODO: handle PersistentCollection (raise InitializeCollectionEvent)
			throw new UnsupportedOperationException("fetch() is not yet implemented for collections");
		}
		return RxUtil.completedFuture( Optional.ofNullable(association) );
	}

	@Override
	public CompletionStage<Void> rxPersist(Object entity) {
		checkOpen();
		return firePersist( new PersistEvent( null, entity, this ) );
	}

	@Override
	public CompletionStage<Void> rxPersist(Object object, Map copiedAlready) {
		checkOpenOrWaitingForAutoClose();
		return firePersist( copiedAlready, new PersistEvent( null, object, this ) );
	}

	// Should be similar to firePersist
	private CompletionStage<Void> firePersist(PersistEvent event) {
		checkTransactionSynchStatus();
		checkNoUnresolvedActionsBeforeOperation();

		return fire(event, EventType.PERSIST, (RxPersistEventListener l) -> l::rxOnPersist)
				.handle( (v, e) -> {
					checkNoUnresolvedActionsAfterOperation();

					if (e instanceof MappingException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
					}
					else if (e instanceof RuntimeException) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e != null) {
						return RxUtil.rethrow(e);
					}
					return v;
				});
	}

	private CompletionStage<Void> firePersist(final Map copiedAlready, final PersistEvent event) {
		pulseTransactionCoordinator();

		return fire(event, copiedAlready, EventType.PERSIST,
				(RxPersistEventListener l) -> l::rxOnPersist)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if (e instanceof MappingException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage() ) );
					}
					else if (e instanceof RuntimeException) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e != null){
						return RxUtil.rethrow(e);
					}
					return v;
				});
	}

	@Override
	public CompletionStage<Void> rxPersistOnFlush(Object entity, Map copiedAlready) {
		checkOpenOrWaitingForAutoClose();
		return firePersistOnFlush( copiedAlready, new PersistEvent( null, entity, this ) );
	}

	private CompletionStage<Void> firePersistOnFlush(Map copiedAlready, PersistEvent event) {
		pulseTransactionCoordinator();

		return fire(event, copiedAlready, EventType.PERSIST_ONFLUSH,
				(RxPersistEventListener l) -> l::rxOnPersist)
				.handle( (v, e) -> { delayedAfterCompletion(); return v; });
	}

	@Override
	public CompletionStage<Void> rxRemove(Object entity) {
		checkOpen();
		return fireRemove( new DeleteEvent( null, entity, this ) );
	}

	@Override
	public CompletionStage<Void> rxRemove(Object object, boolean isCascadeDeleteEnabled, Set transientEntities)
			throws HibernateException {
		checkOpenOrWaitingForAutoClose();
		return fireRemove(
				new DeleteEvent(
						null,
						object,
						isCascadeDeleteEnabled,
						((StatefulPersistenceContext) getPersistenceContextInternal())
								.isRemovingOrphanBeforeUpates(),
						this
				),
				transientEntities
		);
	}

	// Should be similar to fireRemove
	private CompletionStage<Void> fireRemove(DeleteEvent event) {
		pulseTransactionCoordinator();

		return fire(event, EventType.DELETE,
				(RxDeleteEventListener l) -> l::rxOnDelete)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof ObjectDeletedException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if ( e instanceof RuntimeException ) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e != null) {
						return RxUtil.rethrow(e);
					}
					return v;
				});
	}

	private CompletionStage<Void> fireRemove(final DeleteEvent event, final Set transientEntities) {
		pulseTransactionCoordinator();

		return fire(event, transientEntities, EventType.DELETE,
				(RxDeleteEventListener l) -> l::rxOnDelete)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof ObjectDeletedException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if ( e instanceof MappingException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if ( e instanceof RuntimeException ) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e != null) {
						return RxUtil.rethrow(e);
					}
					return v;
				});
	}

	@Override
	public <T> CompletionStage<T> rxMerge(T object) throws HibernateException {
		checkOpen();
		return fireMerge( new MergeEvent( null, object, this ));
	}

	@Override
	public CompletionStage<Void> rxMerge(Object object, Map copiedAlready)
			throws HibernateException {
		checkOpenOrWaitingForAutoClose();
		return fireMerge( copiedAlready, new MergeEvent( null, object, this ) );
	}

	private <T> CompletionStage<T> fireMerge(MergeEvent event) {
		checkTransactionSynchStatus();
		checkNoUnresolvedActionsBeforeOperation();

		return fire(event, EventType.MERGE,
				(RxMergeEventListener l) -> l::rxOnMerge)
				.handle( (v,e) -> {
					checkNoUnresolvedActionsAfterOperation();

					if (e instanceof ObjectDeletedException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if (e instanceof MappingException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if (e instanceof RuntimeException) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e !=null) {
						return RxUtil.rethrow(e);
					}
					return (T) event.getResult();
				});
	}

	private CompletionStage<Void> fireMerge(final Map copiedAlready, final MergeEvent event) {
		pulseTransactionCoordinator();

		return fire(event, copiedAlready, EventType.MERGE,
				(RxMergeEventListener l) -> l::rxOnMerge)
				.handle( (v,e) -> {
					delayedAfterCompletion();

					if (e instanceof ObjectDeletedException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e ) );
					}
					else if (e instanceof MappingException) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					else if (e instanceof RuntimeException) {
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e !=null) {
						return RxUtil.rethrow(e);
					}
					return v;
				});

	}

	@Override
	public CompletionStage<Void> rxFlush() {
		checkOpen();
		return doFlush();
	}

	private CompletionStage<Void> doFlush() {
		checkTransactionNeededForUpdateOperation( "no transaction is in progress" );
		pulseTransactionCoordinator();

		if ( getPersistenceContextInternal().getCascadeLevel() > 0 ) {
			throw new HibernateException( "Flush during cascade is dangerous" );
		}

		return fire(new FlushEvent( this ), EventType.FLUSH,
				(RxFlushEventListener l) -> l::rxOnFlush)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e != null) {
						return RxUtil.rethrow( e );
					}
					return v;
				} );
	}

	@Override
	public CompletionStage<Void> rxRefresh(Object entity) {
		checkOpen();
		return fireRefresh( new RefreshEvent(null, entity, this) );
	}

	@Override
	public CompletionStage<Void> rxRefresh(Object object, Map refreshedAlready) {
		checkOpenOrWaitingForAutoClose();
		return fireRefresh( refreshedAlready, new RefreshEvent( null, object, this ) );
	}

	CompletionStage<Void> fireRefresh(RefreshEvent event) {
		if ( !getSessionFactory().getSessionFactoryOptions().isAllowRefreshDetachedEntity() ) {
			if ( event.getEntityName() != null ) {
				if ( !contains( event.getEntityName(), event.getObject() ) ) {
					throw new IllegalArgumentException( "Entity not managed" );
				}
			}
			else {
				if ( !contains( event.getObject() ) ) {
					throw new IllegalArgumentException( "Entity not managed" );
				}
			}
		}
		pulseTransactionCoordinator();

		return fire(event, EventType.REFRESH,
				(RxRefreshEventListener l) -> l::rxOnRefresh)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if (e instanceof RuntimeException) {
						if ( !getSessionFactory().getSessionFactoryOptions().isJpaBootstrap() ) {
							if ( e instanceof HibernateException ) {
								return RxUtil.rethrow(e);
							}
						}
						//including HibernateException
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e != null) {
						return RxUtil.rethrow(e);
					}
					return v;
				});
	}

	private CompletionStage<Void> fireRefresh(final Map refreshedAlready, final RefreshEvent event) {
		pulseTransactionCoordinator();

		return fire(event, refreshedAlready, EventType.REFRESH,
				(RxRefreshEventListener l) -> l::rxOnRefresh)
				.handle( (v, e) -> {
					delayedAfterCompletion();

					if (e instanceof RuntimeException) {
						throw getExceptionConverter().convert( (RuntimeException) e );
					}
					else if (e != null) {
						return RxUtil.rethrow(e);
					}
					return v;
				});
	}

	@Override
	public <T> CompletionStage<Optional<T>> rxGet(
			Class<T> entityClass,
			Serializable id) {
		return new RxIdentifierLoadAccessImpl<>( entityClass ).load( id );
	}

	@Override
	public <T> CompletionStage<Optional<T>> rxFind(
			Class<T> entityClass,
			Object primaryKey,
			LockModeType lockModeType,
			Map<String, Object> properties) {
		checkOpen();

		getLoadQueryInfluencers().getEffectiveEntityGraph().applyConfiguredGraph( properties );
		Boolean readOnly = properties == null ? null : (Boolean) properties.get( QueryHints.HINT_READONLY );
		getLoadQueryInfluencers().setReadOnly( readOnly );

		final RxIdentifierLoadAccessImpl<T> loadAccess = new RxIdentifierLoadAccessImpl<T>(entityClass);
		loadAccess.with( determineAppropriateLocalCacheMode( properties ) );

		LockOptions lockOptions;
		if ( lockModeType != null ) {
			if ( !LockModeType.NONE.equals( lockModeType) ) {
//					checkTransactionNeededForUpdateOperation();
			}
			lockOptions = buildLockOptions( lockModeType, properties );
			loadAccess.with( lockOptions );
		}
		else {
			lockOptions = null;
		}

		return loadAccess.load( (Serializable) primaryKey )
				.handle( (result, e) -> {
					if ( e instanceof EntityNotFoundException) {
						// DefaultLoadEventListener.returnNarrowedProxy may throw ENFE (see HHH-7861 for details),
						// which find() should not throw. Find() should return null if the entity was not found.
						//			if ( log.isDebugEnabled() ) {
						//				String entityName = entityClass != null ? entityClass.getName(): null;
						//				String identifierValue = primaryKey != null ? primaryKey.toString() : null ;
						//				log.ignoringEntityNotFound( entityName, identifierValue );
						//			}
						return Optional.<T>empty();
					}
					if ( e instanceof ObjectDeletedException) {
						//the spec is silent about people doing remove() find() on the same PC
						return Optional.<T>empty();
					}
					if ( e instanceof ObjectNotFoundException) {
						//should not happen on the entity itself with get
						throw new IllegalArgumentException( e.getMessage(), e );
					}
					if ( e instanceof MappingException
							|| e instanceof TypeMismatchException
							|| e instanceof ClassCastException ) {
						throw getExceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
					}
					if ( e instanceof JDBCException ) {
//						if ( accessTransaction().getRollbackOnly() ) {
//							// assume this is the similar to the WildFly / IronJacamar "feature" described under HHH-12472
//							return Optional.<T>empty();
//						}
						throw getExceptionConverter().convert( (JDBCException) e, lockOptions );
					}
					if ( e instanceof RuntimeException ) {
						throw getExceptionConverter().convert( (RuntimeException) e, lockOptions );
					}

					return result;
				} )
				.whenComplete( (v, e) -> getLoadQueryInfluencers().getEffectiveEntityGraph().clear() );
	}

	@SuppressWarnings("unchecked")
	private <E,L,RL> CompletionStage<Void> fire(E event, EventType<L> eventType,
												Function<RL,Function<E,CompletionStage<Void>>> fun) {
		CompletionStage<Void> ret = RxUtil.nullFuture();
		for ( L listener : eventListeners(eventType) ) {
			//to preserve atomicity of the RxSession methods
			//call apply() from within the arg of thenCompose()
			ret = ret.thenCompose( v -> fun.apply((RL) listener).apply(event) );
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	private <E,L,RL,P> CompletionStage<Void> fire(E event, P extra, EventType<L> eventType,
												  Function<RL, BiFunction<E, P, CompletionStage<Void>>> fun) {
		CompletionStage<Void> ret = RxUtil.nullFuture();
		for ( L listener : eventListeners(eventType) ) {
			//to preserve atomicity of the RxSession methods
			//call apply() from within the arg of thenCompose()
			ret = ret.thenCompose( v -> fun.apply((RL) listener).apply(event, extra) );
		}
		return ret;
	}

	@SuppressWarnings("deprecation")
	private <T> Iterable<T> eventListeners(EventType<T> type) {
		return getFactory().unwrap( SessionFactoryImplementor.class )
				.getServiceRegistry().getService( EventListenerRegistry.class )
				.getEventListenerGroup( type )
				.listeners();
	}

	private CompletionStage<Void> fireLoad(LoadEvent event, LoadEventListener.LoadType loadType) {
		checkOpenOrWaitingForAutoClose();

		return fireLoadNoChecks( event, loadType )
				.handle( (v, e) -> {
					delayedAfterCompletion();
					return v;
				} );
	}

	private CompletionStage<Void> fireLoadNoChecks(LoadEvent event, LoadEventListener.LoadType loadType) {
		pulseTransactionCoordinator();

		return fire(event, loadType, EventType.LOAD, (RxLoadEventListener l) -> l::rxOnLoad);
	}

	@Override
	protected void delayedAfterCompletion() {
		//disable for now, but figure out what to do here
	}

	public void afterOperation(boolean success) {
		//disable for now, but figure out what to do here
	}

	@Override
	public void checkTransactionNeededForUpdateOperation(String exceptionMessage) {
		//no-op because we don't support transactions
	}

	private class RxIdentifierLoadAccessImpl<T> {

		private final EntityPersister entityPersister;

		private LockOptions lockOptions;
		private CacheMode cacheMode;

		//Note that entity graphs aren't supported at all
		//because we're not using the EntityLoader from
		//the plan package, so this stuff is useless
		private RootGraphImplementor<T> rootGraph;
		private GraphSemantic graphSemantic;

		public RxIdentifierLoadAccessImpl(EntityPersister entityPersister) {
			this.entityPersister = entityPersister;
		}

		public RxIdentifierLoadAccessImpl(String entityName) {
			this( getFactory().getMetamodel().locateEntityPersister( entityName ) );
		}

		public RxIdentifierLoadAccessImpl(Class<T> entityClass) {
			this( getFactory().getMetamodel().locateEntityPersister( entityClass ) );
		}

		public final RxIdentifierLoadAccessImpl<T> with(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		public RxIdentifierLoadAccessImpl<T> with(CacheMode cacheMode) {
			this.cacheMode = cacheMode;
			return this;
		}

		public RxIdentifierLoadAccessImpl<T> with(RootGraph<T> graph, GraphSemantic semantic) {
			rootGraph = (RootGraphImplementor<T>) graph;
			graphSemantic = semantic;
			return this;
		}

		public final CompletionStage<Optional<T>> getReference(Serializable id) {
			return perform( () -> doGetReference( id ) );
		}

		protected CompletionStage<Optional<T>> perform(Supplier<CompletionStage<Optional<T>>> executor) {
			if ( graphSemantic != null ) {
				if ( rootGraph == null ) {
					throw new IllegalArgumentException( "Graph semantic specified, but no RootGraph was supplied" );
				}
			}
			CacheMode sessionCacheMode = getCacheMode();
			boolean cacheModeChanged = false;
			if ( cacheMode != null ) {
				// naive check for now...
				// todo : account for "conceptually equal"
				if ( cacheMode != sessionCacheMode ) {
					setCacheMode( cacheMode );
					cacheModeChanged = true;
				}
			}

			if ( graphSemantic != null ) {
				getLoadQueryInfluencers().getEffectiveEntityGraph().applyGraph( rootGraph, graphSemantic );
			}

			boolean finalCacheModeChanged = cacheModeChanged;
			return executor.get()
					.whenComplete( (v, x) -> {
						if ( graphSemantic != null ) {
							getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
						}
						if ( finalCacheModeChanged ) {
							// change it back
							setCacheMode( sessionCacheMode );
						}
					} );
		}

		protected CompletionStage<Optional<T>> doGetReference(Serializable id) {
			if ( lockOptions != null ) {
				LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), lockOptions, RxSessionInternalImpl.this, getReadOnlyFromLoadQueryInfluencers());
				return fireLoad( event, LoadEventListener.LOAD ).thenApply( v -> (Optional<T>) event.getResult() );
			}

			LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), false, RxSessionInternalImpl.this, getReadOnlyFromLoadQueryInfluencers());
			return fireLoad( event, LoadEventListener.LOAD )
					.thenApply( v -> {
						if ( event.getResult() == null ) {
							getFactory().getEntityNotFoundDelegate().handleEntityNotFound(
									entityPersister.getEntityName(),
									id
							);
						}
						return (Optional<T>) event.getResult();
					} ).whenComplete( (v, x) -> afterOperation( x != null ) );
		}

		public final CompletionStage<Optional<T>> load(Serializable id) {
			return perform( () -> doLoad( id, LoadEventListener.GET) );
		}

		public final CompletionStage<Optional<T>> fetch(Serializable id) {
			return perform( () -> doLoad( id, LoadEventListener.IMMEDIATE_LOAD) );
		}

		private Boolean getReadOnlyFromLoadQueryInfluencers() {
			return getLoadQueryInfluencers().getReadOnly();
		}

		protected final CompletionStage<Optional<T>> doLoad(Serializable id, LoadEventListener.LoadType loadType) {
			if ( lockOptions != null ) {
				LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), lockOptions, RxSessionInternalImpl.this, getReadOnlyFromLoadQueryInfluencers());
				return fireLoad( event, loadType ).thenApply( v -> (Optional<T>) event.getResult() );
			}

			LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), false, RxSessionInternalImpl.this, getReadOnlyFromLoadQueryInfluencers());
			return fireLoad( event, loadType )
					.handle( (v, t) -> {
						afterOperation( t != null );
						if ( t != null
								// if session cache contains proxy for non-existing object
								&& !( t instanceof ObjectNotFoundException ) ) {
							RxUtil.rethrow( t );
						}
						return (Optional<T>) event.getResult();
					} );
		}
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		if ( RxSessionInternal.class.isAssignableFrom( clazz ) ) {
			return clazz.cast(this);
		}
		if ( RxSession.class.isAssignableFrom( clazz ) ) {
			return clazz.cast( reactive() );
		}
		return super.unwrap( clazz );
	}
}

