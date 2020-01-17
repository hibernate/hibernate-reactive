package org.hibernate.rx.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import javax.persistence.EntityNotFoundException;
import javax.persistence.LockModeType;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.JDBCException;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.ObjectDeletedException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.TypeMismatchException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.proxy.LazyInitializer;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;
import org.hibernate.rx.event.spi.RxDeleteEventListener;
import org.hibernate.rx.event.spi.RxFlushEventListener;
import org.hibernate.rx.event.spi.RxLoadEventListener;
import org.hibernate.rx.event.spi.RxPersistEventListener;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.service.ServiceRegistry;

public class RxSessionImpl implements RxSession {

	private final RxHibernateSessionFactory factory;
	private final RxHibernateSession rxHibernateSession;

	public RxSessionImpl(RxHibernateSessionFactoryImplementor factory, RxHibernateSession session) {
		this.factory = factory;
		this.rxHibernateSession = session;
	}

	@Override
	public CompletionStage<RxSession> flush() {
//		checkOpen();
		return doFlush().thenApply( v-> this );
	}

	@Override
	public <T> CompletionStage<T> fetch(T association) {
		if ( association instanceof HibernateProxy ) {
			LazyInitializer initializer = ((HibernateProxy) association).getHibernateLazyInitializer();
			//TODO: is this correct?
			// SessionImpl doesn't use IdentifierLoadAccessImpl for initializing proxies
			return new RxIdentifierLoadAccessImpl<T>( initializer.getEntityName() )
					.fetch( initializer.getIdentifier() )
					.thenApply(Optional::get)
					.thenApply( result -> {
						initializer.setSession( rxHibernateSession.delegate() );
						return result;
					} );
		}
		if ( association instanceof PersistentCollection ) {
			//TODO: handle PersistentCollection (raise InitializeCollectionEvent)
			throw new UnsupportedOperationException("fetch() is not yet implemented for collections");
		}
		return RxUtil.completedFuture(association);
	}

	private CompletionStage<Void> doFlush() {
//		checkTransactionNeeded();
//		checkTransactionSynchStatus();

//			if ( persistenceContext.getCascadeLevel() > 0 ) {
//				throw new HibernateException( "Flush during cascade is dangerous" );
//			}

		CompletionStage<Void> ret = RxUtil.nullFuture();
		FlushEvent flushEvent = new FlushEvent( (EventSource) rxHibernateSession );
		for ( FlushEventListener listener : listeners( EventType.FLUSH ) ) {
			ret = ret.thenCompose( v -> ( (RxFlushEventListener) listener ).rxOnFlush( flushEvent ) );
		}

//			delayedAfterCompletion();
		return ret.exceptionally( x -> {
			if ( x instanceof RuntimeException ) {
				throw exceptionConverter().convert( (RuntimeException) x );
			}
			else {
				return RxUtil.rethrow( x );
			}
		} );
	}

	@Override
	public <T> T getReference(Class<T> entityClass, Object id) {
		//it's important that this method does not hit the database!
		//TODO: how can we guarantee that?
		return rxHibernateSession.getReference( entityClass, id );
	}

	@Override
	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object primaryKey) {
		return find( entityClass, primaryKey, null, null );
	}

	public <T> CompletionStage<Optional<T>> find(
			Class<T> entityClass,
			Object primaryKey,
			Map<String, Object> properties) {
		return find( entityClass, primaryKey, null, properties );
	}

	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object primaryKey, LockModeType lockModeType) {
		return find( entityClass, primaryKey, lockModeType, null );
	}

	public <T> CompletionStage<Optional<T>> find(
			Class<T> entityClass,
			Object primaryKey,
			LockModeType lockModeType,
			Map<String, Object> properties) {
//		checkOpen();

		LockOptions lockOptions = null;

		return RxUtil.nullFuture()
				.thenCompose( v -> {
					rxHibernateSession.getLoadQueryInfluencers().getEffectiveEntityGraph().applyConfiguredGraph( properties );

					final RxIdentifierLoadAccessImpl<T> loadAccess = byId( entityClass );
//			loadAccess.with( determineAppropriateLocalCacheMode( properties ) );

//			if ( lockModeType != null ) {
//				if ( !LockModeType.NONE.equals( lockModeType) ) {
//					checkTransactionNeededForUpdateOperation();
//				}
//				lockOptions = buildLockOptions( lockModeType, properties );
//				loadAccess.with( lockOptions );
//			}

					return loadAccess.load( (Serializable) primaryKey );
				} ).handle( (v, x) -> {
					if ( x instanceof EntityNotFoundException ) {
						// DefaultLoadEventListener.returnNarrowedProxy may throw ENFE (see HHH-7861 for details),
						// which find() should not throw. Find() should return null if the entity was not found.
						//			if ( log.isDebugEnabled() ) {
						//				String entityName = entityClass != null ? entityClass.getName(): null;
						//				String identifierValue = primaryKey != null ? primaryKey.toString() : null ;
						//				log.ignoringEntityNotFound( entityName, identifierValue );
						//			}
						return null;
					}
					if ( x instanceof ObjectDeletedException ) {
						//the spec is silent about people doing remove() find() on the same PC
						return null;
					}
					if ( x instanceof ObjectNotFoundException ) {
						//should not happen on the entity itself with get
						throw new IllegalArgumentException( x.getMessage(), x );
					}
					if ( x instanceof MappingException
							|| x instanceof TypeMismatchException
							|| x instanceof ClassCastException ) {
						throw exceptionConverter().convert( new IllegalArgumentException( x.getMessage(), x ) );
					}
					if ( x instanceof JDBCException ) {
//			if ( accessTransaction().getRollbackOnly() ) {
//				// assume this is the similar to the WildFly / IronJacamar "feature" described under HHH-12472
//				return null;
//			}
//			else {
						throw exceptionConverter().convert( (JDBCException) x, lockOptions );
//			}
					}
					if ( x instanceof RuntimeException ) {
						throw exceptionConverter().convert( (RuntimeException) x, lockOptions );
					}
					return v;
				} ).whenComplete( (v, x) -> {
					rxHibernateSession.getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
				} );
	}

	public <T> RxIdentifierLoadAccessImpl<T> byId(Class<T> entityClass) {
		return new RxIdentifierLoadAccessImpl( entityClass );
	}

	@Override
	public CompletionStage<RxSession> persist(Object entity) {
		return schedulePersist( entity ).thenApply( v-> this );
	}

	// Should be similar to firePersist
	private CompletionStage<Void> schedulePersist(Object entity) {
		CompletionStage<Void> ret = RxUtil.nullFuture();
		for ( PersistEventListener listener : listeners( EventType.PERSIST ) ) {
			PersistEvent event = new PersistEvent( null, entity, (EventSource) rxHibernateSession );
			ret = ret.thenCompose( v -> ( (RxPersistEventListener) listener ).rxOnPersist( event ) );
		}
		return ret;
	}

	@Override
	public CompletionStage<RxSession> remove(Object entity) {
		return fireRemove( entity ).thenApply( v-> this );
	}

	// Should be similar to fireRemove
	private CompletionStage<Void> fireRemove(Object entity) {
		CompletionStage<Void> ret = RxUtil.nullFuture();
		for ( DeleteEventListener listener : listeners( EventType.DELETE ) ) {
			DeleteEvent event = new DeleteEvent( null, entity, (EventSource) rxHibernateSession );
			ret = ret.thenCompose( v -> ( (RxDeleteEventListener) listener ).rxOnDelete( event ) );
		}
		return ret;
	}

	private ExceptionConverter exceptionConverter() {
		return rxHibernateSession.unwrap( EventSource.class ).getExceptionConverter();
	}

	private <T> Iterable<T> listeners(EventType<T> type) {
		return eventListenerGroup( type ).listeners();
	}

	private <T> EventListenerGroup<T> eventListenerGroup(EventType<T> type) {
		return factory.unwrap( SessionFactoryImplementor.class )
				.getServiceRegistry().getService( EventListenerRegistry.class )
				.getEventListenerGroup( type );
	}

	@Override
	public <R> RxQuery<R> createQuery(Class<R> resultType, String jpql) {
		return null;
	}

	private ServiceRegistry serviceRegistry() {
		return factory.unwrap( SessionFactoryImplementor.class ).getServiceRegistry();
	}

	private SessionFactoryImplementor getFactory() {
		return factory.unwrap( SessionFactoryImplementor.class );
	}

	private EntityPersister locateEntityPersister(Class entityClass) {
		return getFactory().getMetamodel().locateEntityPersister( entityClass );
	}

	private EntityPersister locateEntityPersister(String entityName) {
		return getFactory().getMetamodel().locateEntityPersister( entityName );
	}

	private CompletionStage<Void> fireLoad(LoadEvent event, LoadEventListener.LoadType loadType) {
//		checkOpenOrWaitingForAutoClose();
		return fireLoadNoChecks( event, loadType );
//		delayedAfterCompletion();
	}

	//Performance note:
	// This version of #fireLoad is meant to be invoked by internal methods only,
	// so to skip the session open, transaction synch, etc.. checks,
	// which have been proven to be not particularly cheap:
	// it seems they prevent these hot methods from being inlined.
	private CompletionStage<Void> fireLoadNoChecks(LoadEvent event, LoadEventListener.LoadType loadType) {
//		pulseTransactionCoordinator();
		CompletionStage<Void> ret = RxUtil.nullFuture();
		for ( LoadEventListener listener : listeners( EventType.LOAD ) ) {
			ret = ret.thenCompose( v -> ( (RxLoadEventListener) listener ).rxOnLoad( event, loadType ) );
		}
		return ret;
	}

	/**
	 * Check if there is a Hibernate or JTA transaction in progress and,
	 * if there is not, flush if necessary, make sure the connection has
	 * been committed (if it is not in autocommit mode) and run the after
	 * completion processing
	 *
	 * @param success Was the operation a success
	 */
	public void afterOperation(boolean success) {
//		if ( !isTransactionInProgress() ) {
//			getJdbcCoordinator().afterTransaction();
//		}
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
			this( locateEntityPersister( entityName ) );
		}

		public RxIdentifierLoadAccessImpl(Class<T> entityClass) {
			this( locateEntityPersister( entityClass ) );
		}

		public final RxIdentifierLoadAccessImpl with(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		public RxIdentifierLoadAccessImpl with(CacheMode cacheMode) {
			this.cacheMode = cacheMode;
			return this;
		}

		public RxIdentifierLoadAccessImpl with(RootGraph<T> graph, GraphSemantic semantic) {
			this.rootGraph = (RootGraphImplementor<T>) graph;
			this.graphSemantic = semantic;
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
			CacheMode sessionCacheMode = rxHibernateSession.getCacheMode();
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
				rxHibernateSession.getLoadQueryInfluencers().getEffectiveEntityGraph().applyGraph( rootGraph, graphSemantic );
			}

			boolean finalCacheModeChanged = cacheModeChanged;
			return executor.get()
					.whenComplete( (v, x) -> {
						if ( graphSemantic != null ) {
							rxHibernateSession.getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
						}
						if ( finalCacheModeChanged ) {
							// change it back
							setCacheMode( sessionCacheMode );
						}
					} );
		}

		protected CompletionStage<Optional<T>> doGetReference(Serializable id) {
			if ( this.lockOptions != null ) {
				LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), lockOptions, (EventSource) rxHibernateSession);
				return fireLoad( event, LoadEventListener.LOAD ).thenApply( v -> (Optional<T>) event.getResult() );
			}

			LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), false, (EventSource) rxHibernateSession);
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

		protected final CompletionStage<Optional<T>> doLoad(Serializable id, LoadEventListener.LoadType loadType) {
			if ( this.lockOptions != null ) {
				LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), lockOptions, (EventSource) rxHibernateSession
				);
				return fireLoad( event, loadType ).thenApply( v -> (Optional<T>) event.getResult() );
			}

			LoadEvent event = new LoadEvent(id, entityPersister.getEntityName(), false, (EventSource) rxHibernateSession);
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
	public FlushMode getFlushMode() {
		switch ( rxHibernateSession.getHibernateFlushMode() ) {
			case MANUAL:
				return FlushMode.MANUAL;
			case COMMIT:
				return FlushMode.COMMIT;
			case AUTO:
				return FlushMode.AUTO;
			case ALWAYS:
				return FlushMode.ALWAYS;
			default:
				throw new IllegalStateException("impossible flush mode");
		}
	}

	@Override
	public void setFlushMode(FlushMode flushMode) {
		switch (flushMode) {
			case COMMIT:
				rxHibernateSession.setHibernateFlushMode(org.hibernate.FlushMode.COMMIT);
				break;
			case AUTO:
				rxHibernateSession.setHibernateFlushMode(org.hibernate.FlushMode.AUTO);
				break;
			case MANUAL:
				rxHibernateSession.setHibernateFlushMode(org.hibernate.FlushMode.MANUAL);
				break;
			case ALWAYS:
				rxHibernateSession.setHibernateFlushMode(org.hibernate.FlushMode.ALWAYS);
				break;
		}
	}

	public CacheMode getCacheMode() {
		return rxHibernateSession.getCacheMode();
	}

	public void setCacheMode(CacheMode cacheMode) {
		rxHibernateSession.setCacheMode(cacheMode);
	}

	@Override
	public void detach(Object entity) {
		rxHibernateSession.detach(entity);
	}

	@Override
	public void clear() {
		rxHibernateSession.clear();
	}

	@Override
	public void enableFetchProfile(String name) {
		rxHibernateSession.enableFetchProfile(name);
	}

	@Override
	public void disableFetchProfile(String name) {
		rxHibernateSession.disableFetchProfile(name);
	}

	@Override
	public boolean isFetchProfileEnabled(String name) {
		return rxHibernateSession.isFetchProfileEnabled(name);
	}
}
