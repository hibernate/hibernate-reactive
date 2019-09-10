package org.hibernate.rx.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

import javax.persistence.CacheRetrieveMode;
import javax.persistence.CacheStoreMode;
import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.LockModeType;

import org.hibernate.CacheMode;
import org.hibernate.IdentifierLoadAccess;
import org.hibernate.JDBCException;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.ObjectDeletedException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.TypeMismatchException;
import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.internal.SessionImpl;
import org.hibernate.jpa.internal.util.CacheModeHelper;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.StateControl;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;
import org.hibernate.rx.event.RxDeleteEvent;
import org.hibernate.rx.event.RxLoadEvent;
import org.hibernate.rx.event.RxPersistEvent;
import org.hibernate.service.ServiceRegistry;

public class RxSessionImpl implements RxSession {

	// Might make sense to have a service or delegator for this
	private Executor executor = ForkJoinPool.commonPool();
	private final RxHibernateSessionFactory factory;
	private final RxHibernateSession rxHibernateSession;
	private CompletionStage<?> stage;
	private transient LoadQueryInfluencers loadQueryInfluencers;

	public RxSessionImpl(RxHibernateSessionFactoryImplementor factory, RxHibernateSession session) {
		this( factory, session, new CompletableFuture<>() );
	}

	public <T> RxSessionImpl(RxHibernateSessionFactoryImplementor factory, RxHibernateSession session, CompletionStage<T> stage) {
		this.factory = factory;
		this.rxHibernateSession = session;
		this.stage = stage;
		this.loadQueryInfluencers = new LoadQueryInfluencers( factory );
	}

	public LoadQueryInfluencers getLoadQueryInfluencers() {
		return loadQueryInfluencers;
	}

	@Override
	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object primaryKey) {
		return find( entityClass, primaryKey, null, null );
	}

	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
		return find( entityClass, primaryKey, null, properties );
	}

	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object primaryKey, LockModeType lockModeType) {
		return find( entityClass, primaryKey, lockModeType, null );
	}

	private void setCacheMode(CacheMode cacheMode) {
		this.rxHibernateSession.setCacheMode( cacheMode );
	}

	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object primaryKey, LockModeType lockModeType, Map<String, Object> properties) {
//		checkOpen();

		LockOptions lockOptions = null;

		try {
			getLoadQueryInfluencers().getEffectiveEntityGraph().applyConfiguredGraph( properties );

			final RxIdentifierLoadAccessImpl loadAccess = byId( entityClass );
//			loadAccess.with( determineAppropriateLocalCacheMode( properties ) );

//			if ( lockModeType != null ) {
//				if ( !LockModeType.NONE.equals( lockModeType) ) {
//					checkTransactionNeededForUpdateOperation();
//				}
//				lockOptions = buildLockOptions( lockModeType, properties );
//				loadAccess.with( lockOptions );
//			}

			return loadAccess.load( (Serializable) primaryKey );
		}
		catch ( EntityNotFoundException ignored ) {
			// DefaultLoadEventListener.returnNarrowedProxy may throw ENFE (see HHH-7861 for details),
			// which find() should not throw.  Find() should return null if the entity was not found.
//			if ( log.isDebugEnabled() ) {
//				String entityName = entityClass != null ? entityClass.getName(): null;
//				String identifierValue = primaryKey != null ? primaryKey.toString() : null ;
//				log.ignoringEntityNotFound( entityName, identifierValue );
//			}
			return null;
		}
		catch ( ObjectDeletedException e ) {
			//the spec is silent about people doing remove() find() on the same PC
			return null;
		}
		catch ( ObjectNotFoundException e ) {
			//should not happen on the entity itself with get
			throw new IllegalArgumentException( e.getMessage(), e );
		}
		catch ( MappingException | TypeMismatchException | ClassCastException e ) {
			throw exceptionConverter().convert( new IllegalArgumentException( e.getMessage(), e ) );
		}
		catch ( JDBCException e ) {
//			if ( accessTransaction().getRollbackOnly() ) {
//				// assume this is the similar to the WildFly / IronJacamar "feature" described under HHH-12472
//				return null;
//			}
//			else {
				throw exceptionConverter().convert( e, lockOptions );
//			}
		}
		catch ( RuntimeException e ) {
			throw exceptionConverter().convert( e, lockOptions );
		}
		finally {
			getLoadQueryInfluencers().getEffectiveEntityGraph().clear();
		}
	}

	public <T> RxIdentifierLoadAccessImpl byId(Class<T> entityClass) {
		return new RxIdentifierLoadAccessImpl( entityClass );
	}

	@Override
	public CompletionStage<Void> persist(Object entity) {
		CompletionStage<Void> stage = new CompletableFuture<>();
		schedulePersist( entity, stage );
		return stage;
	}

	// Should be similar to firePersist
	private void schedulePersist(Object entity, CompletionStage<Void> stage) {
		for ( PersistEventListener listener : listeners( EventType.PERSIST ) ) {
			RxPersistEvent event = new RxPersistEvent( null, entity, (EventSource) rxHibernateSession, this, stage );
			listener.onPersist( event );
		}
	}

	@Override
	public CompletionStage<Void> remove(Object entity) {
		CompletionStage<Void> stage = new CompletableFuture<>();
		fireRemove( entity, stage );
		return stage;
	}

	// Should be similar to fireRemove
	private void fireRemove(Object entity, CompletionStage<Void> stage) {
		for ( DeleteEventListener listener : listeners( EventType.DELETE ) ) {
			DeleteEvent event = new RxDeleteEvent( null, entity, (EventSource) rxHibernateSession, this, stage );
			listener.onDelete( event );
		}
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

	@Override
	public StateControl sessionState() {
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

	private void fireLoad(LoadEvent event, LoadEventListener.LoadType loadType) {
//		checkOpenOrWaitingForAutoClose();
		fireLoadNoChecks( event, loadType );
//		delayedAfterCompletion();
	}

	//Performance note:
	// This version of #fireLoad is meant to be invoked by internal methods only,
	// so to skip the session open, transaction synch, etc.. checks,
	// which have been proven to be not particularly cheap:
	// it seems they prevent these hot methods from being inlined.
	private void fireLoadNoChecks(LoadEvent event, LoadEventListener.LoadType loadType) {
//		pulseTransactionCoordinator();
		for ( LoadEventListener listener : listeners( EventType.LOAD ) ) {
			listener.onLoad( event, loadType );
		}
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

			try {
				if ( graphSemantic != null ) {
					if ( rootGraph == null ) {
						throw new IllegalArgumentException( "Graph semantic specified, but no RootGraph was supplied" );
					}
					loadQueryInfluencers.getEffectiveEntityGraph().applyGraph( rootGraph, graphSemantic );
				}

				try {
					return executor.get();
				}
				finally {
					if ( graphSemantic != null ) {
						loadQueryInfluencers.getEffectiveEntityGraph().clear();
					}
				}
			}
			finally {
				if ( cacheModeChanged ) {
					// change it back
					setCacheMode( sessionCacheMode );
				}
			}
		}

		protected CompletionStage<Optional<T>> doGetReference(Serializable id) {
			if ( this.lockOptions != null ) {
				RxLoadEvent event = new RxLoadEvent( id, entityPersister.getEntityName(), lockOptions, (EventSource) rxHibernateSession, RxSessionImpl.this );
				fireLoad( event, LoadEventListener.LOAD );
				return (CompletionStage<Optional<T>>) event.getResult();
			}

			RxLoadEvent event = new RxLoadEvent( id, entityPersister.getEntityName(), false, (EventSource) rxHibernateSession, RxSessionImpl.this );
			boolean success = false;
			try {
				fireLoad( event, LoadEventListener.LOAD );
				if ( event.getResult() == null ) {
					getFactory().getEntityNotFoundDelegate().handleEntityNotFound(
							entityPersister.getEntityName(),
							id
					);
				}
				success = true;
				return (CompletionStage<Optional<T>>) event.getResult();
			}
			finally {
				afterOperation( success );
			}
		}

		public final CompletionStage<Optional<T>> load(Serializable id) {
			return perform( () -> doLoad( id ) );
		}

		protected final CompletionStage<Optional<T>> doLoad(Serializable id) {
			if ( this.lockOptions != null ) {
				RxLoadEvent event = new RxLoadEvent( id, entityPersister.getEntityName(), lockOptions, (EventSource) rxHibernateSession, RxSessionImpl.this );
				fireLoad( event, LoadEventListener.GET );
				return (CompletionStage<Optional<T>>) event.getResult();
			}

			LoadEvent event = new LoadEvent( id, entityPersister.getEntityName(), false,
											 (EventSource) rxHibernateSession
			);
			boolean success = false;
			try {
				fireLoad( event, LoadEventListener.GET );
				success = true;
			}
			catch (ObjectNotFoundException e) {
				// if session cache contains proxy for non-existing object
			}
			finally {
				afterOperation( success );
			}
			return (CompletionStage<Optional<T>>) event.getResult();
		}
	}
}
