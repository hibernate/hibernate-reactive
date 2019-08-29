package org.hibernate.rx.impl;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxHibernateSessionFactory;
import org.hibernate.rx.RxQuery;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.StateControl;
import org.hibernate.rx.event.RxPersistEvent;
import org.hibernate.service.ServiceRegistry;

public class RxSessionImpl implements RxSession {

	// Might make sense to have a service or delegator for this
	private Executor executor = ForkJoinPool.commonPool();
	private final RxHibernateSessionFactory factory;
	private final RxHibernateSession rxHibernateSession;
	private CompletionStage<?> stage;


	public RxSessionImpl(RxHibernateSessionFactory factory, RxHibernateSession session) {
		this( factory, session, new CompletableFuture<>() );
	}

	public <T> RxSessionImpl(RxHibernateSessionFactory factory, RxHibernateSession session, CompletionStage<T> stage) {
		this.factory = factory;
		this.rxHibernateSession = session;
		this.stage = stage;
	}

	@Override
	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object id) {
		return CompletableFuture.supplyAsync( () -> {
			System.out.println( "Start find" );
			T result = rxHibernateSession.find( entityClass, id );
			System.out.println( "Return result: " + result );
			return Optional.ofNullable( result );
		} );
	}

	@Override
	public CompletionStage<Void> persist(Object entity) {
		return CompletableFuture.runAsync( () -> {
			schedulePersist( entity, null );
		} );
	}

	// Should be similar to firePersist
	private void schedulePersist(Object entity, CompletionStage<?> stage) {
		for ( PersistEventListener listener : listeners( EventType.PERSIST ) ) {
			RxPersistEvent event = new RxPersistEvent( null, entity, rxHibernateSession, this );
			listener.onPersist( event );
			// Let's assume there is only one
			break;
		}
	}

	private Executor executor() {
		return ForkJoinPool.commonPool();
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
	public CompletionStage<Void> remove(Object entity) {
		return CompletableFuture.runAsync( () -> {
			rxHibernateSession.remove( entity );
		} );
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
}
