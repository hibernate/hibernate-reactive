package org.hibernate.rx.engine.impl;

import java.util.function.Consumer;

import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.SessionDelegatorBaseImpl;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.transaction.spi.TransactionImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.internal.ExceptionMapperStandardImpl;
import org.hibernate.resource.transaction.backend.jta.internal.synchronization.ExceptionMapper;
import org.hibernate.resource.transaction.spi.TransactionCoordinator;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.engine.spi.RxActionQueue;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;
import org.hibernate.rx.event.RxPersistEvent;
import org.hibernate.rx.impl.RxSessionImpl;

public class RxHibernateSessionImpl extends SessionDelegatorBaseImpl implements RxHibernateSession, EventSource {

	private final RxHibernateSessionFactoryImplementor factory;
	private final ExceptionConverter exceptionConverter;
	private final ExceptionMapper exceptionMapper = ExceptionMapperStandardImpl.INSTANCE;
	private transient RxActionQueue rxActionQueue;

	public RxHibernateSessionImpl(RxHibernateSessionFactoryImplementor factory, SessionImplementor delegate) {
		super( delegate );
		this.rxActionQueue = new RxActionQueue( this );
		this.factory = factory;
		this.exceptionConverter = delegate.getExceptionConverter();
	}

	@Override
	public TransactionCoordinator getTransactionCoordinator() {
		return super.getTransactionCoordinator();
	}

	@Override
	public RxHibernateSessionFactoryImplementor getSessionFactory() {
		return factory;
	}

	@Override
	public RxActionQueue getRxActionQueue() {
		return rxActionQueue;
	}

	@Override
	public SessionImplementor getSession() {
		return this;
	}

	@Override
	public RxSession reactive() {
		return new RxSessionImpl( factory, this );
	}

	@Override
	public void reactive(Consumer<RxSession> consumer) {
		consumer.accept( new RxSessionImpl( factory, this ) );
	}
}

