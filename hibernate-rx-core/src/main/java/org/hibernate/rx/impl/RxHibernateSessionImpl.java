package org.hibernate.rx.impl;

import org.hibernate.engine.spi.SessionDelegatorBaseImpl;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.engine.spi.RxActionQueue;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;

import java.util.function.Consumer;

public class RxHibernateSessionImpl extends SessionDelegatorBaseImpl implements RxHibernateSession, EventSource {

	private final RxHibernateSessionFactoryImplementor factory;
	private transient RxActionQueue rxActionQueue;

	public RxHibernateSessionImpl(RxHibernateSessionFactoryImplementor factory, SessionImplementor delegate) {
		super( delegate );
		this.factory = factory;
		this.rxActionQueue = new RxActionQueue( this );
	}

	@Override
	public RxHibernateSessionFactoryImplementor getSessionFactory() {
		return factory;
	}

	@Override
	public RxActionQueue getRxActionQueue() {
		return rxActionQueue;
	}

	public SessionImplementor delegate() {
		return super.delegate();
	}

	@Override
	public RxSession reactive() {
		return new RxSessionImpl( factory, this );
	}

	@Override
	public void reactive(Consumer<RxSession> consumer) {
		consumer.accept( new RxSessionImpl( factory, this ) );
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		if ( RxHibernateSession.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}
		return super.unwrap( clazz );
	}
}

