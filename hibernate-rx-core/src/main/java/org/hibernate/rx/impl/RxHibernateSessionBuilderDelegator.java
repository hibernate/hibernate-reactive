package org.hibernate.rx.impl;

import org.hibernate.Session;
import org.hibernate.engine.spi.AbstractDelegatingSessionBuilderImplementor;
import org.hibernate.engine.spi.SessionBuilderImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.rx.RxHibernateSession;
import org.hibernate.rx.engine.spi.RxHibernateSessionBuilderImplementor;
import org.hibernate.rx.engine.spi.RxHibernateSessionFactoryImplementor;

public class RxHibernateSessionBuilderDelegator
		extends AbstractDelegatingSessionBuilderImplementor<RxHibernateSessionBuilderImplementor>
		implements RxHibernateSessionBuilderImplementor {

	private final SessionBuilderImplementor builder;
	private final RxHibernateSessionFactoryImplementor factory;

	public RxHibernateSessionBuilderDelegator(SessionBuilderImplementor sessionBuilder, RxHibernateSessionFactoryImplementor factory) {
		super( sessionBuilder );

		this.builder = sessionBuilder;
		this.factory = factory;
	}

	@Override
	public RxHibernateSession openRxSession() {
		Session session = builder.openSession();
		return new RxHibernateSessionImpl( factory, (EventSource) session );
	}

}
