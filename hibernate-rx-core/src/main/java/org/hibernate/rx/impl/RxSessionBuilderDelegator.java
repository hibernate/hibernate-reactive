package org.hibernate.rx.impl;

import org.hibernate.Session;
import org.hibernate.engine.spi.AbstractDelegatingSessionBuilderImplementor;
import org.hibernate.engine.spi.SessionBuilderImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.engine.spi.RxSessionBuilderImplementor;
import org.hibernate.rx.engine.spi.RxSessionFactoryImplementor;

public class RxSessionBuilderDelegator
		extends AbstractDelegatingSessionBuilderImplementor<RxSessionBuilderImplementor>
		implements RxSessionBuilderImplementor {

	private final SessionBuilderImplementor builder;
	private final RxSessionFactoryImplementor factory;

	public RxSessionBuilderDelegator(SessionBuilderImplementor sessionBuilder, RxSessionFactoryImplementor factory) {
		super( sessionBuilder );

		this.builder = sessionBuilder;
		this.factory = factory;
	}

	@Override
	public RxSession openRxSession() {
		Session session = builder.openSession();
		return new RxSessionInternalImpl( factory, (EventSource) session ).reactive();
	}

}
