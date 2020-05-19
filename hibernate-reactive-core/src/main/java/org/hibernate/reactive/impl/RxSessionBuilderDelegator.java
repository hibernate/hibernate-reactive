package org.hibernate.reactive.impl;

import org.hibernate.SessionBuilder;
import org.hibernate.engine.spi.AbstractDelegatingSessionBuilderImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionFactoryImpl.SessionBuilderImpl;
import org.hibernate.reactive.stage.RxSession;
import org.hibernate.reactive.internal.RxSessionInternal;
import org.hibernate.reactive.engine.spi.RxSessionBuilderImplementor;

public class RxSessionBuilderDelegator
		extends AbstractDelegatingSessionBuilderImplementor<RxSessionBuilderImplementor>
		implements RxSessionBuilderImplementor {

	private SessionCreationOptions options;
	private final SessionFactoryImpl factory;

	public RxSessionBuilderDelegator(
			SessionBuilderImpl<SessionBuilder> builder,
			SessionFactoryImpl factory) {
		super(builder);
		this.options = builder;
		this.factory = factory;
	}

	@Override
	public RxSessionInternal openSession() {
		return new RxSessionInternalImpl(factory, options);
	}

	@Override
	public RxSession openRxSession() {
		return openSession().reactive();
	}

}
