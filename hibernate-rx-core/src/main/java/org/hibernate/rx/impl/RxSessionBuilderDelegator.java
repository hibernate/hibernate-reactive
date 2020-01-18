package org.hibernate.rx.impl;

import org.hibernate.engine.spi.AbstractDelegatingSessionBuilderImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.engine.spi.RxSessionBuilderImplementor;

public class RxSessionBuilderDelegator
		extends AbstractDelegatingSessionBuilderImplementor<RxSessionBuilderImplementor>
		implements RxSessionBuilderImplementor {

	private SessionCreationOptions options;
	private final SessionFactoryImpl factory;

	public RxSessionBuilderDelegator(
			SessionFactoryImpl.SessionBuilderImpl builder,
			SessionFactoryImpl factory) {
		super(builder);
		this.options = builder;
		this.factory = factory;
	}

	@Override
	public RxSession openRxSession() {
		return new RxSessionInternalImpl(factory, options).reactive();
	}

}
