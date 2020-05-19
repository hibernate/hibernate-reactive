package org.hibernate.reactive.impl;

import org.hibernate.SessionBuilder;
import org.hibernate.engine.spi.AbstractDelegatingSessionBuilderImplementor;
import org.hibernate.engine.spi.SessionBuilderImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionFactoryImpl.SessionBuilderImpl;
import org.hibernate.reactive.stage.Stage;

public class StageSessionBuilderImpl
		extends AbstractDelegatingSessionBuilderImplementor<SessionBuilderImplementor<Stage.SessionBuilder>>
		implements Stage.SessionBuilder {

	private SessionCreationOptions options;
	private final SessionFactoryImpl factory;

	public StageSessionBuilderImpl(
			SessionBuilderImpl<SessionBuilder> builder,
			SessionFactoryImpl factory) {
		super(builder);
		this.options = builder;
		this.factory = factory;
	}

	@Override
	public ReactiveSessionInternal openSession() {
		return new ReactiveSessionInternalImpl(factory, options);
	}

	@Override
	public Stage.Session openReactiveSession() {
		return openSession().reactive();
	}

}
