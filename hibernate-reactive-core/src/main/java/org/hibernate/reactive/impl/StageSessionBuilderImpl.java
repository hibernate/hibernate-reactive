package org.hibernate.reactive.impl;

import org.hibernate.SessionBuilder;
import org.hibernate.engine.spi.AbstractDelegatingSessionBuilderImplementor;
import org.hibernate.engine.spi.SessionBuilderImplementor;
import org.hibernate.internal.SessionCreationOptions;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.internal.SessionFactoryImpl.SessionBuilderImpl;
import org.hibernate.reactive.service.ReactiveConnection;
import org.hibernate.reactive.service.initiator.ReactiveConnectionPoolProvider;
import org.hibernate.reactive.stage.Stage;

import java.util.concurrent.CompletionStage;

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
		ReactiveConnection reactiveConnection = factory.getServiceRegistry()
				.getService( ReactiveConnectionPoolProvider.class )
				.getConnection()
				.toCompletableFuture()
				.join();
		return new ReactiveSessionInternalImpl( factory, options, reactiveConnection );
	}

	@Override
	public CompletionStage<Stage.Session> openReactiveSession() {
		return factory.getServiceRegistry()
				.getService( ReactiveConnectionPoolProvider.class )
				.getConnection()
				.thenApply( reactiveConnection -> new ReactiveSessionInternalImpl( factory, options, reactiveConnection )
						.reactive() );
	}

}
