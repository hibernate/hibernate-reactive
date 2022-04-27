/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import java.lang.invoke.MethodHandles;

import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;

import org.hibernate.reactive.context.Context;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * An adaptor for the Vert.x {@link io.vertx.core.Context}.
 *
 * @author Gavin King
 */
public class VertxContext implements Context, ServiceRegistryAwareService {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private VertxInstance vertxInstance;

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		vertxInstance = serviceRegistry.getService( VertxInstance.class );
	}

	@Override
	public <T> void put(Key<T> key, T instance) {
		final io.vertx.core.Context context = Vertx.currentContext();
		if ( context != null ) {
			LOG.tracef( "Putting key,value in context: [%1$s, %2$s]", key, instance );
			context.putLocal( key, instance );
		}
		else {
			LOG.tracef( "Context is null for key,value: [%1$s, %2$s]", key, instance  );
			throw LOG.notVertxContextActive();
		}
	}

	@Override
	public <T> T get(Key<T> key) {
		final io.vertx.core.Context context = Vertx.currentContext();
		if ( context != null ) {
			T local = context.getLocal( key );
			LOG.tracef( "Getting value %2$s from context for key %1$s", key, local  );
			return local;
		}
		else {
			LOG.tracef( "Getting null from context for key %s", key );
			return null;
		}
	}

	@Override
	public void remove(Key<?> key) {
		final io.vertx.core.Context context = Vertx.currentContext();
		if ( context != null ) {
			boolean removed = context.removeLocal( key );
			LOG.tracef( "Key %s removed from context: %s", key, removed );
		}
		else {
			LOG.tracef( "Context is null, nothing to remove for key %s", key );
		}
	}

	@Override
	public void execute(Runnable runnable) {
		final io.vertx.core.Context currentContext = Vertx.currentContext();
		if ( currentContext == null ) {
			LOG.tracef( "Not in a Vert.x context, checking the VertxInstance service" );
			final io.vertx.core.Context newContext = vertxInstance.getVertx().getOrCreateContext();
			// Ensure we don't run on the root context, which is globally scoped:
			// that could lead to unintentionally share the same session with other streams.
			ContextInternal newContextInternal = (ContextInternal) newContext;
			final ContextInternal duplicate = newContextInternal.duplicate();
			LOG.tracef( "Using duplicated context from VertxInstance: %s", duplicate );
			duplicate.runOnContext( x -> runnable.run() );
		}
		else {
			LOG.tracef( "Running in the current Vert.x context %s", currentContext );
			runnable.run();
		}
	}

}
