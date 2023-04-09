/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.classic.Lifecycle;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.PostLoadEvent;
import org.hibernate.event.spi.PostLoadEventListener;
import org.hibernate.jpa.event.spi.CallbackRegistry;
import org.hibernate.jpa.event.spi.CallbackRegistryConsumer;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.impl.ReactiveEntityIncrementVersionProcess;
import org.hibernate.reactive.engine.impl.ReactiveEntityVerifyVersionProcess;
import org.hibernate.reactive.session.ReactiveSession;

/**
 * We do two things here:
 * <ul>
 * <li>Call {@link Lifecycle} interface if necessary</li>
 * <li>Perform needed {@link EntityEntry#getLockMode()} related processing</li>
 * </ul>
 *
 * @author Gavin King
 * @author Steve Ebersole
 */
public class DefaultReactivePostLoadEventListener implements PostLoadEventListener, CallbackRegistryConsumer {
	private CallbackRegistry callbackRegistry;

	@Override
	public void injectCallbackRegistry(CallbackRegistry callbackRegistry) {
		this.callbackRegistry = callbackRegistry;
	}

	@Override
	public void onPostLoad(PostLoadEvent event) {
		final Object entity = event.getEntity();

		callbackRegistry.postLoad( entity );

		final EventSource session = event.getSession();
		final EntityEntry entry = session.getPersistenceContextInternal().getEntry( entity );
		if ( entry == null ) {
			throw new AssertionFailure( "possible non-threadsafe access to the session" );
		}

		final ReactiveActionQueue actionQueue = ((ReactiveSession) session).getReactiveActionQueue();
		switch ( entry.getLockMode() ) {
//			case PESSIMISTIC_FORCE_INCREMENT:
			// This case is handled by DefaultReactiveLoadEventListener
			case OPTIMISTIC_FORCE_INCREMENT:
				actionQueue.registerProcess( new ReactiveEntityIncrementVersionProcess( entity ) );
				break;
			case OPTIMISTIC:
				actionQueue.registerProcess( new ReactiveEntityVerifyVersionProcess( entity ) );
				break;
		}
	}

}
