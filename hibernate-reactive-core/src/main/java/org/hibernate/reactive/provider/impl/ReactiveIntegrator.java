/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.impl;

import org.hibernate.boot.Metadata;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.internal.CoreLogging;
import org.hibernate.reactive.event.impl.*;
import org.hibernate.service.spi.SessionFactoryServiceRegistry;

/**
 * Integrates Hibernate Reactive with Hibernate ORM by
 * replacing the built-in
 * {@link org.hibernate.event.spi.AbstractEvent event}
 * listeners with reactive listeners.
 * This is only applied if the Hibernate ORM instance we're registering with
 * is marked as being reactive.
 */
public class ReactiveIntegrator implements Integrator {

	@Override
	public void integrate(
			Metadata metadata,
			SessionFactoryImplementor sessionFactory,
			SessionFactoryServiceRegistry serviceRegistry) {
		attachEventContextManagingListenersIfRequired( serviceRegistry );
	}

	@Override
	public void disintegrate(
			SessionFactoryImplementor sessionFactory, SessionFactoryServiceRegistry serviceRegistry) {
	}

	private void attachEventContextManagingListenersIfRequired(SessionFactoryServiceRegistry serviceRegistry) {
		if ( ReactiveModeCheck.isReactiveRegistry( serviceRegistry ) ) {

			CoreLogging.messageLogger(ReactiveIntegrator.class).info("HRX000001: Hibernate Reactive Preview");

			EventListenerRegistry eventListenerRegistry = serviceRegistry.getService( EventListenerRegistry.class );
			eventListenerRegistry.addDuplicationStrategy( ReplacementDuplicationStrategy.INSTANCE );

			eventListenerRegistry.getEventListenerGroup( EventType.AUTO_FLUSH ).appendListener( new DefaultReactiveAutoFlushEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.FLUSH ).appendListener( new DefaultReactiveFlushEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.FLUSH_ENTITY ).appendListener( new DefaultReactiveFlushEntityEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.PERSIST ).appendListener( new DefaultReactivePersistEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.PERSIST_ONFLUSH ).appendListener( new DefaultReactivePersistOnFlushEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.MERGE ).appendListener( new DefaultReactiveMergeEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.DELETE ).appendListener( new DefaultReactiveDeleteEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.REFRESH ).appendListener( new DefaultReactiveRefreshEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.LOCK ).appendListener( new DefaultReactiveLockEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.LOAD ).appendListener( new DefaultReactiveLoadEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.INIT_COLLECTION ).appendListener( new DefaultReactiveInitializeCollectionEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.POST_LOAD ).appendListener( new DefaultReactivePostLoadEventListener() );
			eventListenerRegistry.getEventListenerGroup( EventType.RESOLVE_NATURAL_ID ).appendListener( new DefaultReactiveResolveNaturalIdEventListener() );
		}
	}

}
