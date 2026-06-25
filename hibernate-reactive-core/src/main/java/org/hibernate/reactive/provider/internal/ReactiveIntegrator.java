/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.internal;

import java.lang.invoke.MethodHandles;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.reactive.event.internal.DefaultReactiveAutoFlushEventListener;
import org.hibernate.reactive.event.internal.DefaultReactiveDeleteEventListener;
import org.hibernate.reactive.event.internal.DefaultReactiveFlushEntityEventListener;
import org.hibernate.reactive.event.internal.DefaultReactiveFlushEventListener;
import org.hibernate.reactive.event.internal.DefaultReactiveInitializeCollectionEventListener;
import org.hibernate.reactive.event.internal.DefaultReactiveLoadEventListener;
import org.hibernate.reactive.event.internal.DefaultReactiveLockEventListener;
import org.hibernate.reactive.event.internal.DefaultReactiveMergeEventListener;
import org.hibernate.reactive.event.internal.DefaultReactivePersistEventListener;
import org.hibernate.reactive.event.internal.DefaultReactivePersistOnFlushEventListener;
import org.hibernate.reactive.event.internal.DefaultReactivePostLoadEventListener;
import org.hibernate.reactive.event.internal.DefaultReactiveRefreshEventListener;
import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;
import org.hibernate.reactive.logging.internal.Version;
import org.hibernate.service.ServiceRegistry;

/**
 * Integrates Hibernate Reactive with Hibernate ORM by
 * replacing the built-in
 * {@link org.hibernate.event.spi.AbstractEvent event}
 * listeners with reactive listeners.
 * This is only applied if the Hibernate ORM instance we're registering with
 * is marked as being reactive.
 */
public class ReactiveIntegrator implements Integrator {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public void integrate(Metadata metadata, BootstrapContext bootstrapContext, SessionFactoryImplementor sessionFactory) {
		attachEventContextManagingListenersIfRequired( sessionFactory.getServiceRegistry() );
	}

	private void attachEventContextManagingListenersIfRequired(ServiceRegistry serviceRegistry) {
		if ( ReactiveModeCheck.isReactiveRegistry( serviceRegistry ) ) {
			LOG.startHibernateReactive( Version.getVersionString() );

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
		}
	}
}
