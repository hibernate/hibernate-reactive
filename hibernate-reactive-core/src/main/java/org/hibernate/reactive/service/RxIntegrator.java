package org.hibernate.reactive.service;

import org.hibernate.boot.Metadata;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.reactive.event.impl.*;
import org.hibernate.service.spi.SessionFactoryServiceRegistry;

/**
 * Integrates Hibernate Reactive with Hibernate ORM by
 * replacing the built-in
 * {@link org.hibernate.event.spi.AbstractEvent event}
 * listeners with reactive listeners.
 */
public class RxIntegrator implements Integrator {
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
		EventListenerRegistry eventListenerRegistry = serviceRegistry.getService( EventListenerRegistry.class );
		eventListenerRegistry.addDuplicationStrategy( ReplacementDuplicationStrategy.INSTANCE );

		eventListenerRegistry.getEventListenerGroup( EventType.AUTO_FLUSH ).appendListener( new DefaultRxAutoFlushEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.FLUSH ).appendListener( new DefaultRxFlushEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.FLUSH_ENTITY ).appendListener( new DefaultRxFlushEntityEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.PERSIST ).appendListener( new DefaultRxPersistEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.PERSIST_ONFLUSH ).appendListener( new DefaultRxPersistOnFlushEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.MERGE ).appendListener( new DefaultRxMergeEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.DELETE ).appendListener( new DefaultRxDeleteEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.REFRESH ).appendListener( new DefaultRxRefreshEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.LOAD ).appendListener( new DefaultRxLoadEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.INIT_COLLECTION ).appendListener( new DefaultRxInitializeCollectionEventListener() );
	}

}
