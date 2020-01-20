package org.hibernate.rx.service;

import org.hibernate.boot.Metadata;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.rx.event.impl.*;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.service.spi.SessionFactoryServiceRegistry;

/**
 * Integrates Hibernate Reactive with Hibernate ORM
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

		eventListenerRegistry.addDuplicationStrategy( DefaultRxFlushEventListener.EventContextManagingFlushEventListenerDuplicationStrategy.INSTANCE );
		eventListenerRegistry.getEventListenerGroup( EventType.FLUSH ).appendListener( new DefaultRxFlushEventListener() );

		eventListenerRegistry.addDuplicationStrategy( DefaultRxFlushEntityEventListener.EventContextManagingFlushEventListenerDuplicationStrategy.INSTANCE );
		eventListenerRegistry.getEventListenerGroup( EventType.FLUSH_ENTITY ).appendListener( new DefaultRxFlushEntityEventListener() );

		eventListenerRegistry.addDuplicationStrategy( DefaultRxPersistEventListener.EventContextManagingPersistEventListenerDuplicationStrategy.INSTANCE );
		eventListenerRegistry.getEventListenerGroup( EventType.PERSIST ).appendListener( new DefaultRxPersistEventListener() );
		eventListenerRegistry.getEventListenerGroup( EventType.PERSIST_ONFLUSH )
				.appendListener( new DefaultRxPersistOnFlushEventListener() );

		eventListenerRegistry.addDuplicationStrategy( DefaultRxDeleteEventListener.EventContextManagingDeleteEventListenerDuplicationStrategy.INSTANCE );
		eventListenerRegistry.getEventListenerGroup( EventType.DELETE ).appendListener( new DefaultRxDeleteEventListener() );

		eventListenerRegistry.addDuplicationStrategy( DefaultRxRefreshEventListener.EventContextManagingDeleteEventListenerDuplicationStrategy.INSTANCE );
		eventListenerRegistry.getEventListenerGroup( EventType.REFRESH ).appendListener( new DefaultRxRefreshEventListener() );

		eventListenerRegistry.addDuplicationStrategy( DefaultRxLoadEventListener.EventContextManagingLoadEventListenerDuplicationStrategy.INSTANCE );
		eventListenerRegistry.getEventListenerGroup( EventType.LOAD ).appendListener( new DefaultRxLoadEventListener() );
	}

}
