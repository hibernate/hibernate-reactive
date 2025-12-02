/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.AbstractCollectionEvent;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PostCollectionRecreateEvent;
import org.hibernate.event.spi.PostCollectionRecreateEventListener;
import org.hibernate.event.spi.PostCollectionRemoveEvent;
import org.hibernate.event.spi.PostCollectionRemoveEventListener;
import org.hibernate.event.spi.PreCollectionRecreateEvent;
import org.hibernate.event.spi.PreCollectionRecreateEventListener;
import org.hibernate.event.spi.PreCollectionRemoveEvent;
import org.hibernate.event.spi.PreCollectionRemoveEventListener;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Adapt test in ORM: CollectionStatelessSessionListenerTest
 */
public class CollectionStatelessSessionListenerTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return Set.of( EntityA.class, EntityB.class );
	}

	@Test
	public void mutinyStatelessInsert(VertxTestContext context) {
		final List<AbstractCollectionEvent> events = new ArrayList<>();
		initializeListeners( events );

		EntityA a = new EntityA();
		EntityB b = new EntityB();
		a.children.add( b );
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( statelessSession -> statelessSession
						.insert( b )
						.chain( () -> statelessSession.insert( a ) )
						.chain( () -> statelessSession.delete( a ) )
						.chain( () -> statelessSession.delete( b ) )
				)
				.invoke( () -> assertEvents( events ) )
		);
	}

	@Test
	public void mutinyStatelessInsertAll(VertxTestContext context) {
		final List<AbstractCollectionEvent> events = new ArrayList<>();
		initializeListeners( events );

		EntityA a = new EntityA();
		EntityB b = new EntityB();
		a.children.add( b );
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( statelessSession -> statelessSession
						.insertAll( b, a )
						.chain( () -> statelessSession.deleteAll( a, b ) )
				)
				.invoke( () -> assertEvents( events ) )
		);
	}

	@Test
	public void stageStatelessInsert(VertxTestContext context) {
		final List<AbstractCollectionEvent> events = new ArrayList<>();
		initializeListeners( events );

		EntityA a = new EntityA();
		EntityB b = new EntityB();
		a.children.add( b );
		test( context, getSessionFactory()
				.withStatelessTransaction( statelessSession -> statelessSession
						.insert( b )
						.thenCompose( v -> statelessSession.insert( a ) )
						.thenCompose( v -> statelessSession.delete( a ) )
						.thenCompose( v -> statelessSession.delete( b ) )
				)
				.thenAccept( v -> assertEvents( events ) )
		);
	}

	@Test
	public void stageStatelessInsertAll(VertxTestContext context) {
		final List<AbstractCollectionEvent> events = new ArrayList<>();
		initializeListeners( events );

		EntityA a = new EntityA();
		EntityB b = new EntityB();
		a.children.add( b );
		test( context, getSessionFactory()
				.withStatelessTransaction( statelessSession -> statelessSession
						.insert( b, a )
						.thenCompose( v -> statelessSession.delete( a, b ) )
				)
				.thenAccept( v -> assertEvents( events ) )
		);
	}

	private static void assertEvents(List<AbstractCollectionEvent> events) {
		assertThat( events ).hasSize( 4 );
		assertThat( events.get( 0 ) )
				.isInstanceOf( PreCollectionRecreateEvent.class )
				.extracting( AbstractCollectionEvent::getAffectedOwnerEntityName ).isEqualTo( EntityA.class.getName() );
		assertThat( events.get( 1 ) )
				.isInstanceOf( PostCollectionRecreateEvent.class )
				.extracting( AbstractCollectionEvent::getAffectedOwnerEntityName ).isEqualTo( EntityA.class.getName() );
		assertThat( events.get( 2 ) )
				.isInstanceOf( PreCollectionRemoveEvent.class )
				.extracting( AbstractCollectionEvent::getAffectedOwnerEntityName ).isEqualTo( EntityA.class.getName() );
		assertThat( events.get( 3 ) )
				.isInstanceOf( PostCollectionRemoveEvent.class )
				.extracting( AbstractCollectionEvent::getAffectedOwnerEntityName ).isEqualTo( EntityA.class.getName() );
	}

	private void initializeListeners(List<AbstractCollectionEvent> events) {
		final EventListenerRegistry registry = ( (SessionFactoryImplementor) factoryManager
				.getHibernateSessionFactory() )
				.getEventListenerRegistry();

		// Clear previous listeners
		registry.getEventListenerGroup( EventType.PRE_COLLECTION_RECREATE )
				.clearListeners();
		registry.getEventListenerGroup( EventType.PRE_COLLECTION_REMOVE )
				.clearListeners();
		registry.getEventListenerGroup( EventType.POST_COLLECTION_RECREATE )
				.clearListeners();
		registry.getEventListenerGroup( EventType.POST_COLLECTION_REMOVE )
				.clearListeners();

		// Add new listeners
		registry.getEventListenerGroup( EventType.PRE_COLLECTION_RECREATE )
				.appendListener( new MyPreCollectionRecreateEventListener( events ) );
		registry.getEventListenerGroup( EventType.PRE_COLLECTION_REMOVE )
				.appendListener( new MyPreCollectionRemoveEventListener( events ) );
		registry.getEventListenerGroup( EventType.POST_COLLECTION_RECREATE )
				.appendListener( new MyPostCollectionRecreateEventListener( events ) );
		registry.getEventListenerGroup( EventType.POST_COLLECTION_REMOVE )
				.appendListener( new MyPostCollectionRemoveEventListener( events ) );
	}

	@Entity
	@Table(name = "ENTITY_A")
	public static class EntityA {

		@Id
		@GeneratedValue(strategy = GenerationType.AUTO)
		@Column(name = "ID")
		Integer id;

		@OneToMany
		@JoinColumn(name = "ENTITY_A")
		Collection<EntityB> children = new ArrayList<>();
	}

	@Entity
	@Table(name = "ENTITY_B")
	public static class EntityB {
		@Id
		@GeneratedValue(strategy = GenerationType.AUTO)
		@Column(name = "ID")
		Integer id;
	}

	public static class MyPreCollectionRecreateEventListener implements PreCollectionRecreateEventListener {

		private final List<AbstractCollectionEvent> events;


		public MyPreCollectionRecreateEventListener(List<AbstractCollectionEvent> events) {
			this.events = events;
		}

		@Override
		public void onPreRecreateCollection(PreCollectionRecreateEvent event) {
			events.add( event );
		}

	}

	public static class MyPreCollectionRemoveEventListener implements PreCollectionRemoveEventListener {

		private final List<AbstractCollectionEvent> events;

		public MyPreCollectionRemoveEventListener(List<AbstractCollectionEvent> events) {
			this.events = events;
		}

		@Override
		public void onPreRemoveCollection(PreCollectionRemoveEvent event) {
			events.add( event );
		}

	}

	public static class MyPostCollectionRecreateEventListener implements PostCollectionRecreateEventListener {

		private final List<AbstractCollectionEvent> events;

		public MyPostCollectionRecreateEventListener(List<AbstractCollectionEvent> events) {
			this.events = events;
		}

		@Override
		public void onPostRecreateCollection(PostCollectionRecreateEvent event) {
			events.add( event );
		}
	}

	public static class MyPostCollectionRemoveEventListener implements PostCollectionRemoveEventListener {

		private final List<AbstractCollectionEvent> events;

		public MyPostCollectionRemoveEventListener(List<AbstractCollectionEvent> events) {
			this.events = events;
		}

		@Override
		public void onPostRemoveCollection(PostCollectionRemoveEvent event) {
			events.add( event );
		}
	}
}
