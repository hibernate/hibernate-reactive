/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.annotations.BatchSize;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostRemove;
import jakarta.persistence.PostUpdate;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreRemove;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BatchFetchTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Element.class, Node.class );
	}

	@AfterEach
	public void cleanDb(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( s -> s.createQuery( "delete from Element" ).executeUpdate()
						.thenCompose( v -> s.createQuery( "delete from Node" ).executeUpdate() ) ) );
	}

	@Test
	@Disabled// See https://github.com/hibernate/hibernate-reactive/issues/1501
	public void testQuery(VertxTestContext context) {
		Node basik = new Node( "Child" );
		basik.parent = new Node( "Parent" );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.parent.elements.add( new Element( basik.parent ) );
		basik.parent.elements.add( new Element( basik.parent ) );

		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( basik ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.createQuery( "from Node n order by id", Node.class )
						.getResultList()
						.thenCompose( list -> {
							assertEquals( list.size(), 2 );
							Node n1 = list.get( 0 );
							Node n2 = list.get( 1 );
							assertFalse( Hibernate.isInitialized( n1.getElements() ), "'n1.elements' should not be initialize" );
							assertFalse( Hibernate.isInitialized( n2.getElements() ), "'n2.elements' should not be initialize" );
							return s.fetch( n1.getElements() )
									.thenAccept( elements -> {
										assertTrue( Hibernate.isInitialized( elements ), "'elements' after fetch should not be initialize" );
										assertTrue( Hibernate.isInitialized( n1.getElements() ), "'n1.elements' after fetch should be initialize" );
										assertTrue( Hibernate.isInitialized( n2.getElements() ), "'n2.elements' after fetch should be initialize" );
									} );
						} )
				)
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.createQuery( "from Element e order by id", Element.class )
						.getResultList()
						.thenCompose( list -> {
							assertEquals( list.size(), 5 );
							list.forEach( element -> assertFalse( Hibernate.isInitialized( element.node ) ) );
							list.forEach( element -> assertEquals( s.getLockMode( element.node ), LockMode.NONE ) );
							return s.fetch( list.get( 0 ).node )
									.thenAccept( node -> {
										assertTrue( Hibernate.isInitialized( node ) );
										//TODO: I would like to assert that they're all initialized
										//      but apparently it doesn't set the proxies to init'd
										//      so check the LockMode as a workaround
										list.forEach( element -> assertEquals(
												s.getLockMode( element.node ),
												LockMode.READ
										) );
									} );
						} )
				)
		);
	}

	@Test
	public void testBatchLoad(VertxTestContext context) {
		Node basik = new Node( "Child" );
		basik.parent = new Node( "Parent" );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.parent.elements.add( new Element( basik.parent ) );
		basik.parent.elements.add( new Element( basik.parent ) );

		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( basik ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.find( Element.class, basik.elements.get( 1 ).id, basik.elements.get( 2 ).id, basik.elements.get( 0 ).id ) )
				.thenAccept( elements -> {
					assertFalse( elements.isEmpty() );
					assertEquals( 3, elements.size() );
					assertEquals( basik.elements.get( 1 ).id, elements.get( 0 ).id );
					assertEquals( basik.elements.get( 2 ).id, elements.get( 1 ).id );
					assertEquals( basik.elements.get( 0 ).id, elements.get( 2 ).id );
				} )
		);
	}

	// If it's only one collection a different method is called
	// See ReactivePaddedBatchingCollectionInitializer#reactiveInitialize
	@Test
	public void testWithCollection(VertxTestContext context) {
		Node node = new Node( "Child" );
		node.elements.add( new Element( node ) );
		node.elements.add( new Element( node ) );
		node.elements.add( new Element( node ) );

		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( node ) )
				.thenCompose( v -> getSessionFactory()
						.withTransaction( s -> s
								.createQuery( "from Node n order by id", Node.class )
								.getResultList()
								.thenCompose( list -> {
									assertEquals( list.size(), 1 );
									Node n1 = list.get( 0 );
									assertFalse( Hibernate.isInitialized( n1.elements ) );
									return s.fetch( n1.elements ).thenAccept( elements -> {
										assertTrue( Hibernate.isInitialized( elements ) );
										assertTrue( Hibernate.isInitialized( n1.elements ) );
									} );
								} )
						)
				)
		);
	}

	@Entity(name = "Element")
	@Table(name = "Element")
	public static class Element {
		@Id
		@GeneratedValue
		Integer id;

		@ManyToOne(fetch = FetchType.LAZY)
		Node node;

		public Element(Node node) {
			this.node = node;
		}

		Element() {
		}

		public Node getNode() {
			return node;
		}

		public void setNode(Node node) {
			this.node = node;
		}
	}

	@Entity(name = "Node")
	@Table(name = "Node")
	@BatchSize(size = 5)
	public static class Node {

		@Id
		@GeneratedValue
		Integer id;
		@Version
		Integer version;
		String string;

		@ManyToOne(fetch = FetchType.LAZY, cascade = { CascadeType.PERSIST, CascadeType.REFRESH, CascadeType.MERGE, CascadeType.REMOVE })
		Node parent;

		@OneToMany(fetch = FetchType.LAZY, cascade = { CascadeType.PERSIST, CascadeType.REMOVE }, mappedBy = "node")
		@BatchSize(size = 5)
		List<Element> elements = new ArrayList<>();

		@Transient
		boolean prePersisted;
		@Transient
		boolean postPersisted;
		@Transient
		boolean preUpdated;
		@Transient
		boolean postUpdated;
		@Transient
		boolean postRemoved;
		@Transient
		boolean preRemoved;
		@Transient
		boolean loaded;

		public Node(String string) {
			this.string = string;
		}

		Node() {
		}

		@PrePersist
		void prePersist() {
			prePersisted = true;
		}

		@PostPersist
		void postPersist() {
			postPersisted = true;
		}

		@PreUpdate
		void preUpdate() {
			preUpdated = true;
		}

		@PostUpdate
		void postUpdate() {
			postUpdated = true;
		}

		@PreRemove
		void preRemove() {
			preRemoved = true;
		}

		@PostRemove
		void postRemove() {
			postRemoved = true;
		}

		@PostLoad
		void postLoad() {
			loaded = true;
		}

		public List<Element> getElements() {
			return elements;
		}

		public void setElements(List<Element> elements) {
			this.elements = elements;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Node node = (Node) o;
			return Objects.equals( string, node.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}
}
