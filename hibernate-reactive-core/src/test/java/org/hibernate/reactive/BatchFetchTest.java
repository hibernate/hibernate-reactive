/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class BatchFetchTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Element.class, Node.class );
	}

	@AfterEach
	public void cleanDb(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( s -> s.createMutationQuery( "delete from Element" ).executeUpdate()
						.thenCompose( v -> s.createMutationQuery( "delete from Node" ).executeUpdate() ) ) );
	}

	@Test
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
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.createSelectionQuery( "from Node n order by id", Node.class )
						.getResultList()
						.thenCompose( list -> {
							assertThat( list ).hasSize( 2 );
							Node n1 = list.get( 0 );
							Node n2 = list.get( 1 );
							assertThat( Hibernate.isInitialized( n1.getElements() ) ).as( "'n1.elements' should not be initialized" ).isFalse();
							assertThat( Hibernate.isInitialized( n2.getElements() ) ).as( "'n2.elements' should not be initialized" ).isFalse();
							return s.fetch( n1.getElements() )
									.thenAccept( elements -> {
										assertThat( Hibernate.isInitialized( elements ) ).as( "'elements' after fetch should not be initialize" ).isTrue();
										assertThat( Hibernate.isInitialized( n1.getElements() ) ).as( "'n1.elements' after fetch should be initialize" ).isTrue();
										assertThat( Hibernate.isInitialized( n2.getElements() ) ).as( "'n2.elements' after fetch should be initialize" ).isTrue();
									} );
						} )
				) )
				.thenCompose( v -> getSessionFactory().withTransaction( s -> s
						.createSelectionQuery( "from Element e order by id", Element.class )
						.getResultList()
						.thenCompose( list -> {
							assertThat( list ).hasSize( 5 );
							list.forEach( element -> assertThat( Hibernate.isInitialized( element.node ) ).isFalse() );
							list.forEach( element -> assertThat( s.getLockMode( element.node ) ).isEqualTo( LockMode.NONE ) );
							return s.fetch( list.get( 0 ).node )
									.thenAccept( node -> {
										assertThat( Hibernate.isInitialized( node ) ).isTrue();
										//TODO: I would like to assert that they're all initialized
										//      but apparently it doesn't set the proxies to init'd
										//      so check the LockMode as a workaround
										list.forEach( element -> assertThat( s.getLockMode( element.node ) ).isEqualTo( LockMode.READ ) );
									} );
						} )
				) )
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
					assertThat( elements ).isNotEmpty();
					assertThat( elements ).hasSize( 3 );
					assertThat( elements.get( 0 ).id ).isEqualTo( basik.elements.get( 1 ).id );
					assertThat( elements.get( 1 ).id ).isEqualTo( basik.elements.get( 2 ).id );
					assertThat( elements.get( 2 ).id ).isEqualTo( basik.elements.get( 0 ).id );
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
								.createSelectionQuery( "from Node n order by id", Node.class )
								.getResultList()
								.thenCompose( list -> {
									assertThat( list ).hasSize( 1 );
									Node n1 = list.get( 0 );
									assertThat( Hibernate.isInitialized( n1.elements ) ).isFalse();
									return s.fetch( n1.elements ).thenAccept( elements -> {
										assertThat( Hibernate.isInitialized( elements ) ).isTrue();
										assertThat( Hibernate.isInitialized( n1.elements ) ).isTrue();
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

		public Element() {
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

		public Node() {
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
