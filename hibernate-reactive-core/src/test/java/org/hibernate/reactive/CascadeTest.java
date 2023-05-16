/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.hibernate.Hibernate;
import org.hibernate.cfg.Configuration;

import org.junit.jupiter.api.Assertions;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class CascadeTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Node.class );
		configuration.addAnnotatedClass( Element.class );
		return configuration;
	}

	@Test
	public void testQuery(VertxTestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test( context,
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createQuery("select distinct n from Node n left join fetch n.elements").getResultList())
						.thenAccept( list -> assertEquals( list.size(), 2 ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createQuery("select distinct n, e from Node n join n.elements e").getResultList())
						.thenAccept( list -> assertEquals( list.size(), 3 ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createQuery("select distinct n.id, e.id from Node n join n.elements e").getResultList())
						.thenAccept( list -> assertEquals( list.size(), 3 ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createQuery("select max(e.id), min(e.id), sum(e.id) from Node n join n.elements e group by n.id order by n.id").getResultList())
						.thenAccept( list -> assertEquals( list.size(), 1 ) )
		);
	}

	@Test
	public void testCascade(VertxTestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add( new Element(basik) );
		basik.elements.add( new Element(basik) );
		basik.elements.add( new Element(basik) );

		test( context,
				openSession()
						.thenCompose(s -> s.persist(basik)
								.thenApply(v -> { assertTrue(basik.prePersisted && !basik.postPersisted); return s; } )
								.thenApply(v -> { assertTrue(basik.parent.prePersisted && !basik.parent.postPersisted); return s; } )
								.thenCompose(v -> s.flush())
								.thenApply(v -> { assertTrue(basik.prePersisted && basik.postPersisted); return s; } )
								.thenApply(v -> { assertTrue(basik.parent.prePersisted && basik.parent.postPersisted); return s; } )
						)
						.thenCompose( v -> openSession() )
						.thenCompose(s2 -> s2.find( Node.class, basik.getId() )
								.thenCompose( node -> {
									assertNotNull( node );
									assertTrue( node.loaded );
									assertEquals( node.string, basik.string);
									assertEquals( node.version, 0 );
									assertEquals( node.elements.size(), basik.elements.size() );

									node.string = "Adopted";
									node.parent = new Node("New Parent");
									return s2.flush()
											.thenAccept(v -> {
												assertNotNull( node );
												assertTrue( node.postUpdated && node.preUpdated );
												assertFalse( node.postPersisted && node.prePersisted );
												assertTrue( node.parent.postPersisted && node.parent.prePersisted );
												assertEquals( node.version, 1 );
											});
								}))
						.thenCompose( v -> openSession() )
						.thenCompose(s2 -> s2.find( Node.class, basik.getId() )
								.thenCompose( node -> {
									assertNotNull( node );
									assertEquals( node.version, 1 );
									assertEquals( node.string, "Adopted");
									assertTrue(Hibernate.isInitialized(node.elements));
									assertFalse(Hibernate.isInitialized(node.parent));
									return s2.fetch( node.parent )
											.thenCompose( parent -> {
												assertNotNull( parent );
												return s2.createQuery("update Node set string = upper(string)").executeUpdate()
														.thenCompose(v -> s2.refresh(node))
														.thenAccept(v -> {
															assertEquals( node.getString(), "ADOPTED" );
															assertEquals( parent.getString(), "NEW PARENT" );
															assertTrue( Hibernate.isInitialized( node.elements ) );
															assertTrue( Hibernate.isInitialized( parent.elements ) );
														});
											});
								}))
						.thenCompose( v -> openSession() )
						.thenCompose(s3 -> s3.find( Node.class, basik.getId() )
								.thenCompose( node -> {
									assertFalse( node.postUpdated && node.preUpdated );
									assertFalse( node.postPersisted && node.prePersisted );
									assertEquals( node.version, 1 );
									assertEquals( node.string, "ADOPTED");
									basik.version = node.version;
									basik.string = "Hello World!";
									basik.parent.string = "Goodbye World!";
									return s3.merge(basik)
											.thenAccept( b -> {
												assertEquals( b.string, "Hello World!");
												assertEquals( b.parent.string, "Goodbye World!");
											})
											.thenCompose(v -> s3.remove(node))
											.thenAccept(v -> assertTrue( !node.postRemoved && node.preRemoved ) )
											.thenCompose(v -> s3.flush())
											.thenAccept(v -> assertTrue( node.postRemoved && node.preRemoved ) );
								}))
						.thenCompose( v -> openSession() )
						.thenCompose(s4 -> s4.find( Node.class, basik.getId() )
								.thenAccept( Assertions::assertNull))
		);
	}

	@Entity(name = "Element") @Table(name="Element")
	public static class Element {
		@Id @GeneratedValue Integer id;

		@ManyToOne
		Node node;

		public Element(Node node) {
			this.node = node;
		}

		Element() {}
	}

	@Entity(name = "Node") @Table(name="Node")
	public static class Node {

		@Id @GeneratedValue Integer id;
		@Version Integer version;
		String string;

		@ManyToOne(fetch = FetchType.LAZY,
				cascade = {CascadeType.PERSIST,
						CascadeType.REFRESH,
						CascadeType.MERGE,
						CascadeType.REMOVE})
		Node parent;

		@OneToMany(fetch = FetchType.EAGER,
				cascade = {CascadeType.PERSIST,
						CascadeType.REMOVE},
				mappedBy = "node")
		List<Element> elements = new ArrayList<>();

		@Transient boolean prePersisted;
		@Transient boolean postPersisted;
		@Transient boolean preUpdated;
		@Transient boolean postUpdated;
		@Transient boolean postRemoved;
		@Transient boolean preRemoved;
		@Transient boolean loaded;

		public Node(String string) {
			this.string = string;
		}

		Node() {}

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

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
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
			return Objects.equals(string, node.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
