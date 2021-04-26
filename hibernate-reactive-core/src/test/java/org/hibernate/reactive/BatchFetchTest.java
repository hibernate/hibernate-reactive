/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.annotations.BatchSize;
import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Test;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.PostLoad;
import javax.persistence.PostPersist;
import javax.persistence.PostRemove;
import javax.persistence.PostUpdate;
import javax.persistence.PrePersist;
import javax.persistence.PreRemove;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class BatchFetchTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Node.class );
		configuration.addAnnotatedClass( Element.class );
		return configuration;
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.createQuery( "delete from Element" ).executeUpdate()
						.thenCompose( v -> s.createQuery( "delete from Node" ).executeUpdate() ) ) );
	}

	@Test
	public void testQuery(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.parent.elements.add(new Element(basik.parent));
		basik.parent.elements.add(new Element(basik.parent));

		test( context,
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.createQuery("from Node n order by id", Node.class)
								.getResultList()
								.thenCompose( list -> {
									context.assertEquals(list.size(), 2);
									Node n1 = list.get(0);
									Node n2 = list.get(1);
									context.assertFalse( Hibernate.isInitialized(n1.elements) );
									context.assertFalse( Hibernate.isInitialized(n2.elements) );
									return s.fetch( n1.elements ).thenAccept( elements -> {
										context.assertTrue( Hibernate.isInitialized(elements) );
										context.assertTrue( Hibernate.isInitialized(n1.elements) );
										context.assertTrue( Hibernate.isInitialized(n2.elements) );
									} );
								})
						)
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.createQuery("from Element e order by id", Element.class)
								.getResultList()
								.thenCompose( list -> {
									context.assertEquals( list.size(), 5 );
									list.forEach( element -> context.assertFalse( Hibernate.isInitialized(element.node) ) );
									list.forEach( element -> context.assertEquals(s.getLockMode(element.node), LockMode.NONE) );
									return s.fetch( list.get(0).node )
											.thenAccept( node -> {
												context.assertTrue( Hibernate.isInitialized(node) );
												//TODO: I would like to assert that they're all initialized
												//      but apparently it doesn't set the proxies to init'd
												//      so check the LockMode as a workaround
												list.forEach( element -> context.assertEquals(s.getLockMode(element.node), LockMode.READ) );
											});
								})
						)


		);
	}

	@Test
	public void testBatchLoad(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.parent.elements.add(new Element(basik.parent));
		basik.parent.elements.add(new Element(basik.parent));

		test( context,
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find(Element.class,
								basik.elements.get(1).id,
								basik.elements.get(2).id,
								basik.elements.get(0).id)
						).thenAccept(elements -> {
							context.assertFalse( elements.isEmpty() );
							context.assertEquals( 3, elements.size() );
							context.assertEquals( basik.elements.get(1).id, elements.get(0).id );
							context.assertEquals( basik.elements.get(2).id, elements.get(1).id );
							context.assertEquals( basik.elements.get(0).id, elements.get(2).id );
						})
		);
	}

	@Entity(name = "Element") @Table(name="Element")
	public static class Element {
		@Id @GeneratedValue Integer id;

		@ManyToOne(fetch = FetchType.LAZY)
		Node node;

		public Element(Node node) {
			this.node = node;
		}

		Element() {}
	}

	@Entity(name = "Node") @Table(name="Node")
	@BatchSize(size=5)
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

		@OneToMany(fetch = FetchType.LAZY,
				cascade = {CascadeType.PERSIST,
						CascadeType.REMOVE},
				mappedBy = "node")
		@BatchSize(size=5)
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
