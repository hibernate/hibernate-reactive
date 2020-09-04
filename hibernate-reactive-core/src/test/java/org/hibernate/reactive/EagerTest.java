/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.cfg.Configuration;
import org.junit.Ignore;
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

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class EagerTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addPackage(this.getClass().getPackage().getName());
		configuration.addAnnotatedClass(Node.class);
		configuration.addAnnotatedClass(Element.class);
		return configuration;
	}

	@Test
	public void testEagerCollectionFetch(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenApply(v -> openSession())
						.thenCompose(s -> s.find( Node.class, basik.getId() ))
						.thenAccept( node -> {
							context.assertTrue( Hibernate.isInitialized( node.elements ) );
							context.assertEquals( 3, node.elements.size() );
							for ( Element element : node.elements ) {
								context.assertTrue( element.node == node );
							}
						} )
		);
	}

	@Test
	public void testEagerParentFetch(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenApply(v -> openSession())
						.thenCompose(s -> s.find( Element.class, basik.elements.get(0).id ))
						.thenAccept( element -> {
							context.assertTrue( Hibernate.isInitialized( element.node ) );
							context.assertTrue( Hibernate.isInitialized( element.node.elements ) );
							context.assertEquals( 3, element.node.elements.size() );
						} )
		);
	}

	@Test @Ignore
	public void testEagerParentFetchQuery(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenApply(v -> openSession())
						.thenCompose(s -> s.createQuery( "from Element", Element.class ).getResultList())
						.thenAccept( elements -> {
							for (Element element: elements) {
								context.assertTrue( Hibernate.isInitialized( element.node ) );
								context.assertTrue( Hibernate.isInitialized( element.node.elements ) );
								context.assertEquals( 3, element.node.elements.size() );
							}
						} )
		);
	}

	@Test
	public void testEagerFetchQuery(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenApply(v -> openSession())
						.thenCompose(s -> s.createQuery("from Node order by id", Node.class).getResultList())
						.thenAccept(list -> {
							context.assertEquals(list.size(), 2);
							context.assertTrue( Hibernate.isInitialized( list.get(0).elements ) );
							context.assertEquals(list.get(0).elements.size(), 3);
							context.assertEquals(list.get(1).elements.size(), 0);
						})
						.thenApply(v -> openSession())
						.thenCompose(s -> s.createQuery("select distinct n, e from Node n join n.elements e order by n.id").getResultList())
						.thenAccept(list -> {
							context.assertEquals(list.size(), 3);
							Object[] tup = (Object[]) list.get(0);
							context.assertTrue( Hibernate.isInitialized( ((Node) tup[0]).elements ) );
							context.assertEquals(((Node) tup[0]).elements.size(), 3);
						})
		);
	}

	@Entity(name = "Element")
	@Table(name = "Element")
	public static class Element {
		@Id
		@GeneratedValue
		Integer id;

		@ManyToOne
		@Fetch(FetchMode.SELECT)
		Node node;

		public Element(Node node) {
			this.node = node;
		}

		Element() {
		}
	}

	@Entity(name = "Node")
	@Table(name = "Node")
	public static class Node {

		@Id
		@GeneratedValue
		Integer id;
		@Version
		Integer version;
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
		@Fetch(FetchMode.SELECT)
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
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
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
