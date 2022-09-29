/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.Test;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;


public class EagerTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Element.class, Node.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addPackage( this.getClass().getPackage().getName() );
		return configuration;
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		return getSessionFactory()
				.withTransaction( s -> s.createQuery( "delete from Element" ).executeUpdate()
						.thenCompose( v -> s.createQuery( "delete from Node" ).executeUpdate() )
						.thenCompose( CompletionStages::voidFuture ) );
	}

	@Test
	public void testEagerCollectionFetch(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
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
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find( Element.class, basik.elements.get(0).id ))
						.thenAccept( element -> {
							context.assertTrue( Hibernate.isInitialized( element.node ) );
							context.assertTrue( Hibernate.isInitialized( element.node.elements ) );
							context.assertEquals( 3, element.node.elements.size() );
						} )
		);
	}

	@Test
	public void testEagerParentFetchQuery(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
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
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.createQuery("from Node order by id", Node.class).getResultList())
						.thenAccept(list -> {
							context.assertEquals(list.size(), 2);
							context.assertTrue( Hibernate.isInitialized( list.get(0).elements ) );
							context.assertEquals(list.get(0).elements.size(), 3);
							context.assertEquals(list.get(1).elements.size(), 0);
						})
						.thenCompose( v -> openSession() )
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
