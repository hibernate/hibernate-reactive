/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.util.impl.CompletionStages;

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
				.withTransaction( s -> s.createMutationQuery( "delete from Element" ).executeUpdate()
						.thenCompose( v -> s.createMutationQuery( "delete from Node" ).executeUpdate() )
						.thenCompose( CompletionStages::voidFuture ) );
	}

	@Test
	public void testEagerCollectionFetch(VertxTestContext context) {

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
							assertThat( Hibernate.isInitialized( node.getElements() ) ).isTrue();
							assertThat( node.getElements() ).hasSize( 3 );
							for ( Element element : node.getElements() ) {
								assertThat( element.getNode() ).isSameAs( node );
							}
						} )
		);
	}

	@Test
	public void testEagerParentFetch(VertxTestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.find( Element.class, basik.getElements().get(0).getId() ))
						.thenAccept( element -> {
							assertThat( Hibernate.isInitialized( element.getNode() ) ).isTrue();
							assertThat( Hibernate.isInitialized( element.getNode().getElements() ) ).isTrue();
							assertThat( element.getNode().getElements() ).hasSize( 3 );
						} )
		);
	}

	@Test
	public void testEagerParentFetchQuery(VertxTestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.createSelectionQuery( "from Element", Element.class ).getResultList())
						.thenAccept( elements -> {
							for (Element element: elements) {
								assertThat( Hibernate.isInitialized( element.getNode() ) ).isTrue();
								assertThat( Hibernate.isInitialized( element.getNode().getElements() ) ).isTrue();
								assertThat( element.getNode().getElements() ).hasSize( 3 );
							}
						} )
		);
	}

	@Test
	public void testEagerFetchQuery(VertxTestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));
		basik.elements.add(new Element(basik));

		test(context,
				openSession()
						.thenCompose(s -> s.persist(basik).thenCompose(v -> s.flush()))
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.createSelectionQuery("from Node order by id", Node.class).getResultList())
						.thenAccept(list -> {
							assertThat( list ).hasSize( 2 );
							assertThat( Hibernate.isInitialized( list.get(0).getElements() ) ).isTrue();
							assertThat( list.get(0).getElements() ).hasSize( 3 );
							assertThat( list.get(1).getElements() ).hasSize( 0 );
						})
						.thenCompose( v -> openSession() )
						.thenCompose(s -> s.createSelectionQuery("select distinct n, e from Node n join n.elements e order by n.id", Object[].class).getResultList())
						.thenAccept(list -> {
							assertThat( list ).hasSize( 3 );
							Object[] tup = list.get(0);
							assertThat( Hibernate.isInitialized( ((Node) tup[0]).getElements() ) ).isTrue();
							assertThat( ((Node) tup[0]).getElements() ).hasSize( 3 );
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

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
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

		public Integer getVersion() {
			return version;
		}

		public void setVersion(Integer version) {
			this.version = version;
		}

		public Node getParent() {
			return parent;
		}

		public void setParent(Node parent) {
			this.parent = parent;
		}

		public List<Element> getElements() {
			return elements;
		}

		public void setElements(List<Element> elements) {
			this.elements = elements;
		}

		public boolean isPrePersisted() {
			return prePersisted;
		}

		public void setPrePersisted(boolean prePersisted) {
			this.prePersisted = prePersisted;
		}

		public boolean isPostPersisted() {
			return postPersisted;
		}

		public void setPostPersisted(boolean postPersisted) {
			this.postPersisted = postPersisted;
		}

		public boolean isPreUpdated() {
			return preUpdated;
		}

		public void setPreUpdated(boolean preUpdated) {
			this.preUpdated = preUpdated;
		}

		public boolean isPostUpdated() {
			return postUpdated;
		}

		public void setPostUpdated(boolean postUpdated) {
			this.postUpdated = postUpdated;
		}

		public boolean isPostRemoved() {
			return postRemoved;
		}

		public void setPostRemoved(boolean postRemoved) {
			this.postRemoved = postRemoved;
		}

		public boolean isPreRemoved() {
			return preRemoved;
		}

		public void setPreRemoved(boolean preRemoved) {
			this.preRemoved = preRemoved;
		}

		public boolean isLoaded() {
			return loaded;
		}

		public void setLoaded(boolean loaded) {
			this.loaded = loaded;
		}
	}
}
