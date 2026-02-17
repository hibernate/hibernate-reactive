/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

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
import static org.hibernate.Hibernate.isInitialized;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class SubselectFetchTest extends BaseReactiveTest {

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
				.thenCompose( v -> openSession() )
				.thenCompose( s -> s.createSelectionQuery( "from Node n order by id", Node.class )
						.getResultList()
						.thenCompose( list -> {
							assertThat( list ).hasSize( 2 );
							Node n1 = list.get( 0 );
							Node n2 = list.get( 1 );
							assertThat( isInitialized( n1.getElements() ) ).as( "'n1.elements' should not be initialized" ).isFalse();
							assertThat( isInitialized( n2.getElements() ) ).as( "'n2.elements' should not be initialized" ).isFalse();
							return s.fetch( n1.getElements() ).thenAccept( elements -> {
								assertThat( isInitialized( elements ) ).as( "'elements' - after fetch - should be initialized" ).isTrue();
								assertThat( isInitialized( n1.getElements() ) ).as( "'n1.elements' - after fetch - should be initialized" ).isTrue();
								assertThat( isInitialized( n2.getElements() ) ).as( "'n2.elements' - after fetch - should be initialized" ).isTrue();
							} );
						} )
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

		@ManyToOne(fetch = FetchType.LAZY, cascade = {
				CascadeType.PERSIST, CascadeType.REFRESH, CascadeType.MERGE, CascadeType.REMOVE
		})
		Node parent;

		@OneToMany(fetch = FetchType.LAZY,
				cascade = { CascadeType.PERSIST, CascadeType.REMOVE },
				mappedBy = "node")
		@Fetch(FetchMode.SUBSELECT)
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
