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

import org.hibernate.annotations.Filter;
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

public class FilterTest extends BaseReactiveTest {

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
	public void testFilter(VertxTestContext context) {
		Node basik = new Node( "Child" );
		basik.parent = new Node( "Parent" );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.get( 0 ).deleted = true;

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( basik ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> {
									s.enableFilter( "current" );
									return s.createSelectionQuery( "select distinct n from Node n left join fetch n.elements order by n.id", Node.class )
											.setComment( "Hello World!" )
											.getResultList();
								} ) )
						.thenAccept( list -> {
							assertThat( list ).hasSize( 2 );
							assertThat( list.get( 0 ).elements ).hasSize( 2 );
						} )
						.thenCompose( v -> openSession()
								.thenCompose( s -> {
									s.enableFilter( "current" );
									return s.createSelectionQuery( "select distinct n, e from Node n join n.elements e", Object[].class )
											.getResultList();
								} ) )
						.thenAccept( list -> assertThat( list ).hasSize( 2 ) )
		);
	}

	@Test
	public void testFilterWithParameter(VertxTestContext context) {

		Node basik = new Node( "Child" );
		basik.region = "oceania";
		basik.parent = new Node( "Parent" );
		basik.parent.region = "oceania";
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.get( 0 ).region = "asia";
		basik.elements.get( 1 ).region = "oceania";
		basik.elements.get( 2 ).region = "oceania";

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( basik ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> {
									s.enableFilter( "region" ).setParameter( "region", "oceania" );
									return s.createSelectionQuery(
													"select distinct n from Node n left join fetch n.elements order by n.id", Node.class )
											.setComment( "Hello World!" )
											.getResultList();
								} ) )
						.thenAccept( list -> {
							assertThat( list ).hasSize( 2 );
							assertThat( list.get( 0 ).elements ).hasSize( 2 );
						} )
						.thenCompose( v -> openSession()
								.thenCompose( s -> {
									s.enableFilter( "region" ).setParameter( "region", "oceania" );
									return s.createSelectionQuery( "select distinct n, e from Node n join n.elements e", Object[].class )
											.getResultList();
								} ) )
						.thenAccept( list -> assertThat( list ).hasSize( 2 ) )
		);
	}

	@Test
	public void testFilterCollectionFetch(VertxTestContext context) {

		Node basik = new Node( "Child" );
		basik.parent = new Node( "Parent" );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.get( 0 ).deleted = true;

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( basik ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> {
									s.enableFilter( "current" );
									return s.find( Node.class, basik.getId() )
											.thenCompose( node -> s.fetch( node.elements ) );
								} ) )
						.thenAccept( list -> assertThat( list ).hasSize( 2 ) )
		);
	}

	@Test
	public void testFilterCollectionFetchWithParameter(VertxTestContext context) {

		Node basik = new Node( "Child" );
		basik.region = "oceania";
		basik.parent = new Node( "Parent" );
		basik.parent.region = "oceania";
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.get( 0 ).region = "asia";
		basik.elements.get( 1 ).region = "oceania";
		basik.elements.get( 2 ).region = "oceania";

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( basik ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession()
								.thenCompose( s -> {
									s.enableFilter( "region" ).setParameter( "region", "oceania" );
									return s.find( Node.class, basik.getId() )
											.thenCompose( node -> s.fetch( node.elements ) );
								} ) )
						.thenAccept( list -> assertThat( list ).hasSize( 2 ) )
		);
	}

	@Entity(name = "Element")
	@Table(name = "Element")
	@Filter(name = "current")
	@Filter(name = "region")
	public static class Element {
		@Id
		@GeneratedValue
		Integer id;

		@ManyToOne
		Node node;

		boolean deleted;

		String region;

		public Element(Node node) {
			this.node = node;
		}

		Element() {
		}
	}

	@Entity(name = "Node")
	@Table(name = "Node")
	@Filter(name = "current")
	@Filter(name = "region")
	public static class Node {

		@Id
		@GeneratedValue
		Integer id;
		@Version
		Integer version;
		String string;

		boolean deleted;

		String region;

		@ManyToOne(fetch = FetchType.LAZY,
				cascade = {
						CascadeType.PERSIST,
						CascadeType.REFRESH,
						CascadeType.MERGE,
						CascadeType.REMOVE
				})
		Node parent;

		@OneToMany(fetch = FetchType.LAZY,
				cascade = {
						CascadeType.PERSIST,
						CascadeType.REMOVE
				},
				mappedBy = "node")
		@Filter(name = "current")
		@Filter(name = "region")
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
	}
}
