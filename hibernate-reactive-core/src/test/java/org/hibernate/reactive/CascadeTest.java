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
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
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

import static jakarta.persistence.CascadeType.MERGE;
import static jakarta.persistence.CascadeType.PERSIST;
import static jakarta.persistence.CascadeType.REFRESH;
import static jakarta.persistence.CascadeType.REMOVE;
import static jakarta.persistence.FetchType.EAGER;
import static jakarta.persistence.FetchType.LAZY;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.JdbcSettings.DIALECT;
import static org.hibernate.cfg.JdbcSettings.DRIVER;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 10, timeUnit = MINUTES)
public class CascadeTest extends BaseReactiveTest {

	private static SqlStatementTracker sqlTracker;
	private SessionFactory ormFactory;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Node.class );
		configuration.addAnnotatedClass( Element.class );
		return configuration;
	}

	@BeforeEach
	public void prepareOrmFactory() {
		Configuration configuration = constructConfiguration();
		configuration.setProperty( DRIVER, dbType().getJdbcDriver() );
		configuration.setProperty( DIALECT, dbType().getDialectClass().getName() );

		StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder()
				.applySettings( configuration.getProperties() );

		sqlTracker = new SqlStatementTracker( CascadeTest::filter, configuration.getProperties() );
		sqlTracker.registerService( builder );

		StandardServiceRegistry registry = builder.build();
		ormFactory = configuration.buildSessionFactory( registry );
	}

	private static boolean filter(String s) {
		return s.startsWith( "select n1_0.id,n1_0.parent_id,n1_0.s" );
	}

	@AfterEach
	public void closeOrmFactory() {
		ormFactory.close();
	}

	@Test
	public void testQuery(VertxTestContext context) {
		Node basik = new Node( "Child" );
		basik.parent = new Node( "Parent" );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( basik ).thenCompose( v -> s.flush() ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createSelectionQuery( "select distinct n from Node n left join fetch n.elements", Node.class ).getResultList() )
						.thenAccept( list -> assertEquals( list.size(), 2 ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createSelectionQuery( "select distinct n, e from Node n join n.elements e", Object[].class ).getResultList() )
						.thenAccept( list -> assertEquals( list.size(), 3 ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createSelectionQuery( "select distinct n.id, e.id from Node n join n.elements e", Object[].class ).getResultList() )
						.thenAccept( list -> assertEquals( list.size(), 3 ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createSelectionQuery( "select max(e.id), min(e.id), sum(e.id) from Node n join n.elements e group by n.id order by n.id", Object[].class ).getResultList() )
						.thenAccept( list -> assertEquals( list.size(), 1 ) )
		);
	}

	@Test
	public void testManyToOneCascadeRefresh(VertxTestContext context) {
		Node child = new Node( "child" );
		child.setParent( new Node( "parent" ) );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( child ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( s -> s
								.find( Node.class, child.getId() )
								.chain( node -> s
										.fetch( node.getParent() )
										.chain( parent -> s
												.createMutationQuery( "update Node set string = upper(string)" )
												.executeUpdate()
												.chain( () -> s.refresh( node ) )
												.invoke( () -> {
													assertThat( node.getString() ).isEqualTo( "CHILD" );
													assertThat( parent.getString() ).isEqualTo( "PARENT" );
												} ) ) ) )
				)
		);
	}

	@Test
	public void testManyToOneCascadeRefreshWithORM() {
		Node child = new Node( "child" );
		child.setParent( new Node( "parent" ) );

		try (Session s = ormFactory.openSession()) {
			s.beginTransaction();
			s.persist( child );
			s.getTransaction().commit();
		}

		try (Session s = ormFactory.openSession()) {
			s.beginTransaction();
			Node node = s.find( Node.class, child.getId() );
			Node parent = node.getParent();
			s.createMutationQuery( "update Node set string = upper(string)" ).executeUpdate();
			s.refresh( node );
			assertThat( node.getString() ).isEqualTo( "CHILD" );
			assertThat( parent.getString() ).isEqualTo( "PARENT" );
			s.getTransaction().commit();
		}
	}

	@Test
	@Ignore
	public void testCascade(VertxTestContext context) {
		Node basik = new Node( "Child" );
		basik.parent = new Node( "Parent" );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );
		basik.elements.add( new Element( basik ) );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( basik )
								.thenApply( v -> {
									assertTrue( basik.prePersisted && !basik.postPersisted );
									assertTrue( basik.parent.prePersisted && !basik.parent.postPersisted );
									return s;
								} )
								.thenCompose( v -> s.flush() )
								.thenApply( v -> {
									assertTrue( basik.prePersisted && basik.postPersisted );
									assertTrue( basik.parent.prePersisted && basik.parent.postPersisted );
									return s;
								} )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s2 -> s2.find( Node.class, basik.getId() )
								.thenCompose( node -> {
									assertNotNull( node );
									assertTrue( node.loaded );
									assertEquals( node.string, basik.string );
									assertEquals( node.version, 0 );
									assertEquals( node.elements.size(), basik.elements.size() );

									node.string = "Adopted";
									node.parent = new Node( "New Parent" );
									return s2.flush()
											.thenAccept( v -> {
												assertNotNull( node );
												assertTrue( node.postUpdated && node.preUpdated );
												assertFalse( node.postPersisted && node.prePersisted );
												assertTrue( node.parent.postPersisted && node.parent.prePersisted );
												assertEquals( node.version, 1 );
											} );
								} ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s2 -> s2.find( Node.class, basik.getId() )
								.thenCompose( node -> {
									assertNotNull( node );
									assertEquals( node.version, 1 );
									assertEquals( node.string, "Adopted" );
									assertTrue( Hibernate.isInitialized( node.elements ) );
									assertFalse( Hibernate.isInitialized( node.parent ) );
									return s2.fetch( node.parent )
											.thenCompose( parent -> {
												assertNotNull( parent );
												return s2.createMutationQuery( "update Node set string = upper(string)" )
														.executeUpdate()
														.thenCompose( v -> s2.refresh( node ) )
														.thenAccept( v -> {
															assertEquals( node.getString(), "ADOPTED" );
															assertEquals( parent.getString(), "NEW PARENT" );
															assertTrue( Hibernate.isInitialized( node.elements ) );
															assertTrue( Hibernate.isInitialized( parent.elements ) );
														} );
											} );
								} ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s3 -> s3.find( Node.class, basik.getId() )
								.thenCompose( node -> {
									assertFalse( node.postUpdated && node.preUpdated );
									assertFalse( node.postPersisted && node.prePersisted );
									assertEquals( node.version, 1 );
									assertEquals( node.string, "ADOPTED" );
									basik.version = node.version;
									basik.string = "Hello World!";
									basik.parent.string = "Goodbye World!";
									return s3.merge( basik )
											.thenAccept( b -> {
												assertEquals( b.string, "Hello World!" );
												assertEquals( b.parent.string, "Goodbye World!" );
											} )
											.thenCompose( v -> s3.remove( node ) )
											.thenAccept( v -> assertTrue( !node.postRemoved && node.preRemoved ) )
											.thenCompose( v -> s3.flush() )
											.thenAccept( v -> assertTrue( node.postRemoved && node.preRemoved ) );
								} ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s4 -> s4.find( Node.class, basik.getId() )
								.thenAccept( Assertions::assertNull ) )
		);
	}

	@Entity(name = "Element")
	@Table(name = "Element")
	public static class Element {
		@Id
		@GeneratedValue
		Integer id;

		@ManyToOne
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

		@ManyToOne(fetch = LAZY, cascade = {PERSIST, REFRESH, MERGE, REMOVE})
		Node parent;

		@OneToMany(fetch = EAGER, cascade = {PERSIST, REMOVE}, mappedBy = "node")
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
