package org.hibernate.rx;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Ignore;
import org.junit.Test;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Ignore
public class CascadeTest extends BaseRxTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Node.class );
		configuration.addAnnotatedClass( Element.class );
		return configuration;
	}

	@Test
	public void testCascade(TestContext context) {

		Node basik = new Node("Child");
		basik.parent = new Node("Parent");
		basik.elements.add( new Element(basik) );
		basik.elements.add( new Element(basik) );
		basik.elements.add( new Element(basik) );

		test( context,
				openSession()
				.thenCompose(s -> s.persist(basik))
				.thenApply(s -> { context.assertTrue(basik.prePersisted && !basik.postPersisted); return s; } )
				.thenApply(s -> { context.assertTrue(basik.parent.prePersisted && !basik.parent.postPersisted); return s; } )
				.thenCompose(s -> s.flush())
				.thenApply(s -> { context.assertTrue(basik.postPersisted && basik.postPersisted); return s; } )
				.thenApply(s -> { context.assertTrue(basik.parent.prePersisted && basik.parent.postPersisted); return s; } )
				.thenCompose(v -> openSession())
				.thenCompose(s2 ->
					s2.find( Node.class, basik.getId() )
						.thenCompose( option -> {
							context.assertTrue( option.isPresent() );
							Node node = option.get();
							context.assertTrue( node.loaded );
							context.assertEquals( node.string, basik.string);
							context.assertEquals( node.version, 1 );

							node.string = "Adopted";
							node.parent = new Node("New Parent");
							return s2.flush()
									.thenAccept(v -> {
										context.assertTrue( option.isPresent() );
										context.assertTrue( node.postUpdated && node.preUpdated );
										context.assertFalse( node.postPersisted && node.prePersisted );
										context.assertTrue( node.parent.postPersisted && node.parent.prePersisted );
										context.assertEquals( node.version, 2 );
									});
						}))
						.thenCompose(v -> openSession())
						.thenCompose(s2 ->
								s2.find( Node.class, basik.getId() )
										.thenCompose( option -> {
											context.assertTrue( option.isPresent() );
											Node node = option.get();
											context.assertEquals( node.version, 2 );
											context.assertEquals( node.string, "Adopted");
											return s2.fetch( node.parent )
													.thenCompose( opt -> {
														context.assertTrue( opt.isPresent() );
														Node parent = opt.get();
														return connection().preparedQuery("update Node set string = upper(string)")
																.thenCompose(v -> s2.refresh(node))
																.thenAccept(v -> {
																	context.assertEquals( node.getString(), "ADOPTED" );
																	context.assertEquals( parent.getString(), "NEW PARENT" );
																});
													});
										}))
				.thenCompose(v -> openSession())
				.thenCompose(s3 ->
					s3.find( Node.class, basik.getId() )
						.thenCompose( option -> {
							Node node = option.get();
							context.assertFalse( node.postUpdated && node.preUpdated );
							context.assertFalse( node.postPersisted && node.prePersisted );
							context.assertEquals( node.version, 2 );
							context.assertEquals( node.string, "ADOPTED");
							basik.version = node.version;
							basik.string = "Hello World!";
							basik.parent.string = "Goodbye World!";
							return s3.merge(basik)
									.thenAccept( b -> {
										context.assertEquals( b.string, "Hello World!");
										context.assertEquals( b.parent.string, "Goodbye World!");
									})
									.thenCompose(v -> s3.remove(node))
									.thenAccept(v -> context.assertTrue( !node.postRemoved && node.preRemoved ) )
									.thenCompose(v -> s3.flush())
									.thenAccept(v -> context.assertTrue( node.postRemoved && node.preRemoved ) );
						}))
				.thenCompose(v -> openSession())
				.thenCompose(s4 ->
						s4.find( Node.class, basik.getId() )
							.thenAccept( option -> context.assertFalse( option.isPresent() ) ))
		);
	}

	@Entity @Table(name="Element")
	public static class Element {
		@Id @GeneratedValue Integer id;

		@ManyToOne
		Node node;

		public Element(Node node) {
			this.node = node;
		}

		Element() {}
	}

	@Entity @Table(name="Node")
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
		List<Element> elements = new ArrayList<Element>();

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
