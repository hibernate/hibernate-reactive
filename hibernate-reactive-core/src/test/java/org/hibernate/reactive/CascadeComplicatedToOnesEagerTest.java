/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.Hibernate;
import org.hibernate.reactive.stage.Stage;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.*;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * This test uses a complicated model that requires Hibernate to delay
 * inserts until non-nullable transient entity dependencies are resolved.
 *
 * All IDs are generated from a sequence.
 *
 * JPA cascade types are used (javax.persistence.CascadeType)..
 *
 * This test uses the following model:
 *
 * <code>
 *     ------------------------------ N G
 *     |
 *     |                                1
 *     |                                |
 *     |                                |
 *     |                                N
 *     |
 *     |         E N--------------0,1 * F
 *     |
 *     |         1                      N
 *     |         |                      |
 *     |         |                      |
 *     1         N                      |
 *     *                                |
 *     B * N---1 D * 1------------------
 *     *
 *     N         N
 *     |         |
 *     |         |
 *     1         |
 *               |
 *     C * 1-----
 *</code>
 *
 * In the diagram, all associations are bidirectional;
 * assocations marked with '*' cascade persist, save, merge operations to the
 * associated entities (e.g., B cascades persist to D, but D does not cascade
 * persist to B);
 *
 * All many-to-one associations are eager; all collection associations are lazy.
 *
 * b, c, d, e, f, and g are all transient unsaved that are associated with each other.
 *
 * When persisting b with ORM, the entities are added to the ActionQueue in the following order:
 * c, d (depends on e), f (depends on d, g), e, b, g.
 *
 * Entities are inserted in the following order:
 * c, e, d, b, g, f.
 */
public class CascadeComplicatedToOnesEagerTest extends BaseReactiveTest {

	private B b;
	private C c;
	private D d;
	private E e;
	private F f;
	private G g;

	private Long bId;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( B.class );
		configuration.addAnnotatedClass( C.class );
		configuration.addAnnotatedClass( D.class );
		configuration.addAnnotatedClass( E.class );
		configuration.addAnnotatedClass( F.class );
		configuration.addAnnotatedClass( G.class );
		return configuration;
	}

	@Test
	public void testPersist(TestContext context) {
		test(
				context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(b))
						.thenApply( s -> {
							bId = b.id;
							return s;
						})
						.thenCompose(s -> s.flush())
						.thenCompose(ignore -> check( completedFuture(openSession()), context ))
		);
	}

	@Test
	public void testMergeTransient(TestContext context) {
		test(
				context,
				completedFuture( openSession() )
						.thenCompose(s -> s.merge(b)
								.thenApply(bMerged -> {
									this.bId = bMerged.id;
									return s;
								})
						)
						.thenCompose(s -> s.flush())
						.thenCompose(v -> check(completedFuture(openSession()), context))
		);
	}

	@Test
	public void testMergeDetached(TestContext context) {
		test(
				context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(b))
						.thenApply( s -> {
							bId = b.id;
							return s;
						})
						.thenCompose(s -> s.flush())
						.thenCompose(ignore -> completedFuture( openSession() )
								.thenCompose(s2 -> s2.merge(b))
						)
						.thenCompose(v -> check(completedFuture(openSession()), context))
		);
	}


	@Test
	public void testRemove(TestContext context) {

		test(
				context,
				completedFuture( openSession() )
						.thenCompose(s -> s.persist(b))
						.thenApply( s -> {
							bId = b.id;
							return s;
						})
						.thenCompose(s -> s.flush())
						.thenCompose(ignore -> check( completedFuture(openSession()), context ))
						.thenAccept(ignore -> {
							// Cascade-remove is not configured, so remove all associations.
							// Everything will need to be merged, then deleted in the proper order
							prepareEntitiesForDelete();
						})
						.thenApply(v -> openSession())
						.thenCompose(s2 -> s2.merge(b).thenApply(merged -> {
							b = merged;
							return s2;
						}))
						.thenCompose(s2 -> s2.merge(c).thenApply(merged -> {
							c = merged;
							return s2;
						}))
						.thenCompose(s2 -> s2.merge(d).thenApply(merged -> {
							d = merged;
							return s2;
						}))
						.thenCompose(s2 -> s2.merge(e).thenApply(merged -> {
							e = merged;
							return s2;
						}))
						.thenCompose(s2 -> s2.merge(f).thenApply(merged -> {
							f = merged;
							return s2;
						}))
						.thenCompose(s2 -> s2.merge(g).thenApply(merged -> {
							g = merged;
							return s2;
						}))
						.thenCompose(s2 -> s2.remove(f))
						.thenCompose(s2 -> s2.remove(g))
						.thenCompose(s2 -> s2.remove(b))
						.thenCompose(s2 -> s2.remove(d))
						.thenCompose(s2 -> s2.remove(e))
						.thenCompose(s2 -> s2.remove(c))
						.thenCompose(s2 -> s2.flush())
		);
	}

	private void prepareEntitiesForDelete() {
		b.c = null;
		b.d = null;
		b.gCollection.remove(g);

		c.bCollection.remove(b);
		c.dCollection.remove(d);

		d.bCollection.remove(b);
		d.c = null;
		d.e = null;
		d.fCollection.remove(f);

		e.dCollection.remove(d);
		e.f = null;

		f.d = null;
		f.eCollection.remove(e);
		f.g = null;

		g.b = null;
		g.fCollection.remove(f);
	}

	private CompletionStage<Object> check(CompletionStage<Stage.Session> sessionStage, TestContext context) {
		return  sessionStage.thenCompose(sCheck -> sCheck.find(B.class, bId)
				.thenApply(bCheck -> {
					context.assertEquals(b, bCheck);
					context.assertTrue(Hibernate.isInitialized(bCheck.c));
					context.assertEquals(c, bCheck.c);
					context.assertTrue(Hibernate.isInitialized(bCheck.d));
					context.assertEquals(d, bCheck.d);
					context.assertTrue(bCheck.c == bCheck.d.c);
					context.assertEquals(e, bCheck.d.e);
					context.assertFalse(Hibernate.isInitialized(bCheck.gCollection));
					return bCheck;
				})
				.thenCompose(bCheck -> sCheck.fetch(bCheck.gCollection)
						.thenApply(gCollectionCheck -> {
								final G gElement = gCollectionCheck.iterator().next();
								context.assertEquals(g, gElement);
								context.assertTrue(bCheck.d.e.f.g == gElement);
								context.assertTrue(bCheck == gElement.b);
								return bCheck;
							}
						)
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.c.bCollection)
						.thenApply(bCollectionCheck ->
								context.assertTrue(bCheck == bCollectionCheck.iterator().next()))
						.thenApply(v -> bCheck)
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.c.dCollection)
						.thenApply(dCollectionCheck -> {
							context.assertTrue(bCheck.d == dCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.d.bCollection)
						.thenApply(bCollectionCheck -> {
							context.assertTrue(bCheck == bCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.d.fCollection)
						.thenApply(fCollectionCheck -> {
							final F fElement = fCollectionCheck.iterator().next();
							context.assertEquals(f, fElement);
							context.assertTrue(bCheck.d.e.f == fElement);
							context.assertTrue(bCheck.d == fElement.d);
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.d.e.dCollection)
						.thenApply(dCollectionCheck -> {
							context.assertTrue(bCheck.d == dCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.d.e.f.eCollection)
						.thenApply(eCollectionCheck -> {
							context.assertTrue(bCheck.d.e == eCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.d.e.f.g.fCollection)
						.thenApply(fCollectionCheck -> {
							final F fElement = fCollectionCheck.iterator().next();
							context.assertTrue(bCheck.d.e.f == fElement);
							return bCheck;
						})
				)
		);
	}

	@Override
	@Before
	public void before() {
		super.before();

		bId = null;

		b = new B();
		c = new C();
		d = new D();
		e = new E();
		f = new F();
		g = new G();

		b.gCollection.add( g );
		b.c = c;
		b.d = d;

		c.bCollection.add( b );
		c.dCollection.add( d );

		d.bCollection.add( b );
		d.c = c;
		d.e = e;
		d.fCollection.add( f );

		e.dCollection.add( d );
		e.f = f;

		f.eCollection.add( e );
		f.d = d;
		f.g = g;

		g.b = b;
		g.fCollection.add( f );
	}

	@MappedSuperclass
	public static class AbstractEntity {
		public AbstractEntity() {
			uuid = UUID.randomUUID().toString();
		}

		@Basic
		@Column(unique = true, updatable = false, length = 36, columnDefinition = "char(36)")
		private String uuid;

		@Override
		public int hashCode() {
			return uuid == null ? 0 : uuid.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof AbstractEntity ))
				return false;
			final AbstractEntity other = (AbstractEntity) obj;
			if (uuid == null) {
				if (other.uuid != null)
					return false;
			} else if (!uuid.equals(other.uuid))
				return false;
			return true;
		}
	}

	@Entity(name = "B")
	public static class B extends AbstractEntity{

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH}
				, mappedBy = "b")
		private java.util.Set<G> gCollection = new java.util.HashSet<G>();


		@ManyToOne(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH}
				, optional = false)
		private C c;

		@ManyToOne(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH}
				, optional = false)
		private D d;
	}

	@Entity(name = "C")
	public static class C extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(mappedBy = "c")
		private Set<B> bCollection = new java.util.HashSet<B>();

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH}
				, mappedBy = "c")
		private Set<D> dCollection = new java.util.HashSet<D>();
	}

	@Entity(name = "D")
	public static class D extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(mappedBy = "d")
		private java.util.Set<B> bCollection = new java.util.HashSet<B>();

		@ManyToOne(optional = false)
		private C c;

		@ManyToOne(optional = false)
		private E e;

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH},
				mappedBy = "d"
		)
		private java.util.Set<F> fCollection = new java.util.HashSet<F>();
	}

	@Entity(name = "E")
	public static class E extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(mappedBy = "e")
		private java.util.Set<D> dCollection = new java.util.HashSet<D>();

		@ManyToOne(optional = true)
		private F f;
	}

	@Entity(name = "F")
	public static class F extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH}
				, mappedBy = "f")
		private java.util.Set<E> eCollection = new java.util.HashSet<E>();

		@ManyToOne(optional = false)
		private D d;

		@ManyToOne(optional = false)
		private G g;
	}

	@Entity(name = "G")
	public static class G extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@ManyToOne(optional = false)
		private B b;

		@OneToMany(mappedBy = "g")
		private java.util.Set<F> fCollection = new java.util.HashSet<F>();
	}
}
