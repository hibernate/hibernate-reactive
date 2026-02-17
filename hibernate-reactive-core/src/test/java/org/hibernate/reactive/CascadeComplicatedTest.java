/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.cfg.Configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.OneToMany;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test uses a complicated model that requires Hibernate to delay
 * inserts until non-nullable transient entity dependencies are resolved.
 *
 * All IDs are generated from a sequence.
 *
 * JPA cascade types are used (jakarta.persistence.CascadeType).
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
 * All associations are lazy.
 *
 * b, c, d, e, f, and g are all transient unsaved that are associated with each other.
 *
 * When persisting b with ORM, the entities are added to the ActionQueue in the following order:
 * c, d (depends on e), f (depends on d, g), e, b, g.
 *
 * Entities are inserted in the following order:
 * c, e, d, b, g, f.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class CascadeComplicatedTest extends BaseReactiveTest {

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
	public void testPersist(VertxTestContext context) {
		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(b)
								.thenApply(v -> bId = b.id)
								.thenCompose(v -> s.flush())
								.thenCompose(v -> s.close())
						)
						.thenCompose(ignore -> check( context ))
		);
	}

	@Test
	public void testMergeTransient(VertxTestContext context) {
		test(
				context,
				openSession()
						.thenCompose(s -> s.merge(b)
								.thenAccept(bMerged -> bId = bMerged.id)
								.thenCompose(v -> s.flush())
								.thenCompose(v -> s.close())
								.thenCompose(v -> check(context)))
		);
	}

	@Test
	public void testMergeDetachedAssociationsInitialized(VertxTestContext context) {
		test(
				context,
				getSessionFactory()
						.withSession(s -> s.persist(b)
								.thenAccept( v -> bId = b.id )
								.thenCompose(v -> s.flush())
						)
						.thenCompose(ignore -> getSessionFactory()
								.withSession(s1 -> s1.find(B.class, bId))
								.thenCompose( bFound -> getSessionFactory()
										.withSession( s2 -> s2.merge( bFound )
												.thenCompose(v -> s2.flush())))
						)
						.thenCompose(v -> check(context))
		);
	}

	@Test
	public void testMergeDetachedAssociationsUninitialized(VertxTestContext context) {
		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(b)
								.thenAccept( v -> bId = b.id )
								.thenCompose(v -> s.flush())
								.thenCompose(v -> s.close())
						)
						.thenCompose(ignore -> openSession()
								.thenCompose(s2 -> s2.merge(b))
						)
						.thenCompose(v -> check(context))
		);
	}

	@Test
	public void testRemove(VertxTestContext context) {

		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(b)
								.thenAccept(v -> bId = b.id)
								.thenCompose(v -> s.flush())
								.thenCompose(v -> s.close())
						)
						.thenCompose(ignore -> check(context))
						.thenAccept(ignore -> {
							// Cascade-remove is not configured, so remove all associations.
							// Everything will need to be merged, then deleted in the proper order
							prepareEntitiesForDelete();
						})
						.thenCompose( v -> openSession() )
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
						.thenCompose( s2 -> s2.remove(f)
								.thenCompose(v -> s2.remove(g))
								.thenCompose(v -> s2.remove(b))
								.thenCompose(v -> s2.remove(d))
								.thenCompose(v -> s2.remove(e))
								.thenCompose(v -> s2.remove(c))
								.thenCompose(v -> s2.flush())
								.thenCompose(v -> s2.close())
						)
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

	private CompletionStage<Object> check(VertxTestContext context) {
		return  getSessionFactory().withSession(sCheck -> sCheck.find(B.class, bId)
				.thenApply(bCheck -> {
					assertThat(bCheck).isEqualTo(b);
					assertThat(Hibernate.isInitialized(bCheck.getC())).isFalse();
					assertThat(Hibernate.isInitialized(bCheck.getD())).isFalse();
					assertThat(Hibernate.isInitialized(bCheck.getgCollection())).isFalse();
					return bCheck;
				})
				.thenCompose(bCheck -> sCheck.fetch(bCheck.c)
						.thenApply(cCheck -> {
							assertThat(cCheck).isEqualTo(c);
							assertThat(bCheck.getC()).isEqualTo(c);
							assertThat(cCheck).isSameAs(bCheck.getC());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.d)
						.thenApply(dCheck -> {
							assertThat(bCheck.getD()).isEqualTo(d);
							assertThat(dCheck).isEqualTo(d);
							assertThat(Hibernate.isInitialized(bCheck.getD().getC())).isTrue();
							assertThat(bCheck.getC()).isSameAs(bCheck.getD().getC());
							assertThat(Hibernate.isInitialized(bCheck.getD().getE())).isFalse();
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE())
						.thenApply(eCheck -> {
							assertThat(bCheck.getD().getE()).isEqualTo(e);
							assertThat(eCheck).isEqualTo(e);
							assertThat(Hibernate.isInitialized(bCheck.getD().getE().getF())).isFalse();
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getF())
						.thenApply(fCheck -> {
							assertThat(bCheck.getD().getE().getF()).isEqualTo(f);
							assertThat(fCheck).isEqualTo(f);
							assertThat(Hibernate.isInitialized(bCheck.getD().getE().getF().getG())).isFalse();
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getF().getG())
						.thenApply(gCheck -> {
							assertThat(bCheck.getD().getE().getF().getG()).isEqualTo(g);
							assertThat(gCheck).isEqualTo(g);
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getgCollection())
						.thenApply(gCollectionCheck -> {
									final G gElement = gCollectionCheck.iterator().next();
									assertThat(gElement).isEqualTo(g);
									assertThat(bCheck.getD().getE().getF().getG()).isSameAs(gElement);
									assertThat(bCheck).isSameAs(gElement.getB());
									return bCheck;
								}
						)
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getC().getbCollection())
						.thenApply(bCollectionCheck -> {
							assertThat( bCheck ).isSameAs( bCollectionCheck.iterator().next() );
							return bCheck;
						} )
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getC().getdCollection())
						.thenApply(dCollectionCheck -> {
							assertThat(bCheck.getD()).isSameAs(dCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getbCollection())
						.thenApply(bCollectionCheck -> {
							assertThat(bCheck).isSameAs(bCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getfCollection())
						.thenApply(fCollectionCheck -> {
							final F fElement = fCollectionCheck.iterator().next();
							assertThat(fElement).isEqualTo(f);
							assertThat(bCheck.getD().getE().getF()).isSameAs(fElement);
							assertThat(bCheck.getD()).isSameAs(fElement.getD());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getdCollection())
						.thenApply(dCollectionCheck -> {
							assertThat(bCheck.getD()).isSameAs(dCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getF().geteCollection())
						.thenApply(eCollectionCheck -> {
							assertThat(bCheck.getD().getE()).isSameAs(eCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getF().getG().getfCollection())
						.thenApply(fCollectionCheck -> {
							final F fElement = fCollectionCheck.iterator().next();
							assertThat(bCheck.getD().getE().getF()).isSameAs(fElement);
							return bCheck;
						})
				)
		);
	}

	@Override
	@BeforeEach
	public void before(VertxTestContext context) {
		super.before(context);
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


		public String getUuid() {
			return uuid;
		}

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
			if (getUuid() == null) {
				if (other.getUuid() != null)
					return false;
			} else if (!getUuid().equals(other.getUuid()))
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
		private Set<G> gCollection = new java.util.HashSet<>();


		@ManyToOne(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH},
				optional = false,
				fetch = FetchType.LAZY
		)
		private C c;

		@ManyToOne(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH},
				optional = false,
				fetch = FetchType.LAZY
		)
		private D d;

		public Long getId() {
			return id;
		}

		public Set<G> getgCollection() {
			return gCollection;
		}

		public C getC() {
			return c;
		}

		public D getD() {
			return d;
		}
	}

	@Entity(name = "C")
	public static class C extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(mappedBy = "c")
		private Set<B> bCollection = new java.util.HashSet<>();

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH}
				, mappedBy = "c")
		private Set<D> dCollection = new java.util.HashSet<>();

		public Long getId() {
			return id;
		}

		public Set<B> getbCollection() {
			return bCollection;
		}

		public Set<D> getdCollection() {
			return dCollection;
		}
	}

	@Entity(name = "D")
	public static class D extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(mappedBy = "d")
		private Set<B> bCollection = new java.util.HashSet<>();

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		private C c;

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		private E e;

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH},
				mappedBy = "d"
		)
		private Set<F> fCollection = new java.util.HashSet<>();

		public Long getId() {
			return id;
		}

		public Set<B> getbCollection() {
			return bCollection;
		}

		public C getC() {
			return c;
		}

		public E getE() {
			return e;
		}

		public Set<F> getfCollection() {
			return fCollection;
		}
	}

	@Entity(name = "E")
	public static class E extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(mappedBy = "e")
		private Set<D> dCollection = new java.util.HashSet<>();

		@ManyToOne(optional = true, fetch = FetchType.LAZY)
		private F f;

		public Long getId() {
			return id;
		}

		public Set<D> getdCollection() {
			return dCollection;
		}

		public F getF() {
			return f;
		}
	}

	@Entity(name = "F")
	public static class F extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH}
				, mappedBy = "f")
		private Set<E> eCollection = new java.util.HashSet<>();

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		private D d;

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		private G g;

		public Long getId() {
			return id;
		}

		public Set<E> geteCollection() {
			return eCollection;
		}

		public D getD() {
			return d;
		}

		public G getG() {
			return g;
		}
	}

	@Entity(name = "G")
	public static class G extends AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		private Long id;

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		private B b;

		@OneToMany(mappedBy = "g")
		private Set<F> fCollection = new java.util.HashSet<>();

		public Long getId() {
			return id;
		}

		public B getB() {
			return b;
		}

		public Set<F> getfCollection() {
			return fCollection;
		}
	}
}
