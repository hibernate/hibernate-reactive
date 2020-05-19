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
	public void testPersist(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(b))
						.thenApply( s -> {
							bId = b.id;
							return s;
						})
						.thenCompose(s -> s.flush())
						.thenCompose(ignore -> check( openSession(), context ))
		);
	}

	@Test
	public void testMergeTransient(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose(s -> s.merge(b)
								.thenApply(bMerged -> {
									this.bId = bMerged.id;
									return s;
								})
						)
						.thenCompose(s -> s.flush())
						.thenCompose(v -> check(openSession(), context))
		);
	}

	@Test
	public void testMergeDetachedAssociationsInitialized(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(b))
						.thenApply( s -> {
							bId = b.id;
							return s;
						})
						.thenCompose(s -> s.flush())
						.thenCompose(ignore -> openSession()
								.thenCompose(s2 -> s2.find(B.class, bId)
										.thenApply(bFound -> {
											s2.clear();
											s2.merge( bFound );
											return s2;
										}))
						)
						.thenCompose(v -> check(openSession(), context))
		);
	}

	@Test
	public void testMergeDetachedAssociationsUninitialized(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(b))
						.thenApply( s -> {
							bId = b.id;
							return s;
						})
						.thenCompose(s -> s.flush())
						.thenCompose(ignore -> openSession()
								.thenCompose(s2 -> s2.merge(b))
						)
						.thenCompose(v -> check(openSession(), context))
		);
	}

	@Test
	public void testRemove(TestContext context) {

		test(
				context,
				openSession()
						.thenCompose(s -> s.persist(b))
						.thenApply( s -> {
							bId = b.id;
							return s;
						})
						.thenCompose(s -> s.flush())
						.thenCompose(ignore -> check( openSession(), context ))
						.thenAccept(ignore -> {
							// Cascade-remove is not configured, so remove all associations.
							// Everything will need to be merged, then deleted in the proper order
							prepareEntitiesForDelete();
						})
						.thenCompose(v -> openSession())
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
					context.assertFalse(Hibernate.isInitialized(bCheck.getC()));
					context.assertFalse(Hibernate.isInitialized(bCheck.getD()));
					context.assertFalse(Hibernate.isInitialized(bCheck.getgCollection()));
					return bCheck;
				})
				.thenCompose(bCheck -> sCheck.fetch(bCheck.c)
						.thenApply(cCheck -> {
							context.assertEquals(c, cCheck);
							context.assertEquals(c, bCheck.getC());
							// TODO: following is fails; is that expected?
							//context.assertTrue(cCheck == bCheck.getC());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.d)
						.thenApply(dCheck -> {
							context.assertEquals(d, bCheck.getD());
							context.assertEquals(d, dCheck);
							context.assertTrue(Hibernate.isInitialized(bCheck.getD().getC()));
							// TODO: following is fails; is that expected?
							//context.assertTrue(bCheck.getC() == bCheck.getD().getC());
							context.assertFalse(Hibernate.isInitialized(bCheck.getD().getE()));
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE())
						.thenApply(eCheck -> {
							context.assertEquals(e, bCheck.getD().getE());
							context.assertEquals(e, eCheck);
							context.assertFalse(Hibernate.isInitialized(bCheck.getD().getE().getF()));
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getF())
						.thenApply(fCheck -> {
							context.assertEquals(f, bCheck.getD().getE().getF());
							context.assertEquals(f, fCheck);
							context.assertFalse(Hibernate.isInitialized(bCheck.getD().getE().getF().getG()));
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getF().getG())
						.thenApply(gCheck -> {
							context.assertEquals(g, bCheck.getD().getE().getF().getG());
							context.assertEquals(g, gCheck);
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getgCollection())
						.thenApply(gCollectionCheck -> {
								final G gElement = gCollectionCheck.iterator().next();
								context.assertEquals(g, gElement);
								context.assertTrue(bCheck.getD().getE().getF().getG() == gElement);
								context.assertTrue(bCheck == gElement.getB());
								return bCheck;
							}
						)
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getC().getbCollection())
						.thenApply(bCollectionCheck ->
								context.assertTrue(bCheck == bCollectionCheck.iterator().next()))
						.thenApply(v -> bCheck)
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getC().getdCollection())
						.thenApply(dCollectionCheck -> {
							context.assertTrue(bCheck.getD() == dCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getbCollection())
						.thenApply(bCollectionCheck -> {
							context.assertTrue(bCheck == bCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getfCollection())
						.thenApply(fCollectionCheck -> {
							final F fElement = fCollectionCheck.iterator().next();
							context.assertEquals(f, fElement);
							context.assertTrue(bCheck.getD().getE().getF() == fElement);
							context.assertTrue(bCheck.getD() == fElement.getD());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getdCollection())
						.thenApply(dCollectionCheck -> {
							context.assertTrue(bCheck.getD() == dCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getF().geteCollection())
						.thenApply(eCollectionCheck -> {
							context.assertTrue(bCheck.getD().getE() == eCollectionCheck.iterator().next());
							return bCheck;
						})
				)
				.thenCompose(bCheck -> sCheck.fetch(bCheck.getD().getE().getF().getG().getfCollection())
						.thenApply(fCollectionCheck -> {
							final F fElement = fCollectionCheck.iterator().next();
							context.assertTrue(bCheck.getD().getE().getF() == fElement);
							return bCheck;
						})
				)
		);
	}

	@Override
	@Before
	public void before() {
		super.before();
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
		private java.util.Set<G> gCollection = new java.util.HashSet<G>();


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
		private Set<B> bCollection = new java.util.HashSet<B>();

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH}
				, mappedBy = "c")
		private Set<D> dCollection = new java.util.HashSet<D>();

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
		private java.util.Set<B> bCollection = new java.util.HashSet<B>();

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		private C c;

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		private E e;

		@OneToMany(cascade =  {
				CascadeType.MERGE, CascadeType.PERSIST, CascadeType.REFRESH},
				mappedBy = "d"
		)
		private java.util.Set<F> fCollection = new java.util.HashSet<F>();

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
		private java.util.Set<D> dCollection = new java.util.HashSet<D>();

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
		private java.util.Set<E> eCollection = new java.util.HashSet<E>();

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
		private java.util.Set<F> fCollection = new java.util.HashSet<F>();

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
