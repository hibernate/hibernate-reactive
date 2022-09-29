/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.List;
import java.util.Objects;

import org.hibernate.AssertionFailure;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.reactive.common.spi.Implementor;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.stat.spi.StatisticsImplementor;

import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.QueryHint;
import jakarta.persistence.Table;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test what happens when the query is cached but the result entries aren't.
 * <p>
 * Uses an import.sql file for initialize the db and requires the Hibernate EHCache
 * dependency on the classpath at runtime.
 * </p>
 * <p>
 * When a query is cacheable, hibernate will cache the entity ids returned by the query.
 * When the query runs a second time, it will load the entities using the cached ids.
 * If the entities aren't in the cache it will load them from the db.
 * </p>
 */
public class CachedQueryResultsTest extends BaseReactiveTest {

	private static final Fruit[] FRUITS = {
			new Fruit( "Banana" ), new Fruit( "Pineapple" ), new Fruit( "Tomato" )
	};

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.getProperties().put( Settings.USE_SECOND_LEVEL_CACHE, Boolean.TRUE );
		configuration.getProperties().put( Settings.USE_QUERY_CACHE, Boolean.TRUE );
		configuration.setProperty( Environment.CACHE_REGION_FACTORY, "org.hibernate.cache.jcache.internal.JCacheRegionFactory" );
		configuration.setProperty( AvailableSettings.HBM2DDL_IMPORT_FILES, "/import-for-querycachetest.sql" );
		configuration.setProperty( "hibernate.javax.cache.provider", "org.ehcache.jsr107.EhcacheCachingProvider" );
		configuration.setProperty( "hibernate.javax.cache.uri", "/ehcache.xml" );
		configuration.addAnnotatedClass( Fruit.class );
		configuration.getProperties().put( Settings.GENERATE_STATISTICS, Boolean.FALSE );
		return configuration;
	}

	@Test
	public void testLoadFromSecondLevelCacheAndNamedQuery(TestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( CachedQueryResultsTest::findAllWithNamedQuery )
				// We need to close the session between the two findAll or the results will come from the
				// first-level cache
				.chain( () -> getMutinySessionFactory().withSession( CachedQueryResultsTest::findAllWithNamedQuery ) )
				.invoke( list -> assertThat( list ).containsExactly( FRUITS ) )
		);
	}

	@Test
	public void testLoadFromCachedQueryResultAndNamedQuery(TestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( s -> findAllWithNamedQuery( s ).chain( () -> findAllWithNamedQuery( s ) ) )
				.invoke( list -> assertThat( list ).containsExactly( FRUITS ) )
		);
	}

	private static Uni<List<Fruit>> findAllWithNamedQuery(Mutiny.Session session) {
		return session.createNamedQuery( Fruit.FIND_ALL, Fruit.class ).getResultList();
	}

	@Test
	public void testLoadFromSecondLevelCacheAndRegularQuery(TestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( CachedQueryResultsTest::findAllWithCacheableQuery )
				// We need to close the session between the two findAll or the results will come from the
				// first-level cache
				.chain( () -> getMutinySessionFactory().withSession( CachedQueryResultsTest::findAllWithCacheableQuery ) )
				.invoke( list -> assertThat( list ).containsExactly( FRUITS ) )
		);
	}

	@Test
	public void testLoadFromCachedQueryResultAndRegularQuery(TestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( s -> findAllWithCacheableQuery( s ).chain( () -> findAllWithCacheableQuery( s ) ) )
				.invoke( list -> assertThat( list ).containsExactly( FRUITS ) )
		);
	}

	@Test
	public void testQueryPlanCacheHitsGenerateStatisticsFalse(TestContext context) {
		test( context, criteriaFindAll()
				.call( CachedQueryResultsTest::criteriaFindAll )
				.call( CachedQueryResultsTest::criteriaFindAll )
				.invoke( () -> context.assertEquals( 0L, statistics().getQueryPlanCacheHitCount() ) )
		);
	}

	public static StatisticsImplementor statistics() {
		return ( (Implementor) getSessionFactory() ).getServiceRegistry().getService(
				StatisticsImplementor.class );
	}

	public static Uni<List<Fruit>> criteriaFindAll() {
		final Mutiny.SessionFactory sf = getMutinySessionFactory();
		return sf.withStatelessSession( s -> s.createQuery( criteriaQuery( sf.getCriteriaBuilder() ) ).getResultList()
		);
	}

	public static CriteriaQuery<Fruit> criteriaQuery(CriteriaBuilder criteriaBuilder) {
		CriteriaQuery<Fruit> criteriaQuery = criteriaBuilder.createQuery( Fruit.class );
		Root<Fruit> from = criteriaQuery.from( Fruit.class );
		criteriaQuery.select( from );
		return criteriaQuery;
	}

	private static Uni<List<Fruit>> findAllWithCacheableQuery(Mutiny.Session session) {
		return session.createQuery( "FROM Fruit f ORDER BY f.name ASC", Fruit.class )
				.setCacheable( true )
				.getResultList();
	}

	@Entity(name = "Fruit")
	@Table(name = "known_fruits")
	@NamedQuery(
			name = Fruit.FIND_ALL,
			query = "FROM Fruit f ORDER BY f.name ASC",
			hints = @QueryHint(name = "org.hibernate.cacheable", value = "true"))
	public static class Fruit {
		public static final String FIND_ALL = "Fruits.findAll";

		@Id
		@GeneratedValue
		private Integer id;

		@Column(length = 40, unique = true)
		private String name;

		public Fruit() {
		}

		public Fruit(String name) {
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Fruit fruit = (Fruit) o;
			return Objects.equals( name, fruit.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			return id + ":" + name;
		}
	}
}
