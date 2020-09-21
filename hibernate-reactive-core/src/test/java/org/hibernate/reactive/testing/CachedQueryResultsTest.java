/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import java.util.List;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.NamedQuery;
import javax.persistence.QueryHint;
import javax.persistence.Table;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.Settings;

import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;

/**
 * Test what happens when the query is cached but the result entries aren't.
 * <p>
 * Uses an import.sql file for initialize the db and requires the Hibernate EHCache
 * dependency on the classpath at runtime.
 * </p>
 * <p>
 * When a query is cachable, hibernate will cache the entity ids returned by the query.
 * When the query runs a second time, it will load the entities using the cached ids.
 * If the entities aren't in the cache it will load them from the db.
 * </p>
 */
public class CachedQueryResultsTest extends BaseReactiveTest {

	private static final Object[] FRUITS = {
			new Fruit( "Banana" ),
			new Fruit( "Pineapple" ),
			new Fruit( "Tomato" )
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
		return configuration;
	}

	private Uni<?> populateDB() {
		return getMutinySessionFactory()
				.withTransaction( (session, tx) -> session.persist( FRUITS ) );
	}

	private static Uni<List<Fruit>> findall(Mutiny.Session session) {
		return session.createNamedQuery( Fruit.FIND_ALL, Fruit.class )
				.getResultList();
	}

	@Test
	public void testLoadFromCachedQueryResult(TestContext context) {
		test( context, getMutinySessionFactory().withSession( CachedQueryResultsTest::findall )
				// We need to close the session between the two findAll or the results will come from the
				// first-level cache
				.call( () -> getMutinySessionFactory().withSession( CachedQueryResultsTest::findall ) )
				.invoke( list -> {
					context.assertNotNull( list );
					context.assertEquals( 3, list.size() );
					int i = 0;
					for ( Fruit entity : list ) {
						context.assertEquals( entity, FRUITS[i++] );
					}
				} )
		);
	}

	private static Uni<List<Fruit>> findall2(Mutiny.Session session) {
		return session.createQuery( "FROM Fruit f ORDER BY f.name ASC", Fruit.class )
				.setCacheable(true)
				.getResultList();
	}

	@Test
	public void testLoadFromCachedQueryResult2(TestContext context) {
		test( context, getMutinySessionFactory().withSession( CachedQueryResultsTest::findall2 )
				// We need to close the session between the two findAll or the results will come from the
				// first-level cache
				.call( () -> getMutinySessionFactory().withSession( CachedQueryResultsTest::findall2 ) )
				.invoke( list -> {
					context.assertNotNull( list );
					context.assertEquals( 3, list.size() );
					int i = 0;
					for ( Fruit entity : list ) {
						context.assertEquals( entity, FRUITS[i++] );
					}
				} )
		);
	}

	@Entity(name = "Fruit")
	@Table(name = "known_fruits")
	@NamedQuery(name = Fruit.FIND_ALL
			, query = "FROM Fruit f ORDER BY f.name ASC"
			, hints = @QueryHint(name = "org.hibernate.cacheable", value = "true"))
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
