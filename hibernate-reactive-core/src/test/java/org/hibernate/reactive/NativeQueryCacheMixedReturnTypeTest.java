/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.common.spi.Implementor;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.stat.spi.StatisticsImplementor;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Tuple;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;

/**
 * Adapted from ORM's {@code org.hibernate.orm.test.querycache.NativeQueryCacheMixedReturnTypeTest} (HHH-20231).
 * <p>
 * Checks that a native query cache entry is only reused when it's compatible with the return type
 * of the query that reads it. The same SQL string is run once with an entity return type and once with a
 * {@link Tuple} (or scalar) return type: whenever the cached row shape or the mapped Java types don't match,
 * the cache is treated as incompatible, the query is re-executed, and the cache is repopulated.
 * </p>
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = { SQLSERVER, ORACLE, DB2 }, reason = "test uses native ALTER TABLE / INSERT SQL")
public class NativeQueryCacheMixedReturnTypeTest extends BaseReactiveTest {

	private static final String NATIVE_QUERY_EXTRA_COLS_LAST =
			"select u1.id, u1.name, u1.email, u1.age, u1.address, u1.phone, u1.extra_col1, u1.extra_col2 from test_user u1";
	private static final String NATIVE_QUERY_EXTRA_COLS_FIRST =
			"select u1.extra_col1, u1.extra_col2, u1.id, u1.name, u1.email, u1.age, u1.address, u1.phone from test_user u1";
	private static final String NATIVE_QUERY_EXTRA_COLS_SCATTERED =
			"select u1.id, u1.extra_col1, u1.name, u1.email, u1.extra_col2, u1.age, u1.address, u1.phone from test_user u1";
	private static final String NATIVE_QUERY_ENTITY_COLS =
			"select u1.id, u1.name, u1.email, u1.age, u1.address, u1.phone from test_user u1";
	private static final String NATIVE_QUERY_AGE_ONLY = "select u1.age from test_user u1";

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.getProperties().put( Settings.USE_SECOND_LEVEL_CACHE, Boolean.TRUE );
		configuration.getProperties().put( Settings.USE_QUERY_CACHE, Boolean.TRUE );
		configuration.setProperty( Environment.CACHE_REGION_FACTORY, "org.hibernate.cache.jcache.internal.JCacheRegionFactory" );
		configuration.setProperty( "hibernate.javax.cache.provider", "org.ehcache.jsr107.EhcacheCachingProvider" );
		configuration.setProperty( "hibernate.javax.cache.uri", "/ehcache.xml" );
		// Adds extra_col1/extra_col2 to the table (not mapped by any entity) and inserts the test row
		configuration.setProperty( AvailableSettings.HBM2DDL_IMPORT_FILES, "/import-for-native-query-cache-test.sql" );
		configuration.addAnnotatedClass( TestUser.class );
		configuration.addAnnotatedClass( TestUserProfile.class );
		configuration.addAnnotatedClass( TestUserLongAge.class );
		configuration.getProperties().put( Settings.GENERATE_STATISTICS, Boolean.TRUE );
		return configuration;
	}

	@Test
	public void testEntityThenTuple(VertxTestContext context) {
		test( context, resetCache()
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_LAST, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				// Cached data has fewer columns than a Tuple result needs, so it's re-executed
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_LAST, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				// Now the cache holds the complete row, so this reads from it
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_LAST, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testTupleThenEntity(VertxTestContext context) {
		test( context, resetCache()
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_LAST, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				// The Tuple cache entry has all the columns TestUser needs, so this reads from it
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_LAST, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testEntityThenTupleSameColumnCount(VertxTestContext context) {
		test( context, resetCache()
				// The query selects exactly the entity-mapped columns, so the cached row size
				// already matches what a Tuple result needs
				.chain( () -> singleResult( NATIVE_QUERY_ENTITY_COLS, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_ENTITY_COLS, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTuple )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testEntityThenTupleExtraColsFirst(VertxTestContext context) {
		test( context, resetCache()
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_FIRST, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_FIRST, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_FIRST, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testTupleThenEntityExtraColsFirst(VertxTestContext context) {
		test( context, resetCache()
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_FIRST, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_FIRST, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testEntityThenTupleExtraColsScattered(VertxTestContext context) {
		test( context, resetCache()
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testTupleThenEntityExtraColsScattered(VertxTestContext context) {
		test( context, resetCache()
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, Tuple.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserTupleWithExtraCols )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testUserProfileThenTestUser(VertxTestContext context) {
		test( context, resetCache()
				// TestUserProfile maps extra_col1 but not age - populates the cache
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, TestUserProfile.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserProfile )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				// TestUser maps age but not extra_col1 - the stored mapping has no entry for age,
				// so the cache is incompatible and the query is re-executed
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
		);
	}

	@Test
	public void testTestUserThenUserProfile(VertxTestContext context) {
		test( context, resetCache()
				// TestUser maps age but not extra_col1 - populates the cache
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				// TestUserProfile maps extra_col1 but not age - the stored mapping has no entry
				// for extra_col1, so the cache is incompatible and the query is re-executed
				.chain( () -> singleResult( NATIVE_QUERY_EXTRA_COLS_SCATTERED, TestUserProfile.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUserProfile )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
		);
	}

	@Test
	public void testScalarIntegerThenLong(VertxTestContext context) {
		test( context, resetCache()
				.chain( () -> singleResult( NATIVE_QUERY_AGE_ONLY, Integer.class ) )
				.invoke( age -> assertThat( age ).isEqualTo( 30 ) )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				// Same SQL, same position, but a different Java type: the cached value is incompatible
				.chain( () -> singleResult( NATIVE_QUERY_AGE_ONLY, Long.class ) )
				.invoke( age -> assertThat( age ).isEqualTo( 30L ) )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_AGE_ONLY, Long.class ) )
				.invoke( age -> assertThat( age ).isEqualTo( 30L ) )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testScalarLongThenInteger(VertxTestContext context) {
		test( context, resetCache()
				.chain( () -> singleResult( NATIVE_QUERY_AGE_ONLY, Long.class ) )
				.invoke( age -> assertThat( age ).isEqualTo( 30L ) )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_AGE_ONLY, Integer.class ) )
				.invoke( age -> assertThat( age ).isEqualTo( 30 ) )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_AGE_ONLY, Integer.class ) )
				.invoke( age -> assertThat( age ).isEqualTo( 30 ) )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	@Test
	public void testEntityIntegerAgeThenEntityLongAge(VertxTestContext context) {
		test( context, resetCache()
				// TestUser maps age as Integer - populates the cache
				.chain( () -> singleResult( NATIVE_QUERY_ENTITY_COLS, TestUser.class ) )
				.invoke( NativeQueryCacheMixedReturnTypeTest::assertTestUser )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				// Same SQL, same columns, but age is mapped with a different Java type (Long):
				// the cached value is incompatible, so the query is re-executed
				.chain( () -> singleResult( NATIVE_QUERY_ENTITY_COLS, TestUserLongAge.class ) )
				.invoke( user -> assertThat( user.age ).isEqualTo( 30L ) )
				.invoke( () -> assertQueryCacheStatistics( 0, 1, 1 ) )
				.chain( () -> singleResult( NATIVE_QUERY_ENTITY_COLS, TestUserLongAge.class ) )
				.invoke( user -> assertThat( user.age ).isEqualTo( 30L ) )
				.invoke( () -> assertQueryCacheStatistics( 1, 0, 0 ) )
		);
	}

	private static <R> Uni<R> singleResult(String sql, Class<R> resultType) {
		return getMutinySessionFactory()
				.withSession( session -> session.createNativeQuery( sql, resultType )
						.setCacheable( true )
						.getSingleResult() );
	}

	/**
	 * Clears the statistics and evicts the query cache regions so that each test starts
	 * from a clean state, regardless of what earlier tests in the class did.
	 */
	private static Uni<Void> resetCache() {
		return Uni.createFrom().voidItem()
				.invoke( () -> {
					factoryManager.getHibernateSessionFactory().getCache().evictQueryRegions();
					statistics().clear();
				} );
	}

	private static StatisticsImplementor statistics() {
		return ( (Implementor) getSessionFactory() ).getServiceRegistry().getService( StatisticsImplementor.class );
	}

	private static void assertQueryCacheStatistics(long hits, long misses, long puts) {
		StatisticsImplementor statistics = statistics();
		assertThat( statistics.getQueryCacheHitCount() ).isEqualTo( hits );
		assertThat( statistics.getQueryCacheMissCount() ).isEqualTo( misses );
		assertThat( statistics.getQueryCachePutCount() ).isEqualTo( puts );
		statistics.clear();
	}

	private static void assertTestUser(TestUser user) {
		assertThat( user.name ).isEqualTo( "john" );
		assertThat( user.email ).isEqualTo( "john@test.com" );
		assertThat( user.age ).isEqualTo( 30 );
		assertThat( user.address ).isEqualTo( "ny" );
		assertThat( user.phone ).isEqualTo( "123456" );
	}

	private static void assertTestUserProfile(TestUserProfile profile) {
		assertThat( profile.name ).isEqualTo( "john" );
		assertThat( profile.email ).isEqualTo( "john@test.com" );
		assertThat( profile.extraCol1 ).isEqualTo( "ext1" );
		assertThat( profile.address ).isEqualTo( "ny" );
		assertThat( profile.phone ).isEqualTo( "123456" );
	}

	private static void assertTestUserTuple(Tuple tuple) {
		assertThat( tuple.get( "name", String.class ) ).isEqualTo( "john" );
		assertThat( tuple.get( "email", String.class ) ).isEqualTo( "john@test.com" );
		assertThat( tuple.get( "age", Integer.class ) ).isEqualTo( 30 );
		assertThat( tuple.get( "address", String.class ) ).isEqualTo( "ny" );
		assertThat( tuple.get( "phone", String.class ) ).isEqualTo( "123456" );
	}

	private static void assertTestUserTupleWithExtraCols(Tuple tuple) {
		assertTestUserTuple( tuple );
		assertThat( tuple.get( "extra_col1", String.class ) ).isEqualTo( "ext1" );
		assertThat( tuple.get( "extra_col2", String.class ) ).isEqualTo( "ext2" );
	}

	@Entity(name = "TestUser")
	@Table(name = "test_user")
	@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
	public static class TestUser {
		@Id
		Long id;
		String name;
		String email;
		Integer age;
		String address;
		String phone;
	}

	@Entity(name = "TestUserProfile")
	@Table(name = "test_user")
	@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
	public static class TestUserProfile {
		@Id
		Long id;
		String name;
		String email;
		@Column(name = "extra_col1")
		String extraCol1;
		// Does NOT map age - different column subset than TestUser
		String address;
		String phone;
	}

	@Entity(name = "TestUserLongAge")
	@Table(name = "test_user")
	@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
	public static class TestUserLongAge {
		@Id
		Long id;
		String name;
		String email;
		Long age;
		String address;
		String phone;
	}
}
