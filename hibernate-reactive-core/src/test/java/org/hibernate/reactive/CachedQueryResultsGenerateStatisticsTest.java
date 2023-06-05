/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;


import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.Settings;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Checks the # of cache hits when the configuration property Settings.GENERATE_STATISTICS is set to TRUE
 */
@Timeout(value = 10, timeUnit = MINUTES)

public class CachedQueryResultsGenerateStatisticsTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.getProperties().put( Settings.USE_SECOND_LEVEL_CACHE, Boolean.TRUE );
		configuration.getProperties().put( Settings.USE_QUERY_CACHE, Boolean.TRUE );
		configuration.setProperty( Environment.CACHE_REGION_FACTORY, "org.hibernate.cache.jcache.internal.JCacheRegionFactory" );
		configuration.setProperty( AvailableSettings.HBM2DDL_IMPORT_FILES, "/import-for-querycachetest.sql" );
		configuration.setProperty( "hibernate.javax.cache.provider", "org.ehcache.jsr107.EhcacheCachingProvider" );
		configuration.setProperty( "hibernate.javax.cache.uri", "/ehcache.xml" );
		configuration.addAnnotatedClass( CachedQueryResultsTest.Fruit.class );
		configuration.getProperties().put( Settings.GENERATE_STATISTICS, Boolean.TRUE );
		return configuration;
	}

	@Test
	public void testQueryPlanCacheHitsWithGenerateStatisticsTrue(VertxTestContext context) {
		test( context, CachedQueryResultsGenerateStatisticsTest.createFindAllWithNamedQuery()
				.call( CachedQueryResultsGenerateStatisticsTest::createFindAllWithNamedQuery)
				.call( CachedQueryResultsGenerateStatisticsTest::createFindAllWithNamedQuery)
				.invoke( () -> assertEquals( 3L, CachedQueryResultsTest.statistics().getQueryPlanCacheHitCount() ) )
		);
	}

	private static Uni<Mutiny.SelectionQuery<CachedQueryResultsTest.Fruit>> createFindAllWithNamedQuery() {
		return getMutinySessionFactory()
				.withSession( s -> Uni.createFrom().item(
						s.createNamedQuery( CachedQueryResultsTest.Fruit.FIND_ALL, CachedQueryResultsTest.Fruit.class )
				) );
	}

}
