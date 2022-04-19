/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;


import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.reactive.provider.Settings;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

/**
 * Checks the # of cache hits when the configuration property Settings.GENERATE_STATISTICS is set to TRUE
 *
 * @see CachedQueryResultsTest#testQueryPlanCacheHitsGenerateStatisticsFalse(TestContext)
 */
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
	public void testQueryPlanCacheHitsWithGenerateStatisticsTrue(TestContext context) {
		test( context, CachedQueryResultsTest.criteriaFindAll()
				.call( CachedQueryResultsTest::criteriaFindAll )
				.call( CachedQueryResultsTest::criteriaFindAll )
				.invoke( () -> context.assertEquals( 2L, CachedQueryResultsTest.statistics().getQueryPlanCacheHitCount() ) )
		);
	}
}
