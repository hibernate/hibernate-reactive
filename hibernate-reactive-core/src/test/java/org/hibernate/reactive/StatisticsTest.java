/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.cfg.Configuration;
import org.hibernate.stat.EntityStatistics;
import org.hibernate.stat.QueryStatistics;
import org.hibernate.stat.Statistics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.hibernate.cfg.AvailableSettings.GENERATE_STATISTICS;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class StatisticsTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Named.class );
		configuration.setProperty( GENERATE_STATISTICS, "true" );
		return configuration;
	}

	@AfterEach
	public void cleanDB() {
		getSessionFactory().close();
	}

	@Test
	public void testMutinyStatistics(VertxTestContext context) {
		Statistics statistics = getMutinySessionFactory().getStatistics();
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( s -> s
								.persistAll( new Named( "foo" ), new Named( "bar" ), new Named( "baz" ) )
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( s -> s.find( Named.class, 1 ).chain( s::remove ) )
						)
						.invoke( v -> {
							assertEquals( 3L, statistics.getEntityInsertCount() );
							assertEquals( 1L, statistics.getEntityLoadCount() );
							assertEquals( 1L, statistics.getEntityDeleteCount() );

							assertEquals( 2L, statistics.getFlushCount() );
							assertEquals( 2L, statistics.getSessionOpenCount() );
							assertEquals( 2L, statistics.getSessionCloseCount() );
//                            assertEquals( 2L, statistics.getTransactionCount() );
//                            assertEquals( 2L, statistics.getConnectCount() );
//                            assertEquals( 5L, statistics.getPrepareStatementCount() );

							EntityStatistics entityStatistics = statistics.getEntityStatistics( Named.class.getName() );
							assertEquals( 3L, entityStatistics.getInsertCount() );
							assertEquals( 1L, entityStatistics.getLoadCount() );
							assertEquals( 1L, entityStatistics.getDeleteCount() );

							assertEquals( 0, statistics.getQueries().length );
						} )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( s -> s.createQuery( "from Named" ).getResultList() ) )
						.invoke( v -> {
							assertEquals( 3L, statistics.getEntityInsertCount() );
							assertEquals( 3L, statistics.getEntityLoadCount() );
							assertEquals( 1L, statistics.getEntityDeleteCount() );
							assertEquals( 1L, statistics.getQueryExecutionCount() );

							assertEquals( 1, statistics.getQueries().length );

							QueryStatistics queryStatistics = statistics.getQueryStatistics( "from Named" );
							assertEquals( 1L, queryStatistics.getExecutionCount() );
							assertEquals( 2L, queryStatistics.getExecutionRowCount() );
//                            assertNotEquals( 0L, queryStatistics.getExecutionMaxTime() );

							assertEquals( 3L, statistics.getFlushCount() );
							assertEquals( 3L, statistics.getSessionOpenCount() );
							assertEquals( 3L, statistics.getSessionCloseCount() );
//                            assertEquals( 3L, statistics.getTransactionCount() );
//                            assertEquals( 3L, statistics.getConnectCount() );
//                            assertEquals( 6L, statistics.getPrepareStatementCount() );
						} )
		);
	}

	@Test
	public void testStageStatistics(VertxTestContext context) {
		Statistics statistics = getSessionFactory().getStatistics();
		test(
				context,
				getSessionFactory()
						.withTransaction( s -> s
								.persist( new Named( "foo" ), new Named( "bar" ), new Named( "baz" ) ) )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( s -> s.find( Named.class, 1 ).thenCompose( s::remove ) ) )
						.thenAccept( v -> {
							assertEquals( 3L, statistics.getEntityInsertCount() );
							assertEquals( 1L, statistics.getEntityLoadCount() );
							assertEquals( 1L, statistics.getEntityDeleteCount() );

							assertEquals( 2L, statistics.getFlushCount() );
							assertEquals( 2L, statistics.getSessionOpenCount() );
							assertEquals( 2L, statistics.getSessionCloseCount() );
//                            assertEquals( 2L, statistics.getTransactionCount() );
//                            assertEquals( 2L, statistics.getConnectCount() );
//                            assertEquals( 5L, statistics.getPrepareStatementCount() );

							EntityStatistics entityStatistics = statistics.getEntityStatistics( Named.class.getName() );
							assertEquals( 3L, entityStatistics.getInsertCount() );
							assertEquals( 1L, entityStatistics.getLoadCount() );
							assertEquals( 1L, entityStatistics.getDeleteCount() );

							assertEquals( 0, statistics.getQueries().length );
						} )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( s -> s
										.createQuery( "from Named" ).getResultList() ) )
						.thenAccept( v -> {
							assertEquals( 3L, statistics.getEntityInsertCount() );
							assertEquals( 3L, statistics.getEntityLoadCount() );
							assertEquals( 1L, statistics.getEntityDeleteCount() );
							assertEquals( 1L, statistics.getQueryExecutionCount() );

							assertEquals( 1, statistics.getQueries().length );

							QueryStatistics queryStatistics = statistics.getQueryStatistics( "from Named" );
							assertEquals( 1L, queryStatistics.getExecutionCount() );
							assertEquals( 2L, queryStatistics.getExecutionRowCount() );
//                            assertNotEquals( 0L, queryStatistics.getExecutionMaxTime() );

							assertEquals( 3L, statistics.getFlushCount() );
							assertEquals( 3L, statistics.getSessionOpenCount() );
							assertEquals( 3L, statistics.getSessionCloseCount() );
//                            assertEquals( 3L, statistics.getTransactionCount() );
//                            assertEquals( 3L, statistics.getConnectCount() );
//                            assertEquals( 6L, statistics.getPrepareStatementCount() );
						} )
		);
	}

	@Entity(name = "Named")
	@Table(name = "named_thing")
	static class Named {
		@Id
		@GeneratedValue
		Integer id;
		String name;

		public Named(String name) {
			this.name = name;
		}

		public Named() {
		}
	}

}
