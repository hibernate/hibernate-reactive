/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.mutiny.impl.MutinyStatelessSessionImpl;
import org.hibernate.reactive.pool.BatchingConnection;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.SqlClientConnection;
import org.hibernate.reactive.stage.impl.StageSessionImpl;
import org.hibernate.reactive.stage.impl.StageStatelessSessionImpl;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 10, timeUnit = MINUTES)
public class BatchingConnectionTest extends ReactiveSessionTest {

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.STATEMENT_BATCH_SIZE, "5");

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker( BatchingConnectionTest::filter, configuration.getProperties() );
		return configuration;
	}

	@BeforeEach
	public void clearTracker() {
		sqlTracker.clear();
	}

	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean filter(String s) {
		String[] accepted = { "insert ", "update ", "delete " };
		for ( String valid : accepted ) {
			if ( s.toLowerCase().startsWith( valid ) ) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected void assertConnectionIsLazy(ReactiveConnection connection) {
		assertConnectionIsLazy( connection, false );
	}

	@Override
	protected void assertConnectionIsLazy(ReactiveConnection connection, boolean stateless) {
		final ReactiveConnection actualConnection;
		if ( !stateless ) {
			// Only the stateful session creates a batching connection
			assertThat( connection ).isInstanceOf( BatchingConnection.class );
			// A little hack, withBatchSize returns the underlying connection when the parameter is less than 1
			actualConnection = connection.withBatchSize( -1 );
		}
		else {
			actualConnection = connection;
		}
		assertThat( actualConnection.getClass().getName() )
				.isEqualTo( org.hibernate.reactive.pool.impl.SqlClientPool.class.getName() + "$ProxyConnection" );
	}

	@Test
	public void testBatchingWithPersistAll(VertxTestContext context) {
		test( context, openSession().thenCompose( s -> s
				.persist(
						new GuineaPig( 11, "One" ),
						new GuineaPig( 22, "Two" ),
						new GuineaPig( 33, "Three" )
				)
				// Auto-flush
				.thenCompose( v -> s
						.createSelectionQuery( "select name from GuineaPig", String.class )
						.getResultList()
						.thenAccept( names -> {
							assertThat( names ).containsExactlyInAnyOrder( "One", "Two", "Three" );
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
							// Parameters are different for different dbs, so we cannot do an exact match
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
									.startsWith( "insert into pig (name,version,id) values " );
							sqlTracker.clear();
						} )
				)
		) );
	}

	@Test
	public void testBatching(VertxTestContext context) {
		test(
				context,
				openSession()
						.thenCompose( s -> voidFuture()
								.thenCompose( v -> s.persist( new GuineaPig(11, "One") ) )
								.thenCompose( v -> s.persist( new GuineaPig(22, "Two") ) )
								.thenCompose( v -> s.persist( new GuineaPig(33, "Three") ) )
								// Auto-flush
								.thenCompose( v -> s.createSelectionQuery("select name from GuineaPig", String.class )
										.getResultList()
										.thenAccept( names -> {
											assertThat( names ).containsExactlyInAnyOrder( "One", "Two", "Three" );
											assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
											// Parameters are different for different dbs, so we cannot do an exact match
											assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
													.matches("insert into pig \\(name,version,id\\) values (.*)" );
											sqlTracker.clear();
										} )
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createSelectionQuery("from GuineaPig", GuineaPig.class)
								.getResultList()
								.thenAccept( list -> list.forEach( pig -> pig.setName("Zero") ) )
								.thenCompose( v -> s.createSelectionQuery("select count(*) from GuineaPig where name='Zero'", Long.class)
										.getSingleResult()
										.thenAccept( count -> {
											assertEquals( 3L, count);
											assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
											assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
													.matches(
															"update pig set name=.+,\\s*version=.+ where id=.+ and "
																	+ "version=.+" );
											sqlTracker.clear();
										} )
								) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.createSelectionQuery("from GuineaPig", GuineaPig.class)
								.getResultList()
								.thenCompose( list -> loop( list, s::remove ) )
								.thenCompose( v -> s.createSelectionQuery("select count(*) from GuineaPig", Long.class)
										.getSingleResult()
										.thenAccept( count -> {
											assertEquals( 0L, count);
											assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
											assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
													.matches( "delete from pig where id=.+ and version=.+" );
											sqlTracker.clear();
										} )
								)
						)
		);
	}

	@Test
	public void testBatchingWithStateless(VertxTestContext context) {
		final GuineaPig[] pigs = {
				new GuineaPig( 11, "One" ),
				new GuineaPig( 22, "Two" ),
				new GuineaPig( 33, "Three" ),
				new GuineaPig( 44, "Four" ),
				new GuineaPig( 55, "Five" ),
				new GuineaPig( 66, "Six" ),
		};
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( 10, pigs ) )
				.invoke( () -> {
					// We expect only one insert query
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches("insert into pig \\(name,version,id\\) values (.*)" );
					sqlTracker.clear();
				} )
		);
	}

	@Test
	public void testMutinyInsertAllWithStateless(VertxTestContext context) {
		final GuineaPig[] pigs = {
				new GuineaPig( 11, "One" ),
				new GuineaPig( 22, "Two" ),
				new GuineaPig( 33, "Three" ),
				new GuineaPig( 44, "Four" ),
				new GuineaPig( 55, "Five" ),
				new GuineaPig( 66, "Six" ),
		};
		test( context, getMutinySessionFactory()
				.withStatelessTransaction( s -> s.insertAll( pigs ) )
				.invoke( () -> {
					// We expect only 1 insert query, despite hibernate.jdbc.batch_size is set to 5, insertAll by default use the pigs.length as batch size
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches("insert into pig \\(name,version,id\\) values (.*)" );
					sqlTracker.clear();
				} )
		);
	}

	@Test
	public void testStageInsertWithStateless(VertxTestContext context) {
		final GuineaPig[] pigs = {
				new GuineaPig( 11, "One" ),
				new GuineaPig( 22, "Two" ),
				new GuineaPig( 33, "Three" ),
				new GuineaPig( 44, "Four" ),
				new GuineaPig( 55, "Five" ),
				new GuineaPig( 66, "Six" ),
		};
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( pigs ) )
				.thenAccept( v -> {
					// We expect only 1 insert query, despite hibernate.jdbc.batch_size is set to 5, insertAll by default use the pigs.length as batch size
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
					// Parameters are different for different dbs, so we cannot do an exact match
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
							.matches("insert into pig \\(name,version,id\\) values (.*)" );
					sqlTracker.clear();
				} )
		);
	}

	@Test
	public void testBatchingConnection(VertxTestContext context) {
		test( context, openSession()
				.thenAccept( session -> assertThat( ( (StageSessionImpl) session ).getReactiveConnection() )
						.isInstanceOf( BatchingConnection.class ) )
		);
	}

	@Test
	public void testBatchingConnectionWithStateless(VertxTestContext context) {
		test( context, openStatelessSession()
				.thenAccept( session -> assertThat( ( (StageStatelessSessionImpl) session ).getReactiveConnection() )
						// Stateless session is not affected by the STATEMENT_BATCH_SIZE property
						.isInstanceOf( SqlClientConnection.class ) )
		);
	}

	@Test
	public void testBatchingConnectionMutiny(VertxTestContext context) {
		test( context, openMutinySession()
				.invoke( session -> assertThat( ( (MutinySessionImpl) session ).getReactiveConnection() )
						.isInstanceOf( BatchingConnection.class ) )
		);
	}

	@Test
	public void testBatchingConnectionWithMutinyStateless(VertxTestContext context) {
		test( context, openMutinyStatelessSession()
				.invoke( session -> assertThat( ( (MutinyStatelessSessionImpl) session ).getReactiveConnection() )
						// Stateless session is not affected by the STATEMENT_BATCH_SIZE property
						.isInstanceOf( SqlClientConnection.class ) )
		);
	}
}
