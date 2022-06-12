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
import org.hibernate.reactive.pool.impl.SqlClientConnection;
import org.hibernate.reactive.stage.impl.StageSessionImpl;
import org.hibernate.reactive.stage.impl.StageStatelessSessionImpl;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

public class BatchingConnectionTest extends ReactiveSessionTest {

	private SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.STATEMENT_BATCH_SIZE, "5");

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand-off any actual logging properties
		sqlTracker = new SqlStatementTracker( BatchingConnectionTest::filter, configuration.getProperties() );
		return configuration;
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

	@Test
	public void testBatching(TestContext context) {
		test(
				context,
				openSession()
						.thenCompose( s -> voidFuture()
								.thenCompose( v -> s.persist( new GuineaPig(11, "One") ) )
								.thenCompose( v -> s.persist( new GuineaPig(22, "Two") ) )
								.thenCompose( v -> s.persist( new GuineaPig(33, "Three") ) )
								// Auto-flush
								.thenCompose( v -> s.createQuery("select name from GuineaPig")
										.getResultList()
										.thenAccept( names -> {
											assertThat( names ).containsExactlyInAnyOrder( "One", "Two", "Three" );
											assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
											// Parameters are different for different dbs, so we cannot do an exact match
											assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
													.startsWith( "insert into pig (name, version, id) values " );
											sqlTracker.clear();
										} )
								)
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.<GuineaPig>createQuery("from GuineaPig")
								.getResultList()
								.thenAccept( list -> list.forEach( pig -> pig.setName("Zero") ) )
								.thenCompose( v -> s.<Long>createQuery("select count(*) from GuineaPig where name='Zero'")
										.getSingleResult()
										.thenAccept( count -> {
											context.assertEquals( 3L, count);
											assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
											assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
													.matches( "update pig set name=.+, version=.+ where id=.+ and version=.+" );
											sqlTracker.clear();
										} )
								) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.<GuineaPig>createQuery("from GuineaPig")
								.getResultList()
								.thenCompose( list -> loop( list, s::remove ) )
								.thenCompose( v -> s.<Long>createQuery("select count(*) from GuineaPig")
										.getSingleResult()
										.thenAccept( count -> {
											context.assertEquals( 0L, count);
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
	public void testBatchingConnection(TestContext context) {
		test( context, openSession()
				.thenAccept( session -> assertThat( ( (StageSessionImpl) session ).getReactiveConnection() )
						.isInstanceOf( BatchingConnection.class ) )
		);
	}

	@Test
	public void testBatchingConnectionWithStateless(TestContext context) {
		test( context, openStatelessSession()
				.thenAccept( session -> assertThat( ( (StageStatelessSessionImpl) session ).getReactiveConnection() )
						// Stateless session is not affected by the STATEMENT_BATCH_SIZE property
						.isInstanceOf( SqlClientConnection.class ) )
		);
	}

	@Test
	public void testBatchingConnectionMutiny(TestContext context) {
		test( context, openMutinySession()
				.invoke( session -> assertThat( ( (MutinySessionImpl) session ).getReactiveConnection() )
						.isInstanceOf( BatchingConnection.class ) )
		);
	}

	@Test
	public void testBatchingConnectionWithMutinyStateless(TestContext context) {
		test( context, openMutinyStatelessSession()
				.invoke( session -> assertThat( ( (MutinyStatelessSessionImpl) session ).getReactiveConnection() )
						// Stateless session is not affected by the STATEMENT_BATCH_SIZE property
						.isInstanceOf( SqlClientConnection.class ) )
		);
	}
}
