/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import org.hibernate.LazyInitializationException;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

import org.hibernate.testing.SqlStatementTracker;

import io.smallrye.mutiny.Uni;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test fetching of basic lazy fields when
 * bytecode enhancements is enabled.
 */
@Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
public class LazyBasicFieldTest extends BaseReactiveIT {

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker( LazyBasicFieldTest::selectQueryFilter, configuration.getProperties() );
		return configuration;
	}

	@BeforeEach
	public void clearTracker() {
		sqlTracker.clear();
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean selectQueryFilter(String s) {
		return s.toLowerCase().startsWith( "select " );
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Crew.class );
	}

	@Test
	public void testFetchBasicField(VertxTestContext context) {
		final Crew emily = new Crew();
		emily.setId( 21L );
		emily.setName( "Emily Jackson" );
		emily.setRole( "Passenger" );
		emily.setFate( "Unknown" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( emily ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Crew.class, emily.getId() )
								.call( crew -> session.fetch( crew, Crew_.role )
										.invoke( role -> assertThat( role ).isEqualTo( emily.getRole() ) ) )
								.call( crew -> session.fetch( crew, Crew_.fate )
										.invoke( fate -> assertThat( fate ).isEqualTo( emily.getFate() ) )
								) ) )
		);
	}

	@Test
	public void testFetchBasicFieldAlsoInitializesIt(VertxTestContext context) {
		final Crew emily = new Crew();
		emily.setId( 21L );
		emily.setName( "Emily Jackson" );
		emily.setRole( "Passenger" );
		emily.setFate( "Unknown" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( emily ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Crew.class, emily.getId() )
								.call( crew -> session.fetch( crew, Crew_.role )
										.invoke( role -> assertThat( role ).isEqualTo( emily.getRole() ) ) )
								.invoke( sqlTracker::clear )
								.call( crew -> session
										.fetch( crew, Crew_.role )
										.invoke( role -> {
											// No select query expected, the previous fetch must have initialized the role attribute
											assertThat( sqlTracker.getLoggedQueries() ).hasSize( 0 );
											assertThat( role ).isEqualTo( emily.getRole() );
										} )
								) ) )
		);
	}

	@Test
	public void testTransparentLazyFetching(VertxTestContext context) {
		final Crew emily = new Crew();
		emily.setId( 21L );
		emily.setName( "Emily Jackson" );
		emily.setRole( "Passenger" );
		emily.setFate( "Unknown" );

		test( context, assertThrown( LazyInitializationException.class,
						getMutinySessionFactory().withTransaction( session -> session.persist( emily ) )
								.chain( () -> getMutinySessionFactory().withSession( session -> session
										.find( Crew.class, emily.getId() )
										// getRole() must throw a LazyInitializationException because we are not using
										// Mutiny.fetch to load a lazy field
										.map( Crew::getRole ) ) )
			  ).invoke( exception -> assertThat( exception )
					  .as( "Expected LazyInitializationException not thrown" )
					  .hasMessageContaining( "Reactive sessions do not support transparent lazy fetching" )
			  )
		);
	}

	@Test
	public void testGetReferenceAndTransparentLazyFetching(VertxTestContext context) {
		final Crew emily = new Crew();
		emily.setId( 21L );
		emily.setName( "Emily Jackson" );
		emily.setRole( "Passenger" );
		emily.setFate( "Unknown" );

		test(
				context, assertThrown(
						LazyInitializationException.class, getMutinySessionFactory()
								.withTransaction( session -> session.persist( emily ) )
								.chain( () -> getMutinySessionFactory().withSession( session -> {
									Crew crew = session.getReference( Crew.class, emily.getId() );
									// getRole() must throw a LazyInitializationException because we are not using
									// Mutiny.fetch to load a lazy field
									String role = crew.getRole();
									return Uni.createFrom()
											.failure( new AssertionError( "Expected LazyInitializationException not thrown" ) );
								} ) )
				).invoke( exception -> assertThat( exception )
						.as( "Expected LazyInitializationException not thrown" )
						.hasMessageContaining( "Reactive sessions do not support transparent lazy fetching" ) )
		);
	}

	public static <U extends Throwable> Uni<U> assertThrown(Class<U> expectedException, Uni<?> uni) {
		return uni.onItemOrFailure().transform( (s, e) -> {
			assertThat( e ).isInstanceOf( expectedException );
			return (U) e;
		} );
	}
}
