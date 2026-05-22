/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.FlushMode;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.QueryFlushMode;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test that query flush modes work correctly in reactive queries.
 * <p>
 * Based on {@link org.hibernate.orm.test.jpa.query.JpaQueryFlushModeTest}
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = DB2, reason = "IllegalStateException: Needed to have 6 in buffer but only had 0")
public class QueryFlushModeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Thing.class, OtherThing.class );
	}

	@Test
	public void testQueryFlushModeFlushForcesFlushEvenWhenSessionIsManual(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( session -> session
						.createMutationQuery( "delete from QueryFlushThing" )
						.executeUpdate()
						.thenCompose( v -> session
								.createMutationQuery( "delete from QueryFlushOtherThing" )
								.executeUpdate()
						)
				)
				.thenCompose( v -> getSessionFactory().withTransaction( session -> {
					// Set session to MANUAL mode (no auto-flush)
					session.setFlushMode( FlushMode.MANUAL );

					// Persist a new entity
					Thing thing = new Thing( 1L, "thing" );
					session.persist( thing );

					// Query with FLUSH mode should trigger flush even though session is MANUAL
					return session.createSelectionQuery( "select o from QueryFlushOtherThing o", OtherThing.class )
							.setFlushMode( QueryFlushMode.FLUSH )
							.getResultList()
							.thenCompose( list -> countRows( session, "QUERY_FLUSH_THING" )
									.thenAccept( count -> assertEquals( 1L, count,
										"QueryFlushMode.FLUSH should have flushed the entity even with MANUAL session flush mode" ) )
							);
				} ) )
		);
	}

	@Test
	public void testQueryFlushModeNoFlushSuppressesAutoFlush(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( session -> session
						.createMutationQuery( "delete from QueryFlushThing" )
						.executeUpdate()
						.thenCompose( v -> session
								.createMutationQuery( "delete from QueryFlushOtherThing" )
								.executeUpdate()
						)
				)
				.thenCompose( v -> getSessionFactory().withTransaction( session -> {
					// Persist a new entity (session has AUTO flush mode by default)
					Thing thing = new Thing( 2L, "thing" );
					session.persist( thing );

					// Query the same table with NO_FLUSH mode should suppress the auto-flush
					return session.createSelectionQuery( "select t from QueryFlushThing t", Thing.class )
							.setFlushMode( QueryFlushMode.NO_FLUSH )
							.getResultList()
							.thenCompose( list -> countRows( session, "QUERY_FLUSH_THING" )
									.thenAccept( count -> assertEquals( 0L, count,
										"QueryFlushMode.NO_FLUSH should have suppressed the auto-flush" ) )
							);
				} ) )
		);
	}

	@Test
	public void testQueryFlushModeDefaultInheritsManualFlushMode(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( session -> session
						.createMutationQuery( "delete from QueryFlushThing" )
						.executeUpdate()
						.thenCompose( v -> session
								.createMutationQuery( "delete from QueryFlushOtherThing" )
								.executeUpdate()
						)
				)
				.thenCompose( v -> getSessionFactory().withTransaction( session -> {
					// Set session to MANUAL mode
					session.setFlushMode( FlushMode.MANUAL );

					// Persist a new entity
					Thing thing = new Thing( 3L, "thing" );
					session.persist( thing );

					// Query with DEFAULT mode should inherit MANUAL from session (no flush)
					return session.createSelectionQuery( "select t from QueryFlushThing t", Thing.class )
							.setFlushMode( QueryFlushMode.DEFAULT )
							.getResultList()
							.thenCompose( list -> countRows( session, "QUERY_FLUSH_THING" )
									.thenAccept( count -> assertEquals( 0L, count,
										"QueryFlushMode.DEFAULT should have inherited MANUAL mode from session (no flush)" ) )
							);
				} ) )
		);
	}

	@Test
	public void testQueryFlushModeDefaultInheritsAutoFlushMode(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( session -> session
						.createMutationQuery( "delete from QueryFlushThing" )
						.executeUpdate()
						.thenCompose( v -> session
								.createMutationQuery( "delete from QueryFlushOtherThing" )
								.executeUpdate()
						)
				)
				.thenCompose( v -> getSessionFactory().withTransaction( session -> {
					// Session has AUTO flush mode by default

					// Persist a new entity
					Thing thing = new Thing( 4L, "thing" );
					session.persist( thing );

					// Query on overlapping table with DEFAULT mode should trigger auto-flush
					return session.createSelectionQuery( "select t from QueryFlushThing t", Thing.class )
							.setFlushMode( QueryFlushMode.DEFAULT )
							.getResultList()
							.thenCompose( list -> countRows( session, "QUERY_FLUSH_THING" )
									.thenAccept( count -> assertEquals( 1L, count,
										"QueryFlushMode.DEFAULT should have inherited AUTO mode from session (flush occurred)" ) )
							);
				} ) )
		);
	}

	private CompletionStage<Long> countRows(Stage.Session session, String tableName) {
		return session.createNativeQuery( "select count(*) from " + tableName, Long.class )
				.getSingleResult();
	}

	@Entity(name = "QueryFlushThing")
	@Table(name = "QUERY_FLUSH_THING")
	public static class Thing {
		@Id
		private Long id;
		private String name;

		public Thing() {
		}

		public Thing(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	@Entity(name = "QueryFlushOtherThing")
	@Table(name = "QUERY_FLUSH_OTHER_THING")
	public static class OtherThing {
		@Id
		private Long id;
		private String name;

		public OtherThing() {
		}

		public OtherThing(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}
