/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.FlushMode;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.mutiny.Mutiny;

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
 * Test that query flush modes work correctly in Mutiny reactive queries.
 * <p>
 * Based on {@link org.hibernate.orm.test.jpa.query.JpaQueryFlushModeTest}
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = DB2, reason = "IllegalStateException: Needed to have 6 in buffer but only had 0")
public class QueryFlushModeMutinyTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Thing.class, OtherThing.class );
	}

	@Test
	public void testQueryFlushModeFlushForcesFlushEvenWhenSessionIsManual(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.createMutationQuery( "delete from QueryFlushMutinyThing" )
						.executeUpdate()
						.chain( () -> session
								.createMutationQuery( "delete from QueryFlushMutinyOtherThing" )
								.executeUpdate()
						)
				)
				.chain( () -> getMutinySessionFactory().withTransaction( session -> {
					// Set session to MANUAL mode (no auto-flush)
					session.setFlushMode( FlushMode.MANUAL );

					// Persist a new entity
					Thing thing = new Thing( 1L, "thing" );
					return session.persist( thing )
							.chain( () ->
								// Query with FLUSH mode should trigger flush even though session is MANUAL
								session.createSelectionQuery( "select o from QueryFlushMutinyOtherThing o", OtherThing.class )
										.setFlushMode( QueryFlushMode.FLUSH )
										.getResultList()
							)
							.chain( () -> countRows( session, "QUERY_FLUSH_MUTINY_THING" ) )
							.invoke( count -> assertEquals( 1L, count,
									"QueryFlushMode.FLUSH should have flushed the entity even with MANUAL session flush mode" ) );
				} ) )
		);
	}

	@Test
	public void testQueryFlushModeNoFlushSuppressesAutoFlush(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.createMutationQuery( "delete from QueryFlushMutinyThing" )
						.executeUpdate()
						.chain( () -> session
								.createMutationQuery( "delete from QueryFlushMutinyOtherThing" )
								.executeUpdate()
						)
				)
				.chain( () -> getMutinySessionFactory().withTransaction( session -> {
					// Persist a new entity (session has AUTO flush mode by default)
					Thing thing = new Thing( 2L, "thing" );
					return session.persist( thing )
							.chain( () ->
								// Query the same table with NO_FLUSH mode should suppress the auto-flush
								session.createSelectionQuery( "select t from QueryFlushMutinyThing t", Thing.class )
										.setFlushMode( QueryFlushMode.NO_FLUSH )
										.getResultList()
							)
							.chain( () -> countRows( session, "QUERY_FLUSH_MUTINY_THING" ) )
							.invoke( count -> assertEquals( 0L, count,
									"QueryFlushMode.NO_FLUSH should have suppressed the auto-flush" ) );
				} ) )
		);
	}

	@Test
	public void testQueryFlushModeDefaultInheritsManualFlushMode(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.createMutationQuery( "delete from QueryFlushMutinyThing" )
						.executeUpdate()
						.chain( () -> session
								.createMutationQuery( "delete from QueryFlushMutinyOtherThing" )
								.executeUpdate()
						)
				)
				.chain( () -> getMutinySessionFactory().withTransaction( session -> {
					// Set session to MANUAL mode
					session.setFlushMode( FlushMode.MANUAL );

					// Persist a new entity
					Thing thing = new Thing( 3L, "thing" );
					return session.persist( thing )
							.chain( () ->
								// Query with DEFAULT mode should inherit MANUAL from session (no flush)
								session.createSelectionQuery( "select t from QueryFlushMutinyThing t", Thing.class )
										.setFlushMode( QueryFlushMode.DEFAULT )
										.getResultList()
							)
							.chain( () -> countRows( session, "QUERY_FLUSH_MUTINY_THING" ) )
							.invoke( count -> assertEquals( 0L, count,
									"QueryFlushMode.DEFAULT should have inherited MANUAL mode from session (no flush)" ) );
				} ) )
		);
	}

	@Test
	public void testQueryFlushModeDefaultInheritsAutoFlushMode(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.createMutationQuery( "delete from QueryFlushMutinyThing" )
						.executeUpdate()
						.chain( () -> session
								.createMutationQuery( "delete from QueryFlushMutinyOtherThing" )
								.executeUpdate()
						)
				)
				.chain( () -> getMutinySessionFactory().withTransaction( session -> {
					// Session has AUTO flush mode by default

					// Persist a new entity
					Thing thing = new Thing( 4L, "thing" );
					return session.persist( thing )
							.chain( () ->
								// Query on overlapping table with DEFAULT mode should trigger auto-flush
								session.createSelectionQuery( "select t from QueryFlushMutinyThing t", Thing.class )
										.setFlushMode( QueryFlushMode.DEFAULT )
										.getResultList()
							)
							.chain( () -> countRows( session, "QUERY_FLUSH_MUTINY_THING" ) )
							.invoke( count -> assertEquals( 1L, count,
									"QueryFlushMode.DEFAULT should have inherited AUTO mode from session (flush occurred)" ) );
				} ) )
		);
	}

	private io.smallrye.mutiny.Uni<Long> countRows(Mutiny.Session session, String tableName) {
		return session.createNativeQuery( "select count(*) from " + tableName, Long.class )
				.getSingleResult();
	}

	@Entity(name = "QueryFlushMutinyThing")
	@Table(name = "QUERY_FLUSH_MUTINY_THING")
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

	@Entity(name = "QueryFlushMutinyOtherThing")
	@Table(name = "QUERY_FLUSH_MUTINY_OTHER_THING")
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
