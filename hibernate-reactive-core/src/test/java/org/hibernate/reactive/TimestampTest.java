/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, timeUnit = MINUTES)
public class TimestampTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class, Event.class );
	}

	@Test
	public void test(VertxTestContext context) {
		Record record = new Record();
		record.text = "initial text";
		test( context, getMutinySessionFactory()
				.withSession( session -> session.persist( record )
						.chain( session::flush )
						.invoke( () -> assertEquals(
								record.created.truncatedTo( ChronoUnit.HOURS ),
								record.updated.truncatedTo( ChronoUnit.HOURS )
						) )
						.invoke( () -> record.text = "edited text" )
						.chain( session::flush )
						.invoke( () -> assertInstants( record ) ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Record.class, record.id ) ) )
				.invoke( r -> assertInstants( record ) )
		);
	}

	@Test
	public void testEmbedded(VertxTestContext context) {
		Event event = new Event();
		History history = new History();
		event.name = "Concert";
		test( context, getMutinySessionFactory()
				.withSession( session -> session.persist( event )
						.chain( session::flush )
						.invoke( () -> {
							history.created = event.history.created;
							history.updated = event.history.updated;
							assertEquals(
									event.history.created.truncatedTo( ChronoUnit.HOURS ),
									event.history.updated.truncatedTo( ChronoUnit.HOURS )
							); })
						.invoke( () -> event.name = "Conference" )
						.chain( session::flush )
						.invoke( () -> assertInstants( event, history ) ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Record.class, event.id ) ) )
				.invoke( r -> assertInstants( event, history ) )
		);
	}

	private static void assertInstants(Record r) {
		assertNotNull( r.created );
		assertNotNull( r.updated );
		// Sometimes, when the test suite is fast enough, they might be the same
		assertTrue(
				r.updated.compareTo( r.created ) >= 0,
				"Updated instant is before created. Updated[" + r.updated + "], Created[" + r.created + "]"
		);
	}

	private static void assertInstants(Event e, History h) {
		assertNotNull( e.history.created );
		assertNotNull( e.history.updated );
		// Sometimes, when the test suite is fast enough, they might be the same
		assertTrue(
				!e.history.updated.isBefore( e.history.created ),
				"Updated instant is before created. Updated[" + e.history.updated + "], Created[" + e.history.created + "]"
		);
		assertEquals( h.created, e.history.created );

	}

	@Entity(name = "Record")
	static class Record {
		@GeneratedValue
		@Id
		long id;
		@Basic(optional = false)
		String text;
		@CreationTimestamp
		Instant created;
		@UpdateTimestamp
		Instant updated;
	}

	@Entity(name = "Event")
	static class Event {

		@Id
		@GeneratedValue
		public Long id;

		public String name;

		@Embedded
		public History history;

	}

	@Embeddable
	static class History {
		@Column
		@CreationTimestamp
		public LocalDateTime created;

		@Column
		@UpdateTimestamp
		public LocalDateTime updated;

	}
}
