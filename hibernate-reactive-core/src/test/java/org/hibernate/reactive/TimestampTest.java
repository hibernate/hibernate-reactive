/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.CurrentTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.generator.EventType.INSERT;

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
						.invoke( () -> assertThat(
								record.updated.truncatedTo( ChronoUnit.HOURS )
						).isEqualTo( record.created.truncatedTo( ChronoUnit.HOURS ) ) )
						.invoke( () -> assertThat(
								record.currentTime.truncatedTo( ChronoUnit.HOURS )
						).isEqualTo( record.created.truncatedTo( ChronoUnit.HOURS ) ) )
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
							assertThat( event.history.updated.truncatedTo( ChronoUnit.HOURS ) )
									.isEqualTo( event.history.created.truncatedTo( ChronoUnit.HOURS ) );
						} )
						.invoke( () -> event.name = "Conference" )
						.chain( session::flush )
						.invoke( () -> assertInstants( event, history ) ) )
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.find( Record.class, event.id ) ) )
				.invoke( r -> assertInstants( event, history ) )
		);
	}

	private static void assertInstants(Record r) {
		assertThat( r.created ).isNotNull();
		assertThat( r.updated ).isNotNull();
		assertThat( r.currentTime).isNotNull();
		// Sometimes, when the test suite is fast enough, they might be the same
		assertThat( r.updated )
				.as( "Updated instant is before created. Updated[" + r.updated + "], Created[" + r.created + "]" )
				.isAfterOrEqualTo( r.created );
	}

	private static void assertInstants(Event e, History h) {
		assertThat( e.history.created ).isNotNull();
		assertThat( e.history.updated ).isNotNull();
		assertThat( e.currentTimestampEmbedded.currentCreatedAt ).isNotNull();
		assertThat( e.currentTimestampEmbedded.currentLastUpdatedAt ).isNotNull();
		// Sometimes, when the test suite is fast enough, they might be the same:
		assertThat( e.history.updated )
				.as( "Updated instant is before created. Updated[" + e.history.updated + "], Created[" + e.history.created + "]" )
				.isAfterOrEqualTo( e.history.created );
		assertThat( e.history.created ).isEqualTo( h.created );

		assertThat( e.currentTimestampEmbedded.currentCreatedAt.truncatedTo( ChronoUnit.HOURS ) ).isEqualTo( h.created.truncatedTo( ChronoUnit.HOURS ) );
		assertThat( e.currentTimestampEmbedded.currentLastUpdatedAt )
				.as( "Updated instant is before created. Updated[" + e.currentTimestampEmbedded.currentLastUpdatedAt + "], Created[" + e.currentTimestampEmbedded.currentCreatedAt + "]" )
				.isAfterOrEqualTo( e.currentTimestampEmbedded.currentCreatedAt );
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
		@CurrentTimestamp
		Instant currentTime;
	}

	@Entity(name = "Event")
	static class Event {

		@Id
		@GeneratedValue
		public Long id;

		public String name;

		@Embedded
		public History history;

		@Embedded
		public CurrentTimestampEmbedded currentTimestampEmbedded;
	}

	@Embeddable
	static class History {
		@CreationTimestamp
		public LocalDateTime created;

		@UpdateTimestamp
		public LocalDateTime updated;
	}

	@Embeddable
	static class CurrentTimestampEmbedded {
		@CurrentTimestamp(event = INSERT)
		LocalDateTime currentCreatedAt;
		@CurrentTimestamp
		LocalDateTime currentLastUpdatedAt;
	}
}
