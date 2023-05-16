/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Function;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;

public class UTCTest extends BaseReactiveTest {

	// Keeps tract of the values we have saved
	final Thing thing = new Thing();

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Thing.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		final Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.JDBC_TIME_ZONE, "UTC" );
		return configuration;
	}

	@Test
	public void testDate(VertxTestContext context) {
		thing.date = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) ).getTime();
		testField(
				context,
				"date",
				Thing::getDate,
				entity -> assertThat( entity.getDate().toInstant() ).isEqualTo( thing.getDate().toInstant() )
		);
	}

	@Test
	public void testCalendar(VertxTestContext context) {
		thing.calendar = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
		testField(
				context,
				"calendar",
				Thing::getCalendar,
				entity -> assertThat( entity.getCalendar().toInstant() ).isEqualTo( thing.getCalendar().toInstant() )
		);
	}

	@Test
	public void testLocalDate(VertxTestContext context) {
		thing.localDate = LocalDate.now();
		testField( context, "localDate", Thing::getLocalDate );
	}

	@Test
	public void testLocalTime(VertxTestContext context) {
		thing.localTime = LocalTime.MAX.truncatedTo( ChronoUnit.SECONDS );
		testField( context, "localTime", Thing::getLocalTime );
	}

	@Test
	public void testLocalDateTime(VertxTestContext context) {
		thing.localDateTime = LocalDateTime.now().truncatedTo( ChronoUnit.MILLIS );
		testField( context, "localDateTime", Thing::getLocalDateTime );
	}

	@Test
	public void testOffsetDateTime(VertxTestContext context) {
		final ZoneOffset zoneOffset = ZoneOffset.ofHours( 5 );
		LocalDateTime dateTime = LocalDateTime.of( 2021, 3, 25, 12, 30 );
		thing.offsetDateTime = OffsetDateTime.of( dateTime, zoneOffset );

		testField(
				context,
				"offsetDateTime",
				Thing::getOffsetDateTime,
				// The value is stored as UTC, so we need to convert it back the original time zone
				entity -> assertThat( entity.getOffsetDateTime().atZoneSameInstant( zoneOffset ).toOffsetDateTime() )
						.isEqualTo( thing.offsetDateTime )
		);
	}

	@Test
	public void testZonedDateTime(VertxTestContext context) {
		final ZoneOffset zoneOffset = ZoneOffset.ofHours( 7 );
		thing.zonedDateTime = ZonedDateTime.now( zoneOffset ).truncatedTo( ChronoUnit.MILLIS );
		testField(
				context,
				"zonedDateTime",
				Thing::getZonedDateTime,
				// The value is stored as UTC, so we need to convert it back the original time zone
				entity -> assertThat( entity.getZonedDateTime().withZoneSameInstant( zoneOffset ) ).isEqualTo( thing.zonedDateTime )
		);
	}

	private void testField(VertxTestContext context, String columnName, Function<Thing, Object> getFieldValue) {
		testField( context, columnName, getFieldValue, entity -> assertThat( getFieldValue.apply( entity ) ).isEqualTo( getFieldValue.apply( thing ) ) );
	}

	private void testField(VertxTestContext context, String columnName, Function<Thing, ?> fieldValue, Consumer<Thing> assertion) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( thing ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session.find( Thing.class, thing.id ) )
						.invoke( t -> {
							assertThat( t )
									.as( "Entity not found when using id " + thing.id )
									.isNotNull();
							assertion.accept( t );
						} )
				)
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.createQuery( "from ThingInUTC where " + columnName + "=:dt", Thing.class )
								.setParameter( "dt", fieldValue.apply( thing ) )
								.getSingleResultOrNull() )
						.invoke( result -> {
							assertThat( result )
									.as( "No result when querying using filter: " + columnName + " = " + fieldValue.apply( thing ) )
									.isNotNull();
							assertion.accept( result );
						} )
				)
		);
	}

	@Entity(name = "ThingInUTC")
	public static class Thing {
		@Id
		@GeneratedValue
		long id;

		@Column(name = "dateType")
		Date date;

		@Column(name = "calendarType")
		Calendar calendar;

		@Column(name = "offsetDateTimeType")
		OffsetDateTime offsetDateTime;

		@Column(name = "zonedDateTimeType")
		ZonedDateTime zonedDateTime;

		@Column(name = "localDateType")
		LocalDate localDate;

		@Column(name = "localTimeType")
		LocalTime localTime;

		@Column(name = "localDateTimeType")
		LocalDateTime localDateTime;

		public Date getDate() {
			return date;
		}

		public Calendar getCalendar() {
			return calendar;
		}

		public OffsetDateTime getOffsetDateTime() {
			return offsetDateTime;
		}

		public ZonedDateTime getZonedDateTime() {
			return zonedDateTime;
		}

		public LocalDate getLocalDate() {
			return localDate;
		}

		public LocalTime getLocalTime() {
			return localTime;
		}

		public LocalDateTime getLocalDateTime() {
			return localDateTime;
		}
    }
}
