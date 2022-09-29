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
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Supplier;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class UTCTest extends BaseReactiveTest {

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
	public void testDate(TestContext context) {
		thing.date = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) )
				.getTime();

		testField(
				context,
				"dateType",
				thing::getDate,
				entity -> context.assertEquals( thing.date, entity.date )
		);
	}

	@Test
	public void testCalendar(TestContext context) {
		thing.calendar = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );

		testField(
				context,
				"calendarType",
				thing::getCalendar,
				entity -> context.assertEquals( thing.calendar.toInstant(), entity.calendar.toInstant() )
		);
	}

	@Test
	public void testLocalDate(TestContext context) {
		thing.localDate = LocalDate.now();

		testField(
				context,
				"localDateType",
				thing::getLocalDate,
				entity -> context.assertEquals( thing.localDate, entity.localDate )
		);
	}

	@Test
	public void testLocalTime(TestContext context) {
		thing.localTime = LocalTime.MAX.truncatedTo( ChronoUnit.SECONDS );

		testField(
				context,
				"localTimeType",
				thing::getLocalTime,
				entity -> context.assertEquals( thing.localTime, entity.localTime )
		);
	}

	@Test
	public void testLocalDateTime(TestContext context) {
		thing.localDateTime = LocalDateTime.now()
				.truncatedTo( ChronoUnit.MILLIS );

		testField(
				context,
				"localDateTimeType",
				thing::getLocalDateTime,
				entity -> context.assertEquals( thing.localDateTime, entity.localDateTime )
		);
	}

	@Test
	public void testOffsetDateTime(TestContext context) {
		final ZoneOffset zoneOffset = ZoneOffset.ofHours( 5 );
		LocalDateTime dateTime = LocalDateTime.of( 2021, 3, 25, 12, 30 );
		thing.offsetDateTime = OffsetDateTime.of( dateTime, zoneOffset );

		testField(
				context,
				"offsetDateTimeType",
				thing::getOffsetDateTime,
				entity -> {
					context.assertEquals( thing.offsetDateTime,
							entity.offsetDateTime.toInstant().atZone( zoneOffset ).toOffsetDateTime() );
				}
		);
	}

	@Test
	public void testOffsetTime(TestContext context) {
		thing.offsetTime = OffsetTime.now( ZoneOffset.ofHours( 7 ) )
				.truncatedTo( ChronoUnit.SECONDS );

		testField(
				context,
				"offsetTimeType",
				thing::getOffsetTime,
				entity -> context.assertEquals( thing.offsetTime.toLocalTime(), entity.offsetTime.toLocalTime() )
		);
	}

	@Test
	public void testZonedDateTime(TestContext context) {
		final ZoneOffset zoneOffset = ZoneOffset.ofHours( 7 );
		thing.zonedDateTime = ZonedDateTime.now( zoneOffset );

		testField(
				context,
				"zonedDateTimeType",
				thing::getZonedDateTime,
				// The equals fails on JDK 15+ without the truncated
				entity -> context.assertEquals(
						thing.zonedDateTime.truncatedTo( ChronoUnit.MILLIS ),
						entity.zonedDateTime.withZoneSameInstant( zoneOffset ).truncatedTo( ChronoUnit.MILLIS ) )
		);
	}

	private void testField(TestContext context, String columnName, Supplier<?> fieldValue, Consumer<Thing> assertion) {
		test( context, getMutinySessionFactory()
				.withSession( session -> session.persist( thing ).call( session::flush ).invoke( session::clear ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session.find( Thing.class, thing.id ) )
						.invoke( t -> {
							context.assertNotNull( t );
							assertion.accept( t );
						} )
				)
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.createQuery( "from ThingInUTC where " + columnName + "=:dt", Thing.class )
								.setParameter( "dt", fieldValue.get() )
								.getSingleResult() )
						.invoke( assertion )
				)
		);
	}

	@Entity(name = "ThingInUTC")
	static class Thing {
		@Id
		@GeneratedValue
		long id;

		@Column(name = "dateType")
		Date date;

		@Column(name = "calendarType")
		Calendar calendar;

		@Column(name = "offsetDateTimeType")
		OffsetDateTime offsetDateTime;

		@Column(name = "offsetTimeType")
		OffsetTime offsetTime;

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

		public OffsetTime getOffsetTime() {
			return offsetTime;
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
