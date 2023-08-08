/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.TimeZoneColumn;
import org.hibernate.annotations.TimeZoneStorage;
import org.hibernate.annotations.TimeZoneStorageType;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Tuple;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.AvailableSettings.TIMEZONE_DEFAULT_STORAGE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;

/**
 * Adapted from org.hibernate.orm.test.mapping.basic.TimeZoneStorageMappingTests
 *
 * <p>
 * Note that the tests below do not use ORM's annotations below to calculate
 * if a Dialect supports Format or Timezone types:
 *    @RequiresDialectFeature(feature = DialectFeatureChecks.SupportsFormat.class)
 *    @RequiresDialectFeature(feature = DialectFeatureChecks.SupportsTimezoneTypes.class....)
 * </p>
 * <p>
 * It appears that DB2, SQLServer and Oracle do not yet support FORMAT and none of reactive's supported Dialects
 * support Timezone Types via Offset, so the ORM's testNormalizeOffset(...) method is not included in these tests.
 * </p>
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class TimeZoneStorageMappingTest extends BaseReactiveTest {

	// SQLSERVER currently does not support java.time.OffsetTime
	@RegisterExtension
	public DBSelectionExtension selectionRule = DBSelectionExtension.skipTestsFor( SQLSERVER );

	private static final ZoneOffset JVM_TIMEZONE_OFFSET = OffsetDateTime.now().getOffset();
	private static final OffsetTime OFFSET_TIME = OffsetTime.of(
			LocalTime.of(
					12,
					0,
					0
			),
			ZoneOffset.ofHoursMinutes( 5, 45 )
	);
	private static final OffsetDateTime OFFSET_DATE_TIME = OffsetDateTime.of(
			LocalDateTime.of(
					2022,
					3,
					1,
					12,
					0,
					0
			),
			ZoneOffset.ofHoursMinutes( 5, 45 )
	);
	private static final ZonedDateTime ZONED_DATE_TIME = ZonedDateTime.of(
			LocalDateTime.of(
					2022,
					3,
					1,
					12,
					0,
					0
			),
			ZoneOffset.ofHoursMinutes( 5, 45 )
	);
	private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern( "HH:mm:ssxxx" );
	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern( "dd/MM/yyyy 'at' HH:mm:ssxxx" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( TimeZoneStorageEntity.class );
	}

	@Override
	protected void setProperties(Configuration configuration) {
		super.setProperties( configuration );
		configuration.setProperty( TIMEZONE_DEFAULT_STORAGE, "AUTO" );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		TimeZoneStorageEntity entity =  new TimeZoneStorageEntity( 1, OFFSET_TIME, OFFSET_DATE_TIME, ZONED_DATE_TIME );

		test( context, getMutinySessionFactory().withTransaction( (s, t) -> s.persist( entity ) ) );
	}

	@Test
	public void testOffsetRetainedAuto(VertxTestContext context) {
		testOffsetRetained( context, "Auto" );
	}

	@Test
	public void testOffsetRetainedColumn(VertxTestContext context) {
		testOffsetRetained( context, "Column" );
	}

	@Test
	public void testOffsetRetainedFormatAuto(VertxTestContext context) {
		testOffsetRetainedFormat( context, "Auto" );
	}

	@Test
	public void testOffsetRetainedFormatColumn(VertxTestContext context) {
		testOffsetRetainedFormat( context, "Column" );
	}

	public void testOffsetRetained(VertxTestContext context, String suffix) {
		test( context, openSession()
				.thenCompose( session -> session.createQuery(
							  "select " +
									  "e.offsetTime" + suffix + ", " +
									  "e.offsetDateTime" + suffix + ", " +
									  "e.zonedDateTime" + suffix + ", " +
									  "extract(offset from e.offsetTime" + suffix + "), " +
									  "extract(offset from e.offsetDateTime" + suffix + "), " +
									  "extract(offset from e.zonedDateTime" + suffix + "), " +
									  "e.offsetTime" + suffix + " + 1 hour, " +
									  "e.offsetDateTime" + suffix + " + 1 hour, " +
									  "e.zonedDateTime" + suffix + " + 1 hour, " +
									  "e.offsetTime" + suffix + " + 1 hour - e.offsetTime" + suffix + ", " +
									  "e.offsetDateTime" + suffix + " + 1 hour - e.offsetDateTime" + suffix + ", " +
									  "e.zonedDateTime" + suffix + " + 1 hour - e.zonedDateTime" + suffix + ", " +
									  "1 from TimeZoneStorageEntity e " +
									  "where e.offsetDateTime" + suffix + " = e.offsetDateTime" + suffix,
							  Tuple.class
					  ).getSingleResult()
					  .thenAccept( result -> {
						  assertThat( result.get( 0, OffsetTime.class ) ).isEqualTo( OFFSET_TIME );
						  assertThat( result.get( 1, OffsetDateTime.class ) ).isEqualTo( OFFSET_DATE_TIME );
						  assertThat( result.get( 2, ZonedDateTime.class ) ).isEqualTo( ZONED_DATE_TIME );
						  assertThat( result.get( 3, ZoneOffset.class ) ).isEqualTo( OFFSET_TIME.getOffset() );
						  assertThat( result.get( 4, ZoneOffset.class ) ).isEqualTo( OFFSET_DATE_TIME.getOffset() );
						  assertThat( result.get( 5, ZoneOffset.class ) ).isEqualTo( ZONED_DATE_TIME.getOffset() );
						  assertThat( result.get( 6, OffsetTime.class ) ).isEqualTo( OFFSET_TIME.plusHours( 1L ) );
						  assertThat( result.get( 7, OffsetDateTime.class ) ).isEqualTo( OFFSET_DATE_TIME.plusHours( 1L ) );
						  assertThat( result.get( 8, ZonedDateTime.class ) ).isEqualTo( ZONED_DATE_TIME.plusHours( 1L ) );
						  assertThat( result.get( 9, Duration.class ) ).isEqualTo( Duration.ofHours( 1L ) );
						  assertThat( result.get( 10, Duration.class ) ).isEqualTo( Duration.ofHours( 1L ) );
						  assertThat( result.get( 11, Duration.class ) ).isEqualTo( Duration.ofHours( 1L ) );
					  } )
				)
		);
	}

	public void testOffsetRetainedFormat(VertxTestContext context, String suffix) {
		test( context, openSession()
				.thenCompose( session -> session.createQuery(
							  "select " +
									  "format(e.offsetTime" + suffix + " as 'HH:mm:ssxxx'), " +
									  "format(e.offsetDateTime" + suffix + " as 'dd/MM/yyyy ''at'' HH:mm:ssxxx'), " +
									  "format(e.zonedDateTime" + suffix + " as 'dd/MM/yyyy ''at'' HH:mm:ssxxx'), " +
									  "1 from TimeZoneStorageEntity e " +
									  "where e.offsetDateTime" + suffix + " = e.offsetDateTime" + suffix,
							  Tuple.class
					  ).getSingleResult()
					  .thenAccept( result -> {
						  assertThat( result.get( 0, String.class ) ).isEqualTo( TIME_FORMATTER.format( OFFSET_TIME ) );
						  assertThat( result.get( 1, String.class ) ).isEqualTo( FORMATTER.format( OFFSET_DATE_TIME ) );
						  assertThat( result.get( 2, String.class ) ).isEqualTo( FORMATTER.format( ZONED_DATE_TIME ) );
					  } )
				)
		);
	}

	@Test
	public void testNormalize(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( session -> session.createQuery(
							"select " +
									"e.offsetTimeNormalized, " +
									"e.offsetDateTimeNormalized, " +
									"e.zonedDateTimeNormalized, " +
									"e.offsetTimeNormalizedUtc, " +
									"e.offsetDateTimeNormalizedUtc, " +
									"e.zonedDateTimeNormalizedUtc " +
									"from TimeZoneStorageEntity e",
							Tuple.class
					).getSingleResult()
					.thenAccept( result -> {
						assertThat( result.get( 0, OffsetTime.class ).toLocalTime()).isEqualTo( OFFSET_TIME.withOffsetSameInstant( JVM_TIMEZONE_OFFSET ).toLocalTime() );
						assertThat( result.get( 0, OffsetTime.class ).getOffset()).isEqualTo( JVM_TIMEZONE_OFFSET );
						assertThat( result.get( 1, OffsetDateTime.class ).toInstant()).isEqualTo( OFFSET_DATE_TIME.toInstant() );
						assertThat( result.get( 2, ZonedDateTime.class ).toInstant()).isEqualTo( ZONED_DATE_TIME.toInstant() );
						assertThat( result.get( 3, OffsetTime.class ).toLocalTime()).isEqualTo( OFFSET_TIME.withOffsetSameInstant( ZoneOffset.UTC ).toLocalTime() );
						assertThat( result.get( 3, OffsetTime.class ).getOffset()).isEqualTo( ZoneOffset.UTC );
						assertThat( result.get( 4, OffsetDateTime.class ).toInstant()).isEqualTo( OFFSET_DATE_TIME.toInstant() );
						assertThat( result.get( 5, ZonedDateTime.class ).toInstant()).isEqualTo( ZONED_DATE_TIME.toInstant() );
					}
				)
			)
		);
	}

	@Entity(name = "TimeZoneStorageEntity")
	@Table(name = "TimeZoneStorageEntity")
	public static class TimeZoneStorageEntity {
		@Id
		public Integer id;

		//tag::time-zone-column-examples-mapping-example[]
		@TimeZoneStorage(TimeZoneStorageType.COLUMN)
		@TimeZoneColumn(name = "birthtime_offset_offset")
		@Column(name = "birthtime_offset")
		public OffsetTime offsetTimeColumn;

		@TimeZoneStorage(TimeZoneStorageType.COLUMN)
		@TimeZoneColumn(name = "birthday_offset_offset")
		@Column(name = "birthday_offset")
		public OffsetDateTime offsetDateTimeColumn;

		@TimeZoneStorage(TimeZoneStorageType.COLUMN)
		@TimeZoneColumn(name = "birthday_zoned_offset")
		@Column(name = "birthday_zoned")
		public ZonedDateTime zonedDateTimeColumn;
		//end::time-zone-column-examples-mapping-example[]

		@TimeZoneStorage
		@Column(name = "birthtime_offset_auto")
		public OffsetTime offsetTimeAuto;

		@TimeZoneStorage
		@Column(name = "birthday_offset_auto")
		public OffsetDateTime offsetDateTimeAuto;

		@TimeZoneStorage
		@Column(name = "birthday_zoned_auto")
		public ZonedDateTime zonedDateTimeAuto;

		@TimeZoneStorage(TimeZoneStorageType.NORMALIZE)
		@Column(name = "birthtime_offset_normalized")
		public OffsetTime offsetTimeNormalized;

		@TimeZoneStorage(TimeZoneStorageType.NORMALIZE)
		@Column(name = "birthday_offset_normalized")
		public OffsetDateTime offsetDateTimeNormalized;

		@TimeZoneStorage(TimeZoneStorageType.NORMALIZE)
		@Column(name = "birthday_zoned_normalized")
		public ZonedDateTime zonedDateTimeNormalized;

		@TimeZoneStorage(TimeZoneStorageType.NORMALIZE_UTC)
		@Column(name = "birthtime_offset_utc")
		public OffsetTime offsetTimeNormalizedUtc;

		@TimeZoneStorage(TimeZoneStorageType.NORMALIZE_UTC)
		@Column(name = "birthday_offset_utc")
		public OffsetDateTime offsetDateTimeNormalizedUtc;

		@TimeZoneStorage(TimeZoneStorageType.NORMALIZE_UTC)
		@Column(name = "birthday_zoned_utc")
		private ZonedDateTime zonedDateTimeNormalizedUtc;

		public TimeZoneStorageEntity() {
		}

		public TimeZoneStorageEntity(Integer id, OffsetTime offsetTime, OffsetDateTime offsetDateTime, ZonedDateTime zonedDateTime) {
			this.id = id;
			this.offsetTimeColumn = offsetTime;
			this.offsetDateTimeColumn = offsetDateTime;
			this.zonedDateTimeColumn = zonedDateTime;
			this.offsetTimeAuto = offsetTime;
			this.offsetDateTimeAuto = offsetDateTime;
			this.zonedDateTimeAuto = zonedDateTime;
			this.offsetTimeNormalized = offsetTime;
			this.offsetDateTimeNormalized = offsetDateTime;
			this.zonedDateTimeNormalized = zonedDateTime;
			this.offsetTimeNormalizedUtc = offsetTime;
			this.offsetDateTimeNormalizedUtc = offsetDateTime;
			this.zonedDateTimeNormalizedUtc = zonedDateTime;
		}
	}
}
