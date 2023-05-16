/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Consumer;

import org.hibernate.annotations.Type;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Converter;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostRemove;
import jakarta.persistence.PostUpdate;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreRemove;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertWithTruncationThat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test all the types and lifecycle callbacks that we expect to work on all supported DBs
 */
public class BasicTypesAndCallbacksForAllDBsTest extends BaseReactiveTest {

	//Db2: testUUIDType throws NoStackTraceThrowable: parameter of type BufferImpl cannot be coerced to ByteBuf
	@RegisterExtension
	public final DBSelectionExtension skip = DBSelectionExtension.skipTestsFor( DB2 );

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( Basic.class );
	}

	private void testField(VertxTestContext context, Basic original, Consumer<Basic> consumer) {
		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.persist( original ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.find( Basic.class, original.id )
						.thenAccept( found -> {
							assertNotNull( found );
							consumer.accept( found );
						} ) ) )
		);
	}

	@Test
	public void testStringType(VertxTestContext context) {
		String string = "Hello world!";
		Basic basic = new Basic();
		basic.string = string;

		testField( context, basic, found -> assertEquals( string, found.string ) );
	}

	@Test
	public void testIntegerType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.primitiveInt = Integer.MIN_VALUE;
		basic.fieldInteger = Integer.MAX_VALUE;

		testField( context, basic, found -> {
			assertEquals( Integer.MIN_VALUE, found.primitiveInt );
			assertEquals( Integer.MAX_VALUE, found.fieldInteger );
		} );
	}

	@Test
	public void testLongType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.primitiveLong = Long.MIN_VALUE;
		basic.fieldLong = Long.MAX_VALUE;

		testField( context, basic, found -> {
			assertEquals( Long.MIN_VALUE, found.primitiveLong );
			assertEquals( Long.MAX_VALUE, found.fieldLong );
		} );
	}

	@Test
	public void testFloatType(VertxTestContext context) {
		float primitiveFloat = 10.02f;
		Float fieldFloat = 12.562f;

		Basic basic = new Basic();
		basic.primitiveFloat = 10.02f;
		basic.fieldFloat = 12.562f;

		testField( context, basic, found -> {
			assertEquals( primitiveFloat, found.primitiveFloat );
			assertEquals( fieldFloat, found.fieldFloat );
		} );
	}

	@Test
	public void testDoubleType(VertxTestContext context) {
		double primitiveDouble = 10.02d;
		Double fieldDouble = 16.2d;

		Basic basic = new Basic();
		basic.primitiveDouble = primitiveDouble;
		basic.fieldDouble = fieldDouble;

		testField( context, basic, found -> {
			assertEquals( primitiveDouble, found.primitiveDouble );
			assertEquals( fieldDouble, found.fieldDouble );
		} );
	}

	@Test
	public void testBooleanType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.primitiveBoolean = true;
		basic.fieldBoolean = Boolean.FALSE;
		basic.booleanTrueFalse = Boolean.FALSE;
		basic.booleanYesNo = Boolean.FALSE;
		basic.booleanNumeric = Boolean.FALSE;

		testField( context, basic, found -> {
			assertEquals( true, found.primitiveBoolean );
			assertEquals( Boolean.FALSE, found.fieldBoolean );
			assertEquals( Boolean.FALSE, found.booleanTrueFalse );
			assertEquals( Boolean.FALSE, found.booleanYesNo );
			assertEquals( Boolean.FALSE, found.booleanNumeric );
		} );
	}

	@Test
	public void testBytesType(VertxTestContext context) {
		byte primitiveByte = 'D';
		byte[] primitiveBytes = "This too shall pass".getBytes();
		Byte fieldByte = Byte.valueOf( "4" );

		Basic basic = new Basic();
		basic.primitiveByte = primitiveByte;
		basic.primitiveBytes = primitiveBytes;
		basic.fieldByte = fieldByte;

		testField( context, basic, found -> {
			assertEquals( primitiveByte, found.primitiveByte );
			assertTrue( Objects.deepEquals( primitiveBytes, found.primitiveBytes ) );
			assertEquals( fieldByte, found.fieldByte );
		} );
	}

	@Test
	public void testURL(VertxTestContext context) throws Exception {
		URL url = new URL( "http://example.com/" );
		Basic basic = new Basic();
		basic.url = url;

		testField( context, basic, found -> assertEquals( url, found.url ) );
	}

	@Test
	public void testDateType(VertxTestContext context) {
		Date date = new Date( 2000, Calendar.JANUARY, 1 );
		Basic basic = new Basic();
		basic.date = date;

		testField( context, basic, found -> assertEquals( date, found.date ) );
	}

	@Test
	public void testDateAsTimestampType(VertxTestContext context) {
		Date date = new Date();
		Basic basic = new Basic();
		basic.dateAsTimestamp = date;

		testField( context, basic, found -> {
			assertTrue( found.dateAsTimestamp instanceof Timestamp );
			assertEquals( date, found.dateAsTimestamp );
		} );
	}

	@Test
	public void testTimeZoneType(VertxTestContext context) {
		TimeZone timeZone = TimeZone.getTimeZone( "America/Los_Angeles" );
		Basic basic = new Basic();
		basic.timeZone = timeZone;

		testField( context, basic, found -> assertEquals( basic.timeZone, found.timeZone ) );
	}

	@Test
	public void testCalendarAsDateType(VertxTestContext context) {
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.set( Calendar.DAY_OF_MONTH,  15);
		calendar.set( Calendar.MONTH,  7);
		calendar.set( Calendar.YEAR,  2002);

		// TemporalType#Date only deals with year/month/day
		int expectedYear = calendar.get( Calendar.YEAR );
		int expectedMonth = calendar.get( Calendar.MONTH );
		int expectedDay = calendar.get( Calendar.DAY_OF_MONTH );

		Basic basic = new Basic();
		basic.calendarAsDate = calendar;

		testField( context, basic, found -> {
			assertEquals( expectedDay, found.calendarAsDate.get( Calendar.DAY_OF_MONTH ) );
			assertEquals( expectedMonth, found.calendarAsDate.get( Calendar.MONTH ) );
			assertEquals( expectedYear, found.calendarAsDate.get( Calendar.YEAR ) );
		} );
	}

	@Test
	public void testCalendarAsTimestampType(VertxTestContext context) {
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.set( Calendar.DAY_OF_MONTH, 15 );
		calendar.set( Calendar.MONTH, 7 );
		calendar.set( Calendar.YEAR, 2002 );
		final String expected = format( calendar );
		Basic basic = new Basic();
		basic.calendarAsTimestamp = calendar;

		testField( context, basic, found -> {
			String actual = format( found.calendarAsTimestamp );
			assertEquals( expected, actual );
		} );
	}

	private static String format(Calendar calendar) {
		SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
		return sdf.format( calendar.getTime() );
	}

	@Test
	public void testLocalDateType(VertxTestContext context) {
		LocalDate now = LocalDate.now();
		Basic basic = new Basic();
		basic.localDate = now;

		testField( context, basic, found -> assertEquals( now, found.localDate ) );
	}

	@Test
	public void testLocalDateTimeType(VertxTestContext context) {
		// @Temporal(TemporalType.TIMESTAMP) is stored to the mills by Hibernate
		LocalDateTime now = LocalDateTime.now()
				// required for JDK 15+
				.truncatedTo( ChronoUnit.MILLIS );
		Basic basic = new Basic();
		basic.localDateTime = now;

		testField( context, basic, found -> assertEquals( now, found.localDateTime ) );
	}

	@Test
	public void testEnumType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.cover = Cover.HARDER;
		basic.coverAsOrdinal = Cover.HARD;
		basic.coverAsString = Cover.SOFT;

		testField( context, basic, found -> {
			assertEquals( Cover.HARDER, found.cover );
			assertEquals( Cover.HARD, found.coverAsOrdinal );
			assertEquals( Cover.SOFT, found.coverAsString );
		} );
	}

	@Test
	public void testEmbeddableType(VertxTestContext context) {
		Embed embed = new Embed( "one", "two" );
		Basic basic = new Basic();
		basic.embed = embed;

		testField( context, basic, found -> {
			assertEquals( embed, found.embed );
		} );
	}

	@Test
	public void testBigIntegerWithConverterType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.bigIntegerAsString = BigInteger.TEN;

		testField( context, basic, found -> {
			assertEquals( BigInteger.TEN.floatValue(), found.bigIntegerAsString.floatValue() );
		} );
	}

	@Test
	public void testBigDecimalWithUserType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.bigDecimalAsString = BigDecimal.TEN;

		testField( context, basic, found -> {
			assertEquals( BigInteger.TEN.floatValue(), found.bigDecimalAsString.floatValue() );
		} );
	}

	@Test
	public void testSerializableType(VertxTestContext context) {
		String[] thing = { "hello", "world" };

		Basic basic = new Basic();
		basic.thing = thing;

		testField( context, basic, found -> {
			assertTrue( found.thing instanceof String[] );
			assertTrue( Objects.deepEquals( thing, found.thing ) );
		} );
	}

	@Test
	public void testUUIDType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.uuid = UUID.fromString( "123e4567-e89b-42d3-a456-556642440000" );

		testField( context, basic, found -> assertEquals( basic.uuid, found.uuid ) );
	}

	@Test
	public void testDecimalType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.bigDecimal = new BigDecimal( "12.12" );

		testField( context, basic, found -> assertEquals( basic.bigDecimal.floatValue(), found.bigDecimal.floatValue() ) );
	}

	@Test
	public void testBigIntegerType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.bigInteger = BigInteger.valueOf( 123L);

		testField( context, basic, found -> assertEquals( basic.bigInteger, found.bigInteger ) );
	}

	@Test
	@Disabled // Fail for MSSQL because the value changes before it's saved on the db. This also fails for ORM
	public void testLocalTimeType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.localTime = LocalTime.now();

		testField( context, basic, found -> assertEquals(
				basic.localTime.truncatedTo( ChronoUnit.MINUTES ),
				found.localTime.truncatedTo( ChronoUnit.MINUTES )
		) );
	}

	@Test
	public void testDateAsTimeType(VertxTestContext context) {
		Date date = new Date();

		Basic basic = new Basic();
		basic.dateAsTime = date;

		testField( context, basic, found -> {
			SimpleDateFormat timeSdf = new SimpleDateFormat( "HH:mm:ss" );
			assertTrue( found.dateAsTime instanceof Time);
			assertEquals( timeSdf.format( date ), timeSdf.format( found.dateAsTime ) );
		} );
	}

	@Test
	public void testDuration(VertxTestContext context) {
		Basic basic = new Basic();
		basic.duration = Duration.ofMillis( 1894657L );

		testField( context, basic, found -> {
			assertNotNull( found.duration );
			assertEquals( basic.duration, found.duration );
		} );
	}

	@Test
	public void testInstant(VertxTestContext context) {
		Basic basic = new Basic();
		basic.instant = Instant.now();

		testField( context, basic, found -> {
			assertNotNull( found.instant );
			assertWithTruncationThat( found.instant ).isEqualTo( basic.instant );
		} );
	}

	@Test
	public void testCallbacksAndVersioning(VertxTestContext context) {
		Basic parent = new Basic( "Parent" );
		Basic basik = new Basic( "Hello World" );
		basik.cover = Cover.HARD;
		basik.parent = parent;

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( basik.parent ).thenCompose( v -> s.persist( basik ) )
								.thenAccept( v -> assertTrue( basik.prePersisted && !basik.postPersisted ) )
								.thenAccept( v -> assertTrue( basik.parent.prePersisted && !basik.parent.postPersisted ) )
								.thenCompose( v -> s.flush() )
								.thenAccept( v -> assertTrue( basik.prePersisted && basik.postPersisted ) )
								.thenAccept( v -> assertTrue( basik.parent.prePersisted && basik.parent.postPersisted ) )
						)
						.thenCompose( v -> openSession()
								.thenCompose( s2 -> s2.find( Basic.class, basik.getId() )
										.thenCompose( basic -> {
											assertNotNull( basic );
											assertTrue( basic.loaded );
											assertEquals( basic.string, basik.string );
											assertEquals( basic.cover, basik.cover );
											assertEquals( basic.version, 0 );

											basic.string = "Goodbye";
											basic.cover = Cover.SOFT;
											basic.parent = new Basic( "New Parent" );
											return s2.persist( basic.parent )
													.thenCompose( vv -> s2.flush() )
													.thenAccept( vv -> {
														assertNotNull( basic );
														assertTrue( basic.postUpdated && basic.preUpdated );
														assertFalse( basic.postPersisted && basic.prePersisted );
														assertTrue( basic.parent.postPersisted && basic.parent.prePersisted );
														assertEquals( basic.version, 1 );
													} );
										} )
								) )
						.thenCompose( v -> openSession()
								.thenCompose( s3 -> s3.find( Basic.class, basik.getId() )
										.thenCompose( basic -> {
											assertFalse( basic.postUpdated && basic.preUpdated );
											assertFalse( basic.postPersisted && basic.prePersisted );
											assertEquals( basic.version, 1 );
											assertEquals( basic.string, "Goodbye" );
											return s3.remove( basic )
													.thenAccept( vv -> assertTrue( !basic.postRemoved && basic.preRemoved ) )
													.thenCompose( vv -> s3.flush() )
													.thenAccept( vv -> assertTrue( basic.postRemoved && basic.preRemoved ) );
										} )
								) )
						.thenCompose( v -> openSession()
								.thenCompose( s4 -> s4.find( Basic.class, basik.getId() ) )
								.thenAccept( Assertions::assertNull ) )
		);
	}

	enum Cover {HARDER, HARD, SOFT}

	@Embeddable
	static class Embed {
		String one;
		String two;

		public Embed(String one, String two) {
			this.one = one;
			this.two = two;
		}

		Embed() {
		}

		public String getOne() {
			return one;
		}

		public void setOne(String one) {
			this.one = one;
		}

		public String getTwo() {
			return two;
		}

		public void setTwo(String two) {
			this.two = two;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Embed embed = (Embed) o;
			return Objects.equals( one, embed.one ) &&
					Objects.equals( two, embed.two );
		}

		@Override
		public int hashCode() {
			return Objects.hash( one, two );
		}
	}

	@Entity(name = "Basic")
	@Table(name = "Basic")
	private static class Basic {

		@Id
		@GeneratedValue
		Integer id;
		@Version
		Integer version;
		String string;

		boolean primitiveBoolean;
		int primitiveInt;
		long primitiveLong;
		float primitiveFloat;
		double primitiveDouble;
		byte primitiveByte;
		byte[] primitiveBytes;

		Boolean fieldBoolean;
		Integer fieldInteger;
		Long fieldLong;
		Float fieldFloat;
		Double fieldDouble;
		Byte fieldByte;

		@Convert(converter = org.hibernate.type.YesNoConverter.class)
		Boolean booleanTrueFalse;

		@Convert(converter = org.hibernate.type.TrueFalseConverter.class)
		Boolean booleanYesNo;

		@Convert(converter = org.hibernate.type.NumericBooleanConverter.class)
		Boolean booleanNumeric;

		URL url;

		TimeZone timeZone;

		@Temporal(TemporalType.DATE)
		@Column(name = "`date`")
		Date date;
		@Temporal(TemporalType.TIMESTAMP)
		Date dateAsTimestamp;

		@Temporal(TemporalType.DATE)
		Calendar calendarAsDate;
		@Temporal(TemporalType.TIMESTAMP)
		Calendar calendarAsTimestamp;

		@Column(name = "localdayte")
		LocalDate localDate;
		@Column(name = "alocalDT")
		LocalDateTime localDateTime;

		@Convert(converter = BigIntegerAsString.class)
		BigInteger bigIntegerAsString;

		@Type(BigDecimalAsString.class)
		BigDecimal bigDecimalAsString;

		Cover cover;
		@Enumerated(value = EnumType.STRING)
		Cover coverAsString;
		@Enumerated(value = EnumType.ORDINAL)
		Cover coverAsOrdinal;

		@ManyToOne(fetch = FetchType.LAZY)
		Basic parent;

		Embed embed;

		@jakarta.persistence.Basic
		Serializable thing;

		UUID uuid;

		@Column(name="dessimal")
		BigDecimal bigDecimal;
		@Column(name="inteja")
		BigInteger bigInteger;

		@Column(name="localtyme")
		private LocalTime localTime;
		@Temporal(TemporalType.TIME)
		Date dateAsTime;

		Instant instant;

		Duration duration;

		@Transient
		boolean prePersisted;
		@Transient
		boolean postPersisted;
		@Transient
		boolean preUpdated;
		@Transient
		boolean postUpdated;
		@Transient
		boolean postRemoved;
		@Transient
		boolean preRemoved;
		@Transient
		boolean loaded;

		public Basic() {
		}

		public Basic(String string) {
			this.string = string;
		}

		public Basic(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		@PrePersist
		void prePersist() {
			prePersisted = true;
		}

		@PostPersist
		void postPersist() {
			postPersisted = true;
		}

		@PreUpdate
		void preUpdate() {
			preUpdated = true;
		}

		@PostUpdate
		void postUpdate() {
			postUpdated = true;
		}

		@PreRemove
		void preRemove() {
			preRemoved = true;
		}

		@PostRemove
		void postRemove() {
			postRemoved = true;
		}

		@PostLoad
		void postLoad() {
			loaded = true;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Basic basic = (Basic) o;
			return Objects.equals( string, basic.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}


	@Converter
	private static class BigIntegerAsString implements AttributeConverter<BigInteger, String> {
		@Override
		public String convertToDatabaseColumn(BigInteger attribute) {
			return attribute == null ? null : attribute.toString( 2 );
		}

		@Override
		public BigInteger convertToEntityAttribute(String string) {
			return string == null ? null : new BigInteger( string, 2 );
		}
	}

}
