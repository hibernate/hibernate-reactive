/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import io.vertx.ext.unit.TestContext;
import org.hibernate.annotations.Type;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.junit.After;
import org.junit.Test;

import javax.persistence.*;
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
import java.util.*;
import java.util.function.Consumer;

/**
 * Test all the types and lifecycle callbacks that we expect to work on all supported DBs
 */
public class BasicTypesAndCallbacksForAllDBsTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Basic.class );
		return configuration;
	}

	@After
	public void deleteTable(TestContext context) {
		test( context, deleteEntities( "Basic" ) );
	}

	private void testField(TestContext context, Basic original, Consumer<Basic> consumer) {
		test( context, getSessionFactory()
				.withTransaction( (s, t) -> s.persist( original ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.find( Basic.class, original.id )
						.thenAccept( found -> {
							context.assertNotNull( found );
							consumer.accept( found );
						} ) ) )
		);
	}

	@Test
	public void testStringType(TestContext context) {
		String string = "Hello world!";
		Basic basic = new Basic();
		basic.string = string;

		testField( context, basic, found -> context.assertEquals( string, found.string ) );
	}

	@Test
	public void testIntegerType(TestContext context) {
		Basic basic = new Basic();
		basic.primitiveInt = Integer.MIN_VALUE;
		basic.fieldInteger = Integer.MAX_VALUE;

		testField( context, basic, found -> {
			context.assertEquals( Integer.MIN_VALUE, found.primitiveInt );
			context.assertEquals( Integer.MAX_VALUE, found.fieldInteger );
		} );
	}

	@Test
	public void testLongType(TestContext context) {
		Basic basic = new Basic();
		basic.primitiveLong = Long.MIN_VALUE;
		basic.fieldLong = Long.MAX_VALUE;

		testField( context, basic, found -> {
			context.assertEquals( Long.MIN_VALUE, found.primitiveLong );
			context.assertEquals( Long.MAX_VALUE, found.fieldLong );
		} );
	}

	@Test
	public void testFloatType(TestContext context) {
		float primitiveFloat = 10.02f;
		Float fieldFloat = 12.562f;

		Basic basic = new Basic();
		basic.primitiveFloat = 10.02f;
		basic.fieldFloat = 12.562f;

		testField( context, basic, found -> {
			context.assertEquals( primitiveFloat, found.primitiveFloat );
			context.assertEquals( fieldFloat, found.fieldFloat );
		} );
	}

	@Test
	public void testDoubleType(TestContext context) {
		double primitiveDouble = 10.02d;
		Double fieldDouble = 16.2d;

		Basic basic = new Basic();
		basic.primitiveDouble = primitiveDouble;
		basic.fieldDouble = fieldDouble;

		testField( context, basic, found -> {
			context.assertEquals( primitiveDouble, found.primitiveDouble );
			context.assertEquals( fieldDouble, found.fieldDouble );
		} );
	}

	@Test
	public void testBooleanType(TestContext context) {
		Basic basic = new Basic();
		basic.primitiveBoolean = true;
		basic.fieldBoolean = Boolean.FALSE;
		basic.booleanTrueFalse = Boolean.FALSE;
		basic.booleanYesNo = Boolean.FALSE;
		basic.booleanNumeric = Boolean.FALSE;

		testField( context, basic, found -> {
			context.assertEquals( true, found.primitiveBoolean );
			context.assertEquals( Boolean.FALSE, found.fieldBoolean );
			context.assertEquals( Boolean.FALSE, found.booleanTrueFalse );
			context.assertEquals( Boolean.FALSE, found.booleanYesNo );
			context.assertEquals( Boolean.FALSE, found.booleanNumeric );
		} );
	}

	@Test
	public void testBytesType(TestContext context) {
		byte primitiveByte = 'D';
		byte[] primitiveBytes = "This too shall pass".getBytes();
		Byte fieldByte = Byte.valueOf( "4" );

		Basic basic = new Basic();
		basic.primitiveByte = primitiveByte;
		basic.primitiveBytes = primitiveBytes;
		basic.fieldByte = fieldByte;

		testField( context, basic, found -> {
			context.assertEquals( primitiveByte, found.primitiveByte );
			context.assertTrue( Objects.deepEquals( primitiveBytes, found.primitiveBytes ) );
			context.assertEquals( fieldByte, found.fieldByte );
		} );
	}

	@Test
	public void testURL(TestContext context) throws Exception {
		URL url = new URL( "http://example.com/" );
		Basic basic = new Basic();
		basic.url = url;

		testField( context, basic, found -> context.assertEquals( url, found.url ) );
	}

	@Test
	public void testDateType(TestContext context) {
		Date date = new Date( 2000, Calendar.JANUARY, 1 );
		Basic basic = new Basic();
		basic.date = date;

		testField( context, basic, found -> context.assertEquals( date, found.date ) );
	}

	@Test
	public void testDateAsTimestampType(TestContext context) {
		Date date = new Date();
		Basic basic = new Basic();
		basic.dateAsTimestamp = date;

		testField( context, basic, found -> {
			context.assertTrue( found.dateAsTimestamp instanceof Timestamp );
			context.assertEquals( date, found.dateAsTimestamp );
		} );
	}

	@Test
	public void testTimeZoneType(TestContext context) {
		TimeZone timeZone = TimeZone.getTimeZone( "America/Los_Angeles" );
		Basic basic = new Basic();
		basic.timeZone = timeZone;

		testField( context, basic, found -> context.assertEquals( basic.timeZone, found.timeZone ) );
	}

	@Test
	public void testCalendarAsDateType(TestContext context) {
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
			context.assertEquals( expectedDay, found.calendarAsDate.get( Calendar.DAY_OF_MONTH ) );
			context.assertEquals( expectedMonth, found.calendarAsDate.get( Calendar.MONTH ) );
			context.assertEquals( expectedYear, found.calendarAsDate.get( Calendar.YEAR ) );
		} );
	}

	@Test
	public void testCalendarAsTimestampType(TestContext context) {
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.set( Calendar.DAY_OF_MONTH, 15 );
		calendar.set( Calendar.MONTH, 7 );
		calendar.set( Calendar.YEAR, 2002 );
		final String expected = format( calendar );
		Basic basic = new Basic();
		basic.calendarAsTimestamp = calendar;

		testField( context, basic, found -> {
			String actual = format( found.calendarAsTimestamp );
			context.assertEquals( expected, actual );
		} );
	}

	private static String format(Calendar calendar) {
		SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
		return sdf.format( calendar.getTime() );
	}

	@Test
	public void testLocalDateType(TestContext context) {
		LocalDate now = LocalDate.now();
		Basic basic = new Basic();
		basic.localDate = now;

		testField( context, basic, found -> context.assertEquals( now, found.localDate ) );
	}

	@Test
	public void testLocalDateTimeType(TestContext context) {
		// @Temporal(TemporalType.TIMESTAMP) is stored to the mills by Hibernate
		LocalDateTime now = LocalDateTime.now()
				// required for JDK 15+
				.truncatedTo( ChronoUnit.MILLIS );
		Basic basic = new Basic();
		basic.localDateTime = now;

		testField( context, basic, found -> context.assertEquals( now, found.localDateTime ) );
	}

	@Test
	public void testEnumType(TestContext context) {
		Basic basic = new Basic();
		basic.cover = Cover.HARDER;
		basic.coverAsOrdinal = Cover.HARD;
		basic.coverAsString = Cover.SOFT;

		testField( context, basic, found -> {
			context.assertEquals( Cover.HARDER, found.cover );
			context.assertEquals( Cover.HARD, found.coverAsOrdinal );
			context.assertEquals( Cover.SOFT, found.coverAsString );
		} );
	}

	@Test
	public void testEmbeddableType(TestContext context) {
		Embed embed = new Embed( "one", "two" );
		Basic basic = new Basic();
		basic.embed = embed;

		testField( context, basic, found -> {
			context.assertEquals( embed, found.embed );
		} );
	}

	@Test
	public void testBigIntegerWithConverterType(TestContext context) {
		Basic basic = new Basic();
		basic.bigIntegerAsString = BigInteger.TEN;

		testField( context, basic, found -> {
			context.assertEquals( BigInteger.TEN.floatValue(), found.bigIntegerAsString.floatValue() );
		} );
	}

	@Test
	public void testBigDecimalWithUserType(TestContext context) {
		Basic basic = new Basic();
		basic.bigDecimalAsString = BigDecimal.TEN;

		testField( context, basic, found -> {
			context.assertEquals( BigInteger.TEN.floatValue(), found.bigDecimalAsString.floatValue() );
		} );
	}

	@Test
	public void testSerializableType(TestContext context) {
		String[] thing = { "hello", "world" };

		Basic basic = new Basic();
		basic.thing = thing;

		testField( context, basic, found -> {
			context.assertTrue( found.thing instanceof String[] );
			context.assertTrue( Objects.deepEquals( thing, found.thing ) );
		} );
	}

	@Test
	public void testUUIDType(TestContext context) {
		Basic basic = new Basic();
		basic.uuid = UUID.fromString( "123e4567-e89b-42d3-a456-556642440000" );

		testField( context, basic, found -> context.assertEquals( basic.uuid, found.uuid ) );
	}

	@Test
	public void testDecimalType(TestContext context) {
		Basic basic = new Basic();
		basic.bigDecimal = new BigDecimal( "12.12" );

		testField( context, basic, found -> context.assertEquals( basic.bigDecimal.floatValue(), found.bigDecimal.floatValue() ) );
	}

	@Test
	public void testBigIntegerType(TestContext context) {
		Basic basic = new Basic();
		basic.bigInteger = BigInteger.valueOf( 123L);

		testField( context, basic, found -> context.assertEquals( basic.bigInteger, found.bigInteger ) );
	}

	@Test
	public void testLocalTimeType(TestContext context) {
		Basic basic = new Basic();
		basic.localTime = LocalTime.now();

		testField( context, basic, found -> context.assertEquals(
				basic.localTime.truncatedTo( ChronoUnit.MINUTES ),
				found.localTime.truncatedTo( ChronoUnit.MINUTES )
		) );
	}

	@Test
	public void testDateAsTimeType(TestContext context) {
		Date date = new Date();

		Basic basic = new Basic();
		basic.dateAsTime = date;

		testField( context, basic, found -> {
			SimpleDateFormat timeSdf = new SimpleDateFormat( "HH:mm:ss" );
			context.assertTrue( found.dateAsTime instanceof Time);
			context.assertEquals( timeSdf.format( date ), timeSdf.format( found.dateAsTime ) );
		} );
	}

	@Test
	public void testDuration(TestContext context) {
		Basic basic = new Basic();
		basic.duration = Duration.ofMillis( 1894657L );

		testField( context, basic, found -> {
			context.assertTrue( found.duration instanceof Duration );
			context.assertEquals( basic.duration, found.duration );
		} );
	}

	@Test
	public void testInstant(TestContext context) {
		Basic basic = new Basic();
		basic.instant = Instant.now();

		testField( context, basic, found -> {
			context.assertTrue( found.instant instanceof Instant );
			// Without the truncated it fails with JDK 15+
			context.assertEquals( basic.instant.truncatedTo( ChronoUnit.MILLIS ),
								  found.instant.truncatedTo( ChronoUnit.MILLIS ) );
		} );
	}

	@Test
	public void testCallbacksAndVersioning(TestContext context) {
		Basic parent = new Basic( "Parent" );
		Basic basik = new Basic( "Hello World" );
		basik.cover = Cover.HARD;
		basik.parent = parent;

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( basik.parent ).thenCompose( v -> s.persist( basik ) )
								.thenAccept( v -> context.assertTrue( basik.prePersisted && !basik.postPersisted ) )
								.thenAccept( v -> context.assertTrue( basik.parent.prePersisted && !basik.parent.postPersisted ) )
								.thenCompose( v -> s.flush() )
								.thenAccept( v -> context.assertTrue( basik.prePersisted && basik.postPersisted ) )
								.thenAccept( v -> context.assertTrue( basik.parent.prePersisted && basik.parent.postPersisted ) )
						)
						.thenCompose( v -> openSession()
								.thenCompose( s2 -> s2.find( Basic.class, basik.getId() )
										.thenCompose( basic -> {
											context.assertNotNull( basic );
											context.assertTrue( basic.loaded );
											context.assertEquals( basic.string, basik.string );
											context.assertEquals( basic.cover, basik.cover );
											context.assertEquals( basic.version, 0 );

											basic.string = "Goodbye";
											basic.cover = Cover.SOFT;
											basic.parent = new Basic( "New Parent" );
											return s2.persist( basic.parent )
													.thenCompose( vv -> s2.flush() )
													.thenAccept( vv -> {
														context.assertNotNull( basic );
														context.assertTrue( basic.postUpdated && basic.preUpdated );
														context.assertFalse( basic.postPersisted && basic.prePersisted );
														context.assertTrue( basic.parent.postPersisted && basic.parent.prePersisted );
														context.assertEquals( basic.version, 1 );
													} );
										} )
								) )
						.thenCompose( v -> openSession()
								.thenCompose( s3 -> s3.find( Basic.class, basik.getId() )
										.thenCompose( basic -> {
											context.assertFalse( basic.postUpdated && basic.preUpdated );
											context.assertFalse( basic.postPersisted && basic.prePersisted );
											context.assertEquals( basic.version, 1 );
											context.assertEquals( basic.string, "Goodbye" );
											return s3.remove( basic )
													.thenAccept( vv -> context.assertTrue( !basic.postRemoved && basic.preRemoved ) )
													.thenCompose( vv -> s3.flush() )
													.thenAccept( vv -> context.assertTrue( basic.postRemoved && basic.preRemoved ) );
										} )
								) )
						.thenCompose( v -> openSession()
								.thenCompose( s4 -> s4.find( Basic.class, basik.getId() ) )
								.thenAccept( context::assertNull ) )
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

		@Type(type = "true_false")
		Boolean booleanTrueFalse;

		@Type(type = "yes_no")
		Boolean booleanYesNo;

		@Type(type = "numeric_boolean")
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

		@Type(type="org.hibernate.reactive.types.BigDecimalAsString")
		BigDecimal bigDecimalAsString;

		Cover cover;
		@Enumerated(value = EnumType.STRING)
		Cover coverAsString;
		@Enumerated(value = EnumType.ORDINAL)
		Cover coverAsOrdinal;

		@ManyToOne(fetch = FetchType.LAZY)
		Basic parent;

		Embed embed;

		@javax.persistence.Basic
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
