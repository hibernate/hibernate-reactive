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
import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.Test;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertWithTruncationThat;

/**
 * Test all the types and lifecycle callbacks that we expect to work on all supported DBs
 */
public class BasicTypesAndCallbacksForAllDBsTest extends BaseReactiveTest {

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( Basic.class );
	}

	private void testField(VertxTestContext context, Basic original, Consumer<Basic> consumer) {
		test(
				context, getSessionFactory()
						.withTransaction( (s, t) -> s.persist( original ) )
						.thenCompose( v -> getSessionFactory().withSession( s -> s
								.find( Basic.class, original.id )
								.thenAccept( found -> {
									assertThat( found ).isNotNull();
									consumer.accept( found );
								} ) ) )
		);
	}

	@Test
	public void testStringType(VertxTestContext context) {
		String string = "Hello world!";
		Basic basic = new Basic();
		basic.string = string;

		testField( context, basic, found -> assertThat( found.string ).isEqualTo( string ) );
	}

	@Test
	public void testIntegerType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.primitiveInt = Integer.MIN_VALUE;
		basic.fieldInteger = Integer.MAX_VALUE;

		testField(
				context, basic, found -> {
					assertThat( found.primitiveInt ).isEqualTo( Integer.MIN_VALUE );
					assertThat( found.fieldInteger ).isEqualTo( Integer.MAX_VALUE );
				}
		);
	}

	@Test
	public void testLongType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.primitiveLong = Long.MIN_VALUE;
		basic.fieldLong = Long.MAX_VALUE;

		testField(
				context, basic, found -> {
					assertThat( found.primitiveLong ).isEqualTo( Long.MIN_VALUE );
					assertThat( found.fieldLong ).isEqualTo( Long.MAX_VALUE );
				}
		);
	}

	@Test
	public void testFloatType(VertxTestContext context) {
		float primitiveFloat = 10.02f;
		Float fieldFloat = 12.562f;

		Basic basic = new Basic();
		basic.primitiveFloat = 10.02f;
		basic.fieldFloat = 12.562f;

		testField(
				context, basic, found -> {
					assertThat( found.primitiveFloat ).isEqualTo( primitiveFloat );
					assertThat( found.fieldFloat ).isEqualTo( fieldFloat );
				}
		);
	}

	@Test
	public void testDoubleType(VertxTestContext context) {
		double primitiveDouble = 10.02d;
		Double fieldDouble = 16.2d;

		Basic basic = new Basic();
		basic.primitiveDouble = primitiveDouble;
		basic.fieldDouble = fieldDouble;

		testField(
				context, basic, found -> {
					assertThat( found.primitiveDouble ).isEqualTo( primitiveDouble );
					assertThat( found.fieldDouble ).isEqualTo( fieldDouble );
				}
		);
	}

	@Test
	public void testBooleanType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.primitiveBoolean = true;
		basic.fieldBoolean = Boolean.FALSE;
		basic.booleanTrueFalse = Boolean.FALSE;
		basic.booleanYesNo = Boolean.FALSE;
		basic.booleanNumeric = Boolean.FALSE;

		testField(
				context, basic, found -> {
					assertThat( found.primitiveBoolean ).isEqualTo( true );
					assertThat( found.fieldBoolean ).isEqualTo( Boolean.FALSE );
					assertThat( found.booleanTrueFalse ).isEqualTo( Boolean.FALSE );
					assertThat( found.booleanYesNo ).isEqualTo( Boolean.FALSE );
					assertThat( found.booleanNumeric ).isEqualTo( Boolean.FALSE );
				}
		);
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

		testField(
				context, basic, found -> {
					assertThat( found.primitiveByte ).isEqualTo( primitiveByte );
					assertThat( Objects.deepEquals( primitiveBytes, found.primitiveBytes ) ).isTrue();
					assertThat( found.fieldByte ).isEqualTo( fieldByte );
				}
		);
	}

	@Test
	public void testURL(VertxTestContext context) throws Exception {
		URL url = new URL( "http://example.com/" );
		Basic basic = new Basic();
		basic.url = url;

		testField( context, basic, found -> assertThat( found.url ).isEqualTo( url ) );
	}

	@Test
	public void testDateType(VertxTestContext context) {
		Date date = new Date( 2000, Calendar.JANUARY, 1 );
		Basic basic = new Basic();
		basic.date = date;

		testField( context, basic, found -> assertThat( found.date ).isEqualTo( date ) );
	}

	@Test
	public void testDateAsTimestampType(VertxTestContext context) {
		Date date = new Date();
		Basic basic = new Basic();
		basic.dateAsTimestamp = date;

		testField(
				context, basic, found -> {
					assertThat( found.dateAsTimestamp ).isInstanceOf( Timestamp.class );
					assertThat( found.dateAsTimestamp ).isEqualTo( new Timestamp( date.getTime() ) );
				}
		);
	}

	@Test
	public void testTimeZoneType(VertxTestContext context) {
		TimeZone timeZone = TimeZone.getTimeZone( "America/Los_Angeles" );
		Basic basic = new Basic();
		basic.timeZone = timeZone;

		testField( context, basic, found -> assertThat( found.timeZone ).isEqualTo( basic.timeZone ) );
	}

	@Test
	public void testCalendarAsDateType(VertxTestContext context) {
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.set( Calendar.DAY_OF_MONTH, 15 );
		calendar.set( Calendar.MONTH, 7 );
		calendar.set( Calendar.YEAR, 2002 );

		// TemporalType#Date only deals with year/month/day
		int expectedYear = calendar.get( Calendar.YEAR );
		int expectedMonth = calendar.get( Calendar.MONTH );
		int expectedDay = calendar.get( Calendar.DAY_OF_MONTH );

		Basic basic = new Basic();
		basic.calendarAsDate = calendar;

		testField(
				context, basic, found -> {
					assertThat( found.calendarAsDate.get( Calendar.DAY_OF_MONTH ) ).isEqualTo( expectedDay );
					assertThat( found.calendarAsDate.get( Calendar.MONTH ) ).isEqualTo( expectedMonth );
					assertThat( found.calendarAsDate.get( Calendar.YEAR ) ).isEqualTo( expectedYear );
				}
		);
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

		testField(
				context, basic, found -> {
					String actual = format( found.calendarAsTimestamp );
					assertThat( actual ).isEqualTo( expected );
				}
		);
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

		testField( context, basic, found -> assertThat( found.localDate ).isEqualTo( now ) );
	}

	@Test
	public void testLocalDateTimeType(VertxTestContext context) {
		// @Temporal(TemporalType.TIMESTAMP) is stored to the mills by Hibernate
		LocalDateTime now = LocalDateTime.now()
				// required for JDK 15+
				.truncatedTo( ChronoUnit.MILLIS );
		Basic basic = new Basic();
		basic.localDateTime = now;

		testField( context, basic, found -> assertThat( found.localDateTime ).isEqualTo( now ) );
	}

	@Test
	public void testEnumType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.cover = Cover.HARDER;
		basic.coverAsOrdinal = Cover.HARD;
		basic.coverAsString = Cover.SOFT;

		testField(
				context, basic, found -> {
					assertThat( found.cover ).isEqualTo( Cover.HARDER );
					assertThat( found.coverAsOrdinal ).isEqualTo( Cover.HARD );
					assertThat( found.coverAsString ).isEqualTo( Cover.SOFT );
				}
		);
	}

	@Test
	public void testEmbeddableType(VertxTestContext context) {
		Embed embed = new Embed( "one", "two" );
		Basic basic = new Basic();
		basic.embed = embed;

		testField(
				context, basic, found -> {
					assertThat( found.embed ).isEqualTo( embed );
				}
		);
	}

	@Test
	public void testBigIntegerWithConverterType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.bigIntegerAsString = BigInteger.TEN;

		testField(
				context, basic, found -> {
					assertThat( found.bigIntegerAsString.floatValue() ).isEqualTo( BigInteger.TEN.floatValue() );
				}
		);
	}

	@Test
	public void testBigDecimalWithUserType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.bigDecimalAsString = BigDecimal.TEN;

		testField(
				context, basic, found -> {
					assertThat( found.bigDecimalAsString.floatValue() ).isEqualTo( BigInteger.TEN.floatValue() );
				}
		);
	}

	@Test
	public void testSerializableType(VertxTestContext context) {
		String[] thing = { "hello", "world" };

		Basic basic = new Basic();
		basic.thing = thing;

		testField(
				context, basic, found -> {
					assertThat( found.thing instanceof String[] ).isTrue();
					assertThat( Objects.deepEquals( thing, found.thing ) ).isTrue();
				}
		);
	}

	@Test
	public void testUUIDType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.uuid = UUID.fromString( "123e4567-e89b-42d3-a456-556642440000" );

		testField( context, basic, found -> assertThat( found.uuid ).isEqualTo( basic.uuid ) );
	}

	@Test
	public void testDecimalType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.bigDecimal = new BigDecimal( "12.12" );

		testField(
				context,
				basic,
				found -> assertThat( found.bigDecimal.floatValue() ).isEqualTo( basic.bigDecimal.floatValue() )
		);
	}

	@Test
	public void testBigIntegerType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.bigInteger = BigInteger.valueOf( 123L );

		testField( context, basic, found -> assertThat( found.bigInteger ).isEqualTo( basic.bigInteger ) );
	}

	@Test
	public void testLocalTimeType(VertxTestContext context) {
		Basic basic = new Basic();
		basic.localTime = LocalTime.now();

		testField( context, basic, found -> assertThat( found.localTime.truncatedTo( ChronoUnit.MINUTES ) )
				.isEqualTo( basic.localTime.truncatedTo( ChronoUnit.MINUTES ) )
		);
	}

	@Test
	public void testDateAsTimeType(VertxTestContext context) {
		Date date = new Date();

		Basic basic = new Basic();
		basic.dateAsTime = date;

		testField(
				context, basic, found -> {
					SimpleDateFormat timeSdf = new SimpleDateFormat( "HH:mm:ss" );
					assertThat( found.dateAsTime instanceof Time ).isTrue();
					assertThat( timeSdf.format( found.dateAsTime ) ).isEqualTo( timeSdf.format( date ) );
				}
		);
	}

	@Test
	public void testDuration(VertxTestContext context) {
		Basic basic = new Basic();
		basic.duration = Duration.ofMillis( 1894657L );

		testField(
				context, basic, found -> {
					assertThat( found.duration ).isNotNull();
					assertThat( found.duration ).isEqualTo( basic.duration );
				}
		);
	}

	@Test
	@DisabledFor(value = DB2, reason = "java.sql.SQLException: An error occurred with a DB2 operation, SQLCODE=-180  SQLSTATE=22007 in insert query")
	public void testInstant(VertxTestContext context) {
		Basic basic = new Basic();
		basic.instant = Instant.now();

		testField(
				context, basic, found -> {
					assertThat( found.instant ).isNotNull();
					assertWithTruncationThat( found.instant ).isEqualTo( basic.instant );
				}
		);
	}

	@Test
	public void testCallbacksAndVersioning(VertxTestContext context) {
		Basic parent = new Basic( "Parent" );
		Basic basik = new Basic( "Hello World" );
		basik.cover = Cover.HARD;
		basik.parent = parent;

		test(
				context, getSessionFactory()
						.withSession( s -> s.persist( basik.parent ).thenCompose( v -> s.persist( basik ) )
								.thenAccept( v -> assertThat( basik.prePersisted && !basik.postPersisted ).isTrue() )
								.thenAccept( v -> assertThat( basik.parent.prePersisted && !basik.parent.postPersisted ).isTrue() )
								.thenCompose( v -> s.flush() )
								.thenAccept( v -> assertThat( basik.prePersisted && basik.postPersisted ).isTrue() )
								.thenAccept( v -> assertThat( basik.parent.prePersisted && basik.parent.postPersisted ).isTrue() )
						)
						.thenCompose( v -> getSessionFactory().withSession( s2 -> s2
								.find( Basic.class, basik.getId() )
								.thenCompose( basic -> {
									assertThat( basic ).isNotNull();
									assertThat( basic.loaded ).isTrue();
									assertThat( basic.string ).isEqualTo( basik.string );
									assertThat( basic.cover ).isEqualTo( basik.cover );
									assertThat( basic.version ).isEqualTo( 0 );

									basic.string = "Goodbye";
									basic.cover = Cover.SOFT;
									basic.parent = new Basic( "New Parent" );
									return s2.persist( basic.parent )
											.thenCompose( vv -> s2.flush() )
											.thenAccept( vv -> {
												assertThat( basic ).isNotNull();
												assertThat( basic.postUpdated && basic.preUpdated ).isTrue();
												assertThat( basic.postPersisted && basic.prePersisted ).isFalse();
												assertThat( basic.parent.postPersisted && basic.parent.prePersisted ).isTrue();
												assertThat( basic.version ).isEqualTo( 1 );
											} );
								} )
						) )
						.thenCompose( v -> getSessionFactory().withSession( s3 -> s3
								.find( Basic.class, basik.getId() )
								.thenCompose( basic -> {
									assertThat( basic.postUpdated && basic.preUpdated ).isFalse();
									assertThat( basic.postPersisted && basic.prePersisted ).isFalse();
									assertThat( basic.version ).isEqualTo( 1 );
									assertThat( basic.string ).isEqualTo( "Goodbye" );
									return s3.remove( basic )
											.thenAccept( vv -> assertThat( !basic.postRemoved && basic.preRemoved ).isTrue() )
											.thenCompose( vv -> s3.flush() )
											.thenAccept( vv -> assertThat( basic.postRemoved && basic.preRemoved ).isTrue() );
								} )
						) )
						.thenCompose( v -> getSessionFactory().withSession( s4 -> s4
								.find( Basic.class, basik.getId() )
								.thenAccept( result -> assertThat( result ).isNull() )
						) )
		);
	}

	enum Cover {HARDER, HARD, SOFT}

	@Embeddable
	public static class Embed {
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
		Boolean booleanYesNo;

		@Convert(converter = org.hibernate.type.TrueFalseConverter.class)
		Boolean booleanTrueFalse;

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

		@Type(org.hibernate.reactive.types.BigDecimalAsString.class)
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

		@Column(name = "dessimal")
		BigDecimal bigDecimal;
		@Column(name = "inteja")
		BigInteger bigInteger;

		@Column(name = "localtyme")
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
