/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor( value = ORACLE, reason = "Vert.x does not support arrays for Oracle" )
public class JavaTypesArrayTest extends BaseReactiveTest {

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( Basic.class );
	}

	private void testField(
			VertxTestContext context, Basic
			original, Consumer<Basic> consumer) {
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( original ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.find( Basic.class, original.id )
						.thenAccept( found -> {
							assertNotNull( found );
							consumer.accept( found );
						} ) ) )
		);
	}

	@Test
	public void testStringArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		String[] dataArray = {"Hello world!", "Hello earth"};
		basic.stringArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.stringArray ) );
	}

	@Test
	public void testBooleanArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Boolean[] dataArray = {TRUE, FALSE, null, TRUE};
		basic.booleanArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.booleanArray ) );
	}

	@Test
	public void testPrimitiveBooleanArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		boolean[] dataArray = {true, false, true};
		basic.primitiveBooleanArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveBooleanArray ) );
	}

	@Test
	public void testIntegerArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Integer[] dataArray = {null, Integer.MIN_VALUE, 2, Integer.MAX_VALUE};
		basic.integerArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.integerArray ) );
	}

	@Test
	public void testPrimitiveIntegerArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		int[] dataArray = {1, 2, 3};
		basic.primitiveIntegerArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveIntegerArray ) );
	}

	@Test
	public void testLongArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Long[] dataArray = {Long.MIN_VALUE, Long.MAX_VALUE, 3L, null};
		basic.longArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.longArray ) );
	}

	@Test
	public void testPrimitiveLongArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		long[] dataArray = {Long.MIN_VALUE, Long.MAX_VALUE, 3L};
		basic.primitiveLongArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveLongArray ) );
	}

	@Test
	public void testFloatArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Float[] dataArray = {12.562f, null, 13.562f};
		basic.floatArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.floatArray ) );
	}

	@Test
	public void testPrimitiveFloatArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		float[] dataArray = {12.562f, 13.562f};
		basic.primitiveFloatArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveFloatArray ) );
	}

	@Test
	public void testDoubleArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Double[] dataArray = {12.562d, null, 13.562d};
		basic.doubleArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.doubleArray ) );
	}

	@Test
	public void testPrimitiveDoubleArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		double[] dataArray = {12.562d, 13.562d};
		basic.primitiveDoubleArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveDoubleArray ) );
	}

	@Test
	public void testUUIDArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		UUID[] dataArray = {
				UUID.fromString( "123e4567-e89b-42d3-a456-556642440000" ),
				UUID.fromString( "123e4567-e89b-42d3-a456-556642440001" ),
				UUID.fromString( "123e4567-e89b-42d3-a456-556642440002" )
		};
		basic.uuidArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.uuidArray ) );
	}

	@Test
	public void testEnumArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		AnEnum[] dataArray = {AnEnum.FOURTH, AnEnum.FIRST, AnEnum.THIRD};
		basic.enumArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.enumArray ) );
	}

	@Test
	public void testShortArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Short[] dataArray = {512, 112, null, 0};
		basic.shortArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.shortArray ) );
	}

	@Test
	public void testPrimitiveShortArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		short[] dataArray = {500, 32, -1};
		basic.primitiveShortArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveShortArray ) );
	}

	@Test
	public void testLocalDateArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		LocalDate date1 = LocalDate.now();
		LocalDate[] dataArray = {
				date1.plusDays( 5 ),
				date1.plusMonths( 4 ),
				date1.plusYears( 3 )
		};
		basic.localDateArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.localDateArray ) );
	}

	@Test
	public void testDateArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Date[] dataArray = {Calendar.getInstance().getTime(), Calendar.getInstance().getTime()};
		basic.dateArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.dateArray ) );
	}

	@Test
	public void testLocalTimeArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		LocalTime[] dataArray = {
				LocalTime.of( 0, 0, 0 ),
				LocalTime.of( 6, 15, 0 ),
				LocalTime.of( 12, 30, 0 ),
				LocalTime.of( 23, 59, 59 )
		};
		basic.localTimeArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.localTimeArray ) );
	}

	@Test
	public void testLocalDateTimeArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		LocalDateTime[] dataArray = {
				// Unix epoch start if you're in the UK
				LocalDateTime.of( 1970, Month.JANUARY, 1, 0, 0, 0, 0 ),
				// pre-Y2K
				LocalDateTime.of( 1999, Month.DECEMBER, 31, 23, 59, 59, 0 ),
				// We survived! Why was anyone worried?
				LocalDateTime.of( 2000, Month.JANUARY, 1, 0, 0, 0, 0 ),
				// Silence will fall!
				LocalDateTime.of( 2010, Month.JUNE, 26, 20, 4, 0, 0 )
		};
		basic.localDateTimeArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.localDateTimeArray ) );
	}

	@Test
	public void testBigIntegerArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		BigInteger[] dataArray = {BigInteger.TEN, BigInteger.ZERO};
		basic.bigIntegerArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.bigIntegerArray ) );
	}

	@Test
	public void testBigDecimalArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		BigDecimal[] dataArray = {BigDecimal.valueOf( 123384967L ), BigDecimal.ZERO};
		basic.bigDecimalArray = dataArray;

		testField( context, basic, found -> {
			assertEquals( dataArray.length, found.bigDecimalArray.length );
			assertEquals( dataArray[0].compareTo( found.bigDecimalArray[0] ), 0 );
			assertEquals( dataArray[1].compareTo( found.bigDecimalArray[1] ), 0 );
		} );
	}

	@Entity(name = "Basic")
	@Table(name = "Basic")
	private static class Basic {
		@Id
		@GeneratedValue
		Integer id;
		String[] stringArray;
		Boolean[] booleanArray;
		boolean[] primitiveBooleanArray;
		Integer[] integerArray;
		int[] primitiveIntegerArray;
		Long[] longArray;
		long[] primitiveLongArray;
		Float[] floatArray;
		float[] primitiveFloatArray;
		Double[] doubleArray;
		double[] primitiveDoubleArray;
		UUID[] uuidArray;
		AnEnum[] enumArray;
		Short[] shortArray;
		short[] primitiveShortArray;
		Date[] dateArray;
		LocalDate[] localDateArray;
		LocalTime[] localTimeArray;
		LocalDateTime[] localDateTimeArray;

		// We have to specify the length for BigDecimal and BigInteger because
		// the default column type when creating the schema is too small on some databases
		@Column(length = 5000)
		BigInteger[] bigIntegerArray;
		@Column(length = 5000)
		BigDecimal[] bigDecimalArray;
	}

	enum AnEnum {FIRST, SECOND, THIRD, FOURTH}
}
