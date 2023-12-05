/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.reactive.BaseReactiveTest;

import org.hibernate.reactive.testing.DBSelectionExtension;
import org.hibernate.type.SqlTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, timeUnit = MINUTES)

public class JavaTypesArrayTest extends BaseReactiveTest {

	@RegisterExtension
	public final DBSelectionExtension skip = DBSelectionExtension.skipTestsFor( ORACLE );

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( Basic.class );
	}

	private void testField( VertxTestContext context, Basic
			original, Consumer<Basic> consumer) {
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
	public void testStringArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		String[] dataArray = new String[] { "Hello world!", "Hello earth" };
		basic.stringArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.stringArray ) );
	}

	@Test
	public void testBooleanArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Boolean[] dataArray = new Boolean[] { Boolean.TRUE, Boolean.FALSE, Boolean.TRUE };
		basic.booleanArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.booleanArray ) );
	}

	@Test
	public void testIntegerArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Integer[] dataArray = new Integer[] { 1, 2, 3 };
		basic.integerArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.integerArray ) );
	}

	@Test
	public void testLongArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Long[] dataArray = new Long[] { Long.MIN_VALUE, Long.MAX_VALUE, 3L };
		basic.longArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.longArray ) );
	}

	@Test
	public void testFloatArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Float[] dataArray = new Float[] { 12.562f, 13.562f };
		basic.floatArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.floatArray ) );
	}

	@Test
	public void testDoubleArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Double[] dataArray = new Double[] { 12.562d, 13.562d };
		basic.doubleArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.doubleArray ) );
	}

	@Test
	public void testUUIDArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		UUID[] dataArray = new UUID[] {
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
		AnEnum[] dataArray = { AnEnum.FOURTH, AnEnum.FIRST, AnEnum.THIRD };
		basic.enumArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.enumArray ) );
	}

	@Test
	public void testPrimitiveIntArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		int[] dataArray = new int[] { 1, 2, 3 };
		basic.primitiveIntArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveIntArray ) );
	}

	@Test
	public void testPrimitiveLongArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		long[] dataArray = new long[] { 9L, 11L, 22L };
		basic.primitiveLongArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveLongArray ) );
	}

	@Test
	public void testPrimitiveBooleanArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		boolean[] dataArray = new boolean[] { true, false };
		basic.primitiveBooleanArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveBooleanArray ) );
	}

	@Test
	public void testPrimitiveFloatArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		float[] dataArray = new float[] { 10.02f, 10.03f };
		basic.primitiveFloatArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveFloatArray ) );
	}

	@Test
	public void testPrimitiveDoubleArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		double[] dataArray = new double[] { 1.01, 1.02 };
		basic.primitiveDoubleArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveDoubleArray ) );
	}

	@Test
	public void testPrimitiveShortArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		short[] dataArray = new short[] { 512, 112, 0 };
		basic.primitiveShortArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.primitiveShortArray ) );
	}

	@Test
	public void testShortArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Short[] dataArray = new Short[] { 512, 112, null, 0 };
		basic.shortArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.shortArray ) );
	}

	@Test
	public void testLocalDateArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		LocalDate date1 = LocalDate.now();
		LocalDate[] dataArray = new LocalDate[] {
				date1.plusDays( 5 ),
				date1.plusMonths( 4 ),
				date1.plusYears( 3 )
		};
		basic.localDateArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.localDateArray ) );
	}

	@Test
	public void testLocalTimeArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		LocalTime[] dataArray = new LocalTime[] {
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
		LocalDateTime[] dataArray = new LocalDateTime[] {
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
	public void testSerializableArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Serializable[] dataArray = new Serializable[] { "one", 102 };
		basic.serializableArray = dataArray;

		testField( context, basic, found -> assertArrayEquals( dataArray, found.serializableArray ) );
	}

	@Entity(name = "Basic")
	@Table(name = "Basic")
	private static class Basic {
		@Id
		@GeneratedValue
		Integer id;

		String[] stringArray;
		Boolean[] booleanArray;
		Integer[] integerArray;
		Long[] longArray;
		Float[] floatArray;
		Double[] doubleArray;
		UUID[] uuidArray;
		AnEnum[] enumArray;

		@JdbcTypeCode(SqlTypes.VARBINARY)
		int[] primitiveIntArray;

		@JdbcTypeCode(SqlTypes.VARBINARY)
		long[] primitiveLongArray;

		@JdbcTypeCode(SqlTypes.VARBINARY)
		boolean[] primitiveBooleanArray;

		@JdbcTypeCode(SqlTypes.VARBINARY)
		float[] primitiveFloatArray;

		@JdbcTypeCode(SqlTypes.VARBINARY)
		double[] primitiveDoubleArray;

		@JdbcTypeCode(SqlTypes.VARBINARY)
		short[] primitiveShortArray;

		Short[] shortArray;

		LocalDate[] localDateArray;

		LocalTime[] localTimeArray;

		LocalDateTime[] localDateTimeArray;

		@JdbcTypeCode(SqlTypes.VARBINARY)
		private Serializable[] serializableArray;
	}

	enum AnEnum {FIRST, SECOND, THIRD, FOURTH}
}
