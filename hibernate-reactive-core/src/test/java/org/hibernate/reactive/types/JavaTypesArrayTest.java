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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.annotations.Array;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.testing.SqlStatementTracker;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test that we handle arrays as basic types and the @{@link Array} annotation in combination with @{@link Column}.
 * <p>
 * Specifying the length doesn't seem to have any effect at the moment.
 * We use it when creating the table with Postgres, but Postgres ignore it anyway.
 * All the other dbs will save the array as a `varbinary` column and length is set using @{@link Column}
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = ORACLE, reason = "Vert.x does not support arrays for Oracle")
public class JavaTypesArrayTest extends BaseReactiveTest {

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker( JavaTypesArrayTest::filterCreateTable, configuration.getProperties() );
		return configuration;
	}

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		// Deleting the entities after each test is not really necessary, and sometimes it causes errors in the log
		// that make it harder to figure out what's going on
		return voidFuture();
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean filterCreateTable(String s) {
		return s.toLowerCase().startsWith( "create table basic " );
	}

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( Basic.class );
	}

	private void testField(VertxTestContext context, Basic original, Consumer<Basic> consumer) {
		if ( List.of( DB2, SQLSERVER ).contains( dbType() ) ) {
			test( context, assertThrown( HibernateException.class, getSessionFactory()
					.withTransaction( s -> s.persist( original ) ) )
					.thenAccept( e -> assertThat( e.getMessage() ).startsWith( "HR000081: " ) )
			);
		}
		else {
			test(
					context, getSessionFactory()
							.withTransaction( s -> s.persist( original ) )
							.thenCompose( v -> getSessionFactory().withSession( s -> s
									.find( Basic.class, original.id )
									.thenAccept( found -> {
										assertNotNull( found );
										consumer.accept( found );
									} ) ) )
			);
		}
	}

	@Test
	public void testStringArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		String[] dataArray = {"Hello world!", "Hello earth"};
		basic.stringArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "stringArray", null, 255 );
			assertArrayEquals( dataArray, found.stringArray );
		} );
	}

	@Test
	public void testStringArrayTypeWithArrayAnnotation(VertxTestContext context) {
		Basic basic = new Basic();
		String[] dataArray = {"Hello world!", "Hello earth"};
		basic.stringArrayWithArrayAnnotation = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "stringArrayWithArrayAnnotation", 5, null );
			assertArrayEquals( dataArray, found.stringArrayWithArrayAnnotation );
		} );
	}

	@Test
	public void testStringArrayTypeWithColumnAnnotation(VertxTestContext context) {
		Basic basic = new Basic();
		String[] dataArray = {"Hello world!", "Hello earth"};
		basic.stringArrayWithColumnAnnotation = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "stringArrayWithColumnAnnotation", null, 200 );
			assertArrayEquals( dataArray, found.stringArrayWithColumnAnnotation );
		} );
	}

	@Test
	public void testStringArrayTypeWithBothAnnotations(VertxTestContext context) {
		Basic basic = new Basic();
		String[] dataArray = {"Hello world!", "Hello earth"};
		basic.stringArrayWithBothAnnotations = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "stringArrayWithBothAnnotations", 5, 200 );
			assertArrayEquals( dataArray, found.stringArrayWithBothAnnotations );
		} );
	}

	@Test
	public void testBooleanArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Boolean[] dataArray = {TRUE, FALSE, null, TRUE};
		basic.booleanArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "booleanArray", null, null );
			assertArrayEquals( dataArray, found.booleanArray );
		} );
	}

	@Test
	public void testPrimitiveBooleanArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		boolean[] dataArray = {true, false, true};
		basic.primitiveBooleanArray = dataArray;

		testField( context, basic, found -> {
					validateArrayColumn( "primitiveBooleanArray", null, null );
					assertArrayEquals( dataArray, found.primitiveBooleanArray );
		} );
	}

	@Test
	public void testIntegerArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Integer[] dataArray = {null, Integer.MIN_VALUE, 2, Integer.MAX_VALUE};
		basic.integerArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "integerArray", null, null );
			assertArrayEquals( dataArray, found.integerArray );
		} );
	}

	@Test
	public void testPrimitiveIntegerArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		int[] dataArray = {1, 2, 3};
		basic.primitiveIntegerArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "primitiveIntegerArray", null, null );
			assertArrayEquals( dataArray, found.primitiveIntegerArray );
		} );
	}

	@Test
	public void testLongArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Long[] dataArray = {Long.MIN_VALUE, Long.MAX_VALUE, 3L, null};
		basic.longArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "longArray", null, null );
			assertArrayEquals( dataArray, found.longArray );
		} );
	}

	@Test
	public void testPrimitiveLongArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		long[] dataArray = {Long.MIN_VALUE, Long.MAX_VALUE, 3L};
		basic.primitiveLongArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "primitiveLongArray", null, null );
			assertArrayEquals( dataArray, found.primitiveLongArray );
		} );
	}

	@Test
	public void testFloatArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Float[] dataArray = {12.562f, null, 13.562f};
		basic.floatArray = dataArray;

		testField( context, basic, found -> {
			assertArrayEquals( dataArray, found.floatArray );
			validateArrayColumn( "floatArray", null, null );
		} );
	}

	@Test
	public void testPrimitiveFloatArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		float[] dataArray = {12.562f, 13.562f};
		basic.primitiveFloatArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "primitiveFloatArray", null, null );
			assertArrayEquals( dataArray, found.primitiveFloatArray );
		} );
	}

	@Test
	public void testDoubleArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Double[] dataArray = {12.562d, null, 13.562d};
		basic.doubleArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "doubleArray", null, null );
			assertArrayEquals( dataArray, found.doubleArray );
		} );
	}

	@Test
	public void testPrimitiveDoubleArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		double[] dataArray = {12.562d, 13.562d};
		basic.primitiveDoubleArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "primitiveDoubleArray", null, null );
			assertArrayEquals( dataArray, found.primitiveDoubleArray );
		} );
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

		testField( context, basic, found -> {
			validateArrayColumn( "uuidArray", null, null );
			assertArrayEquals( dataArray, found.uuidArray );
		} );
	}

	@Test
	public void testEnumArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		AnEnum[] dataArray = {AnEnum.FOURTH, AnEnum.FIRST, AnEnum.THIRD};
		basic.enumArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "enumArray", null, null );
			assertArrayEquals( dataArray, found.enumArray );
		} );
	}

	@Test
	public void testShortArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Short[] dataArray = {512, 112, null, 0};
		basic.shortArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "shortArray", null, null );
			assertArrayEquals( dataArray, found.shortArray );
		} );
	}

	@Test
	public void testPrimitiveShortArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		short[] dataArray = {500, 32, -1};
		basic.primitiveShortArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "primitiveShortArray", null, null );
			assertArrayEquals( dataArray, found.primitiveShortArray );
		} );
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

		testField( context, basic, found -> {
			validateArrayColumn( "localDateArray", null, null );
			assertArrayEquals( dataArray, found.localDateArray );
		} );
	}

	@Test
	@DisabledFor(value = {MYSQL, MARIA}, reason = "HHH-18881: Problem with the conversion of dates")
	public void testDateArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		Date[] dataArray = {Calendar.getInstance().getTime(), Calendar.getInstance().getTime()};
		basic.dateArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "dateArray", null, null );
			assertArrayEquals( dataArray, found.dateArray );
		} );
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

		testField( context, basic, found -> {
			validateArrayColumn( "localTimeArray", null, null );
			assertArrayEquals( dataArray, found.localTimeArray );
		} );
	}

	@Test
	@DisabledFor(value = {MYSQL, MARIA}, reason = "HHH-18881: Problem with the conversion of dates")
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

		testField( context, basic, found -> {
			validateArrayColumn( "localDateTimeArray", null, null );
			assertArrayEquals( dataArray, found.localDateTimeArray );
		} );
	}

	@Test
	public void testBigIntegerArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		BigInteger[] dataArray = {BigInteger.TEN, BigInteger.ZERO};
		basic.bigIntegerArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "bigIntegerArray", null, 5000 );
			assertArrayEquals( dataArray, found.bigIntegerArray );
		} );
	}

	@Test
	public void testBigDecimalArrayType(VertxTestContext context) {
		Basic basic = new Basic();
		BigDecimal[] dataArray = {BigDecimal.valueOf( 123384967L ), BigDecimal.ZERO};
		basic.bigDecimalArray = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "bigDecimalArray", null, 5000 );
			assertEquals( dataArray.length, found.bigDecimalArray.length );
			assertEquals( 0, dataArray[0].compareTo( found.bigDecimalArray[0] ) );
			assertEquals( 0, dataArray[1].compareTo( found.bigDecimalArray[1] ) );
		} );
	}

	@Test
	public void testBigDecimalArrayTypeWithArrayAnnotation(VertxTestContext context) {
		Basic basic = new Basic();
		BigDecimal[] dataArray = {BigDecimal.valueOf( 123384967L ), BigDecimal.ZERO};
		basic.bigDecimalArrayWithArrayAnnotation = dataArray;

		testField( context, basic, found -> {
			validateArrayColumn( "bigDecimalArrayWithArrayAnnotation", 5, 5000 );
			assertEquals( dataArray.length, found.bigDecimalArrayWithArrayAnnotation.length );
			assertEquals( 0, dataArray[0].compareTo( found.bigDecimalArrayWithArrayAnnotation[0] ) );
			assertEquals( 0, dataArray[1].compareTo( found.bigDecimalArrayWithArrayAnnotation[1] ) );
		} );
	}


	private void validateArrayColumn(String columnName, Integer arrayLength, Integer columnLength) {
		assertThat( sqlTracker.getLoggedQueries() )
				.allMatch( arrayColumnPredicate( columnName, arrayLength, columnLength ) );
	}

	// A predicate that checks we apply the right size to the array when required
	private static Predicate<String> arrayColumnPredicate(
			String columnName,
			Integer arrayLength,
			Integer columnLength) {
		switch ( dbType() ) {
			case POSTGRESQL:
			case COCKROACHDB:
				return postgresPredicate( columnName, arrayLength, columnLength );
			case MYSQL:
			case MARIA:
				return arrayAsJsonPredicate( columnName );
			case SQLSERVER:
			case DB2:
				return arrayAsXmlPredicate( columnName );
			default:
				throw new AssertionFailure( "Unexpected database: " + dbType() );
		}
	}

	/**
	 * For Postgres, we expect arrays to be defined as {@code array}.
	 * <p>
	 * For example: {@code varchar(255) array[2]}
	 * </p>
	 */
	private static Predicate<String> postgresPredicate(String columnName, Integer arrayLength, Integer columnLength) {
		StringBuilder regexBuilder = new StringBuilder();
		regexBuilder.append( ".*" );

		regexBuilder.append( columnName ).append( " \\w+" );
		// Column length only affects arrays of strings
		if ( columnLength != null && columnName.startsWith( "string" ) ) {
			regexBuilder.append( "\\(" ).append( columnLength ).append( "\\)" );
		}
		else {
			// for some types we have a default size. For example: `varchar(255)` or `numeric(38,0)`
			regexBuilder.append( "(\\(\\d+(,\\d+)?\\))?" );
		}
		regexBuilder.append( " array" );
		if ( arrayLength != null ) {
			regexBuilder.append( "\\[" ).append( arrayLength ).append( "\\]" );
		}
		regexBuilder.append( ".*" );
		return s -> s.matches( regexBuilder.toString() );
	}

	private static Predicate<String> arrayAsJsonPredicate(String columnName) {
		return s -> s.contains( columnName + " json" );
	}

	private static Predicate<String> arrayAsXmlPredicate(String columnName) {
		// Example of correct query definition: columnName xml
		return s -> s.contains( columnName + " xml" );
	}

	private static Predicate<String> arrayAsVarbinaryPredicate(String columnName, Integer columnLength) {
		StringBuilder regexBuilder = new StringBuilder();
		// Example of correct query definition: columnName varbinary(255)
		regexBuilder.append( columnName ).append( " varbinary" ).append( "(" );
		if ( columnLength != null ) {
			regexBuilder.append( columnLength );
		}
		else {
			// Default size
			regexBuilder.append( 255 );
		}
		regexBuilder.append( ")" );
		return s -> s.contains( regexBuilder.toString() );
	}

	@Entity(name = "Basic")
	@Table(name = "Basic")
	private static class Basic {
		@Id
		@GeneratedValue
		Integer id;
		String[] stringArray;
		@Array(length = 5)
		String[] stringArrayWithArrayAnnotation;
		@Column(length = 200)
		String[] stringArrayWithColumnAnnotation;
		@Array(length = 5)
		@Column(length = 200)
		String[] stringArrayWithBothAnnotations;
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
		@Array(length = 5)
		@Column(length = 5000)
		BigDecimal[] bigDecimalArrayWithArrayAnnotation;
	}

	enum AnEnum {FIRST, SECOND, THIRD, FOURTH}
}
