/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.type.NumericBooleanConverter;
import org.hibernate.type.TrueFalseConverter;
import org.hibernate.type.YesNoConverter;
import org.hibernate.type.descriptor.java.DataHelper;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.expectedDatatype;
import static org.hibernate.reactive.containers.DatabaseConfiguration.getDatatypeQuery;

/**
 * Check that each property is mapped as the expected type in the database.
 */
public class ColumnTypesMappingTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( BasicTypesTestEntity.class );
	}

	private void testDatatype(VertxTestContext context, String columnName, Class<?> type) {
		test( context, openSession()
				.thenCompose( s -> s
						.createNativeQuery( getDatatypeQuery( BasicTypesTestEntity.TABLE_NAME, columnName ), String.class )
						.getSingleResult()
						.thenAccept( typeOnTheDb -> assertThat( toString( typeOnTheDb ) ).isEqualTo( expectedDatatype( type ) ) ) )
		);
	}

	private String toString(Object result) {
		try {
			// MySQL returns a Blob
			return result instanceof Blob
					? new String( DataHelper.extractBytes( ( (Blob) result ).getBinaryStream() ) )
					: (String) result;
		}
		catch (SQLException sqlException) {
			throw new RuntimeException( sqlException );
		}
	}

	@Test
	public void testBigDecimal(VertxTestContext context) {
		testDatatype( context, "bigDecimal", BigDecimal.class );
	}

	@Test
	public void testStringType(VertxTestContext context) {
		testDatatype( context, "aString", String.class );
	}

	@Test
	public void testIntegerFieldType(VertxTestContext context) {
		testDatatype( context, "fieldInteger", Integer.class );
	}

	@Test
	public void testIntegerPrimitiveType(VertxTestContext context) {
		testDatatype( context, "primitiveInt", int.class );
	}

	@Test
	public void testBigIntegerType(VertxTestContext context) {
		testDatatype( context, "bigInteger", BigInteger.class );
	}

	@Test
	public void testLongFieldType(VertxTestContext context) {
		testDatatype( context, "fieldLong", Long.class );
	}

	@Test
	public void testLongPrimitiveType(VertxTestContext context) {
		testDatatype( context, "primitiveLong", long.class );
	}

	@Test
	public void testFloatFieldType(VertxTestContext context) {
		testDatatype( context, "fieldFloat", Float.class );
	}

	@Test
	public void testFloatPrimitiveType(VertxTestContext context) {
		testDatatype( context, "primitiveFloat", float.class );
	}


	@Test
	public void testDoubleFieldType(VertxTestContext context) {
		testDatatype( context, "fieldDouble", Double.class );
	}

	@Test
	public void testDoublePrimitiveType(VertxTestContext context) {
		testDatatype( context, "primitiveDouble", double.class );
	}

	@Test
	public void testBooleanPrimitiveType(VertxTestContext context) {
		testDatatype( context, "primitiveBoolean", boolean.class );
	}

	@Test
	public void testBooleanFieldType(VertxTestContext context) {
		testDatatype( context, "fieldBoolean", Boolean.class );
	}

	@Test
	public void testBooleanTrueFalseType(VertxTestContext context) {
		testDatatype( context, "booleanTrueFalse", TrueFalseConverter.class );
	}

	@Test
	public void testBooleanYesNoType(VertxTestContext context) {
		testDatatype( context, "booleanYesNo", YesNoConverter.class );
	}

	@Test
	public void testBooleanNumericType(VertxTestContext context) {
		testDatatype( context, "booleanNumeric", NumericBooleanConverter.class );
	}

	@Test
	public void testBytePrimitiveType(VertxTestContext context) {
		testDatatype( context, "primitiveByte", byte.class );
	}

	@Test
	public void testBytesPrimitiveType(VertxTestContext context) {
		testDatatype( context, "primitiveBytes", byte[].class );
	}

	@Test
	public void testByteFieldType(VertxTestContext context) {
		testDatatype( context, "fieldByte", Byte.class );
	}

	@Test
	public void testUrlType(VertxTestContext context) {
		testDatatype( context, "url", URL.class );
	}

	@Test
	public void testDateType(VertxTestContext context) {
		testDatatype( context, "someDate", Date.class );
	}

	@Test
	public void testDateAsTimeType(VertxTestContext context) {
		testDatatype( context, "dateAsTime", Time.class );
	}

	@Test
	public void testDateAsTimestampType(VertxTestContext context) {
		testDatatype( context, "dateAsTimestamp", Timestamp.class );
	}

	@Test
	public void testTimeZoneType(VertxTestContext context) {
		testDatatype( context, "timeZone", TimeZone.class );
	}

	@Test
	public void testCalendarAsDateType(VertxTestContext context) {
		testDatatype( context, "calendarAsDate", Date.class );
	}

	@Test
	public void testCalendarAsTimestampType(VertxTestContext context) {
		testDatatype( context, "calendarAsTimestamp", Timestamp.class );
	}

	@Test
	public void testLocalDateType(VertxTestContext context) {
		testDatatype( context, "localdayte", LocalDate.class );
	}

	@Test
	public void testLocalDateTimeType(VertxTestContext context) {
		testDatatype( context, "alocalDT", LocalDateTime.class );
	}

	@Test
	public void testSerializableType(VertxTestContext context) {
		testDatatype( context, "serializable", Serializable.class );
	}
}
