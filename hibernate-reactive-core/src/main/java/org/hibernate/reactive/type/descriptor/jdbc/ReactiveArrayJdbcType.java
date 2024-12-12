/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.type.descriptor.jdbc;

import java.lang.reflect.Array;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.hibernate.reactive.adaptor.impl.ArrayAdaptor;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.BasicPluralJavaType;
import org.hibernate.type.descriptor.java.ByteArrayJavaType;
import org.hibernate.type.descriptor.java.ByteJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.BasicExtractor;
import org.hibernate.type.descriptor.jdbc.JdbcLiteralFormatter;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.internal.JdbcLiteralFormatterArray;
import org.hibernate.type.spi.TypeConfiguration;



/**
 * {@link java.sql.Connection} has a method {@link java.sql.Connection#createArrayOf(String, Object[])}, but we don't have
 * it in Vert.x SQL Client.
 * <p>
 *     Plus, the Vert.x SQL client accept arrays as parameters.
 * </p>
 *
 * @see org.hibernate.type.descriptor.jdbc.ArrayJdbcType
 */
public class ReactiveArrayJdbcType implements JdbcType {

	private final JdbcType elementJdbcType;

	public ReactiveArrayJdbcType(JdbcType elementJdbcType) {
		this.elementJdbcType = elementJdbcType;
	}

	@Override
	public int getJdbcTypeCode() {
		return Types.ARRAY;
	}

	@Override
	public <T> JavaType<T> getJdbcRecommendedJavaTypeMapping(
			Integer precision,
			Integer scale,
			TypeConfiguration typeConfiguration) {
		final JavaType<Object> elementJavaType = elementJdbcType
				.getJdbcRecommendedJavaTypeMapping( precision, scale, typeConfiguration );
		return typeConfiguration.getJavaTypeRegistry()
				.resolveDescriptor( Array.newInstance( elementJavaType.getJavaTypeClass(), 0 ).getClass() );
	}

	@Override
	public <T> JdbcLiteralFormatter<T> getJdbcLiteralFormatter(JavaType<T> javaTypeDescriptor) {
		final JavaType<T> elementJavaType;
		if ( javaTypeDescriptor instanceof ByteArrayJavaType ) {
			// Special handling needed for Byte[], because that would conflict with the VARBINARY mapping
			//noinspection unchecked
			elementJavaType = (JavaType<T>) ByteJavaType.INSTANCE;
		}
		else {
			//noinspection unchecked
			elementJavaType = ( (BasicPluralJavaType<T>) javaTypeDescriptor ).getElementJavaType();
		}
		final JdbcLiteralFormatter<T> elementFormatter = elementJdbcType.getJdbcLiteralFormatter( elementJavaType );
		return new JdbcLiteralFormatterArray<>( javaTypeDescriptor, elementFormatter );
	}

	@Override
	public Class<?> getPreferredJavaTypeClass(WrapperOptions options) {
		return java.sql.Array.class;
	}

	@Override
	public <X> ValueBinder<X> getBinder(final JavaType<X> javaTypeDescriptor) {
		return new BasicBinder<>( javaTypeDescriptor, this ) {

			@Override
			protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options)
					throws SQLException {

				ArrayAdaptor arrayObject = getArrayObject( value, options, getJdbcType(), getJavaType() );
				st.setArray( index, arrayObject );
			}

			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options) {
				throw new UnsupportedOperationException();
			}

			private static <X> ArrayAdaptor getArrayObject(X value, WrapperOptions options, JdbcType jdbcType, JavaType<X> javaType) {
				final TypeConfiguration typeConfiguration = options.getSessionFactory().getTypeConfiguration();
				ReactiveArrayJdbcType arrayJdbcType = (ReactiveArrayJdbcType) jdbcType;
				final JdbcType elementJdbcType = arrayJdbcType.getElementJdbcType();
				final JdbcType underlyingJdbcType = typeConfiguration.getJdbcTypeRegistry()
						.getDescriptor( elementJdbcType.getDefaultSqlTypeCode() );
				final Class<?> elementJdbcJavaTypeClass = elementJdbcJavaTypeClass(
						options,
						elementJdbcType,
						underlyingJdbcType,
						typeConfiguration
				);
				//noinspection unchecked
				final Class<Object[]> arrayClass = (Class<Object[]>) Array.newInstance( elementJdbcJavaTypeClass, 0 )
						.getClass();
				final Object[] objects = javaType.unwrap( value, arrayClass, options );
				return new ArrayAdaptor( elementJdbcType, objects );
			}

			private static Class<?> elementJdbcJavaTypeClass(
					WrapperOptions options,
					JdbcType elementJdbcType,
					JdbcType underlyingJdbcType,
					TypeConfiguration typeConfiguration) {
				final Class<?> preferredJavaTypeClass = elementJdbcType.getPreferredJavaTypeClass( options );
				final Class<?> elementJdbcJavaTypeClass;
				if ( preferredJavaTypeClass == null ) {
					elementJdbcJavaTypeClass = underlyingJdbcType
							.getJdbcRecommendedJavaTypeMapping( null, null, typeConfiguration )
							.getJavaTypeClass();
				}
				else {
					elementJdbcJavaTypeClass = preferredJavaTypeClass;
				}
				return convertTypes( elementJdbcJavaTypeClass );
			}

			private static Class<?> convertTypes(Class<?> elementJdbcJavaTypeClass) {
				if ( Timestamp.class.equals( elementJdbcJavaTypeClass ) ) {
					return LocalDateTime.class;
				}
				if ( Date.class.equals( elementJdbcJavaTypeClass ) ) {
					return LocalDate.class;
				}
				if ( Time.class.equals( elementJdbcJavaTypeClass ) ) {
					return LocalTime.class;
				}
				return elementJdbcJavaTypeClass;
			}
		};
	}

	@Override
	public <X> ValueExtractor<X> getExtractor(final JavaType<X> javaTypeDescriptor) {
		return new BasicExtractor<>( javaTypeDescriptor, this ) {
			@Override
			protected X doExtract(ResultSet rs, int paramIndex, WrapperOptions options) {
				return javaTypeDescriptor.wrap(
						( (ResultSetAdaptor) rs ).getArray( paramIndex, elementJdbcType ),
						options
				);
			}

			@Override
			protected X doExtract(CallableStatement statement, int index, WrapperOptions options) {
				return javaTypeDescriptor.wrap(
						( (ResultSetAdaptor) statement ).getArray( index, elementJdbcType ),
						options
				);
			}

			@Override
			protected X doExtract(CallableStatement statement, String name, WrapperOptions options)
					throws SQLException {
				return javaTypeDescriptor.wrap( statement.getArray( name ), options );
			}
		};
	}

	@Override
	public String getFriendlyName() {
		return "ARRAY";
	}

	public JdbcType getElementJdbcType() {
		return elementJdbcType;
	}

	@Override
	public String toString() {
		return ReactiveArrayJdbcType.class.getSimpleName() + "(" + getFriendlyName() + ")";
	}

	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( o == null || getClass() != o.getClass() ) {
			return false;
		}

		ReactiveArrayJdbcType that = (ReactiveArrayJdbcType) o;

		return elementJdbcType.equals( that.elementJdbcType );
	}

	@Override
	public int hashCode() {
		return elementJdbcType.hashCode();
	}
}
