/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.type.descriptor.jdbc;

import java.lang.reflect.Array;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.HibernateException;
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
import org.hibernate.type.descriptor.jdbc.ObjectJdbcType;
import org.hibernate.type.descriptor.jdbc.internal.JdbcLiteralFormatterArray;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * {@link java.sql.Connection} has a method {@link java.sql.Connection#createArrayOf(String, Object[])}, but we don't have
 * it in Vert.x SQL Client.
 *
 * @see org.hibernate.type.descriptor.jdbc.ArrayJdbcType
 */
public class ReactiveArrayJdbcType implements JdbcType {

	public static final ReactiveArrayJdbcType INSTANCE = new ReactiveArrayJdbcType( ObjectJdbcType.INSTANCE );

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
				st.setObject( index, value );
			}

			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options){
				try {
					st.setObject( name, value );
				}
				catch (SQLException ex) {
					throw new HibernateException(
							"JDBC driver does not support named parameters for setArray. Use positional.",
							ex
					);
				}
			}
		};
	}

	@Override
	public <X> ValueExtractor<X> getExtractor(final JavaType<X> javaTypeDescriptor) {
		return new BasicExtractor<>( javaTypeDescriptor, this ) {
			@Override
			protected X doExtract(ResultSet rs, int paramIndex, WrapperOptions options) throws SQLException {
				return javaTypeDescriptor.wrap( rs.getArray( paramIndex ), options );
			}

			@Override
			protected X doExtract(CallableStatement statement, int index, WrapperOptions options) throws SQLException {
				return javaTypeDescriptor.wrap( statement.getArray( index ), options );
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
