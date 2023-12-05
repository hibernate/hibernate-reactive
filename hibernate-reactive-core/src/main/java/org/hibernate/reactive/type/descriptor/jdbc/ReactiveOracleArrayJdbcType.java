/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.type.descriptor.jdbc;

import java.lang.reflect.Array;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleArrayJdbcType;
import org.hibernate.reactive.adaptor.impl.ArrayAdaptor;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.BasicPluralJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.JdbcType;

import static java.sql.Types.ARRAY;

/**
 * @see org.hibernate.dialect.OracleArrayJdbcType
 */
public class ReactiveOracleArrayJdbcType extends OracleArrayJdbcType {

	public ReactiveOracleArrayJdbcType(JdbcType elementJdbcType, String typeName) {
		super( elementJdbcType, typeName );
	}

	public static String getTypeName(WrapperOptions options, BasicPluralJavaType<?> containerJavaType) {
		Dialect dialect = options.getSessionFactory().getJdbcServices().getDialect();
		return getTypeName( containerJavaType.getElementJavaType(), dialect );
	}

	public static String getTypeName(JavaType<?> elementJavaType, Dialect dialect) {
		return dialect.getArrayTypeName(
				elementJavaType.getJavaTypeClass().getSimpleName(),
				null, // not needed by OracleDialect.getArrayTypeName()
				null // not needed by OracleDialect.getArrayTypeName()
		);
	}

	@Override
	public <X> ValueBinder<X> getBinder(final JavaType<X> javaTypeDescriptor) {
		//noinspection unchecked
		final BasicPluralJavaType<X> containerJavaType = (BasicPluralJavaType<X>) javaTypeDescriptor;
		return new BasicBinder<>( javaTypeDescriptor, this ) {
			private String typeName(WrapperOptions options) {
				String typeName = getTypeName() == null ? getTypeName( options, containerJavaType ) : getTypeName();
				return typeName.toUpperCase( Locale.ROOT );
			}

			@Override
			protected void doBindNull(PreparedStatement st, int index, WrapperOptions options) throws SQLException {
				st.setNull( index, ARRAY, typeName( options ) );
			}

			@Override
			protected void doBindNull(CallableStatement st, String name, WrapperOptions options) throws SQLException {
				st.setNull( name, ARRAY, typeName( options ) );
			}

			@Override
			protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options)
					throws SQLException {
				st.setArray( index, getArray( value, containerJavaType, options ) );
			}

			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options) {
				final java.sql.Array arr = getArray( value, containerJavaType, options );
				try {
					st.setObject( name, arr, ARRAY );
				}
				catch (SQLException ex) {
					throw new HibernateException( ex );
				}
			}

			private ArrayAdaptor getArray(X value, BasicPluralJavaType<X> containerJavaType, WrapperOptions options) {
				//noinspection unchecked
				final Class<Object[]> arrayClass = (Class<Object[]>) Array
						.newInstance( getElementJdbcType().getPreferredJavaTypeClass( options ), 0 ).getClass();
				final Object[] objects = javaTypeDescriptor.unwrap( value, arrayClass, options );
				final String arrayTypeName = typeName( options ).toUpperCase( Locale.ROOT );
				return new ArrayAdaptor( arrayTypeName, objects );
			}
		};
	}
}
