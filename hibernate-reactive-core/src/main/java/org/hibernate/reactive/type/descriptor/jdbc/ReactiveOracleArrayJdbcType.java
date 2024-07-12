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
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.converter.spi.BasicValueConverter;
import org.hibernate.type.descriptor.converter.spi.JpaAttributeConverter;
import org.hibernate.type.descriptor.java.BasicPluralJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.ArrayJdbcType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.StructJdbcType;

import static java.sql.Types.ARRAY;

/**
 * @see org.hibernate.dialect.OracleArrayJdbcType
 */
public class ReactiveOracleArrayJdbcType extends OracleArrayJdbcType {

	private final String upperTypeName;

	public ReactiveOracleArrayJdbcType(JdbcType elementJdbcType, String typeName) {
		super( elementJdbcType, typeName );
		this.upperTypeName = typeName == null ? null : typeName.toUpperCase( Locale.ROOT );
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
				return ( upperTypeName == null
						? getTypeName( options, (BasicPluralJavaType<?>) getJavaType(), (ArrayJdbcType) getJdbcType() ).toUpperCase( Locale.ROOT )
						: upperTypeName
				);
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

	/*
	 * FIXME: We should change the scope of these methods in ORM: see OracleArrayJdbcType#getTypeName
	 */

	static String getTypeName(WrapperOptions options, BasicPluralJavaType<?> containerJavaType, ArrayJdbcType arrayJdbcType) {
		Dialect dialect = options.getSessionFactory().getJdbcServices().getDialect();
		return getTypeName( containerJavaType.getElementJavaType(), arrayJdbcType.getElementJdbcType(), dialect );
	}

	static String getTypeName(BasicType<?> elementType, Dialect dialect) {
		final BasicValueConverter<?, ?> converter = elementType.getValueConverter();
		if ( converter != null ) {
			final String simpleName;
			if ( converter instanceof JpaAttributeConverter<?, ?> ) {
				simpleName = ( (JpaAttributeConverter<?, ?>) converter ).getConverterJavaType()
						.getJavaTypeClass()
						.getSimpleName();
			}
			else {
				simpleName = converter.getClass().getSimpleName();
			}
			return dialect.getArrayTypeName(
					simpleName,
					null, // not needed by OracleDialect.getArrayTypeName()
					null // not needed by OracleDialect.getArrayTypeName()
			);
		}
		return getTypeName( elementType.getJavaTypeDescriptor(), elementType.getJdbcType(), dialect );
	}

	static String getTypeName(JavaType<?> elementJavaType, JdbcType elementJdbcType, Dialect dialect) {
		final String simpleName;
		if ( elementJavaType.getJavaTypeClass().isArray() ) {
			simpleName = dialect.getArrayTypeName(
					elementJavaType.getJavaTypeClass().getComponentType().getSimpleName(),
					null, // not needed by OracleDialect.getArrayTypeName()
					null // not needed by OracleDialect.getArrayTypeName()
			);
		}
		else if ( elementJdbcType instanceof StructJdbcType ) {
			simpleName = ( (StructJdbcType) elementJdbcType ).getStructTypeName();
		}
		else {
			final Class<?> preferredJavaTypeClass = elementJdbcType.getPreferredJavaTypeClass( null );
			if ( preferredJavaTypeClass == elementJavaType.getJavaTypeClass() ) {
				simpleName = elementJavaType.getJavaTypeClass().getSimpleName();
			}
			else {
				if ( preferredJavaTypeClass.isArray() ) {
					simpleName = elementJavaType.getJavaTypeClass().getSimpleName() + dialect.getArrayTypeName(
							preferredJavaTypeClass.getComponentType().getSimpleName(),
							null,
							null
					);
				}
				else {
					simpleName = elementJavaType.getJavaTypeClass().getSimpleName() + preferredJavaTypeClass.getSimpleName();
				}
			}
		}
		return dialect.getArrayTypeName(
				simpleName,
				null, // not needed by OracleDialect.getArrayTypeName()
				null // not needed by OracleDialect.getArrayTypeName()
		);
	}
}
