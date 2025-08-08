/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.type.descriptor.jdbc;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.BasicPluralJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.spi.UnknownBasicJavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.BasicExtractor;
import org.hibernate.type.descriptor.jdbc.JdbcLiteralFormatter;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.JsonHelper;
import org.hibernate.type.descriptor.jdbc.spi.JsonGeneratingVisitor;
import org.hibernate.type.format.StringJsonDocumentWriter;

import io.vertx.core.json.JsonArray;

/**
 * @see org.hibernate.type.descriptor.jdbc.JsonArrayJdbcType
 */
public class ReactiveJsonArrayJdbcType extends ReactiveArrayJdbcType {

	public ReactiveJsonArrayJdbcType(JdbcType elementJdbcType) {
		super( elementJdbcType );
	}

	@Override
	public int getJdbcTypeCode() {
		return SqlTypes.VARCHAR;
	}

	@Override
	public int getDdlTypeCode() {
		return SqlTypes.JSON;
	}

	@Override
	public int getDefaultSqlTypeCode() {
		return SqlTypes.JSON_ARRAY;
	}

	@Override
	public String toString() {
		return "JsonArrayJdbcType";
	}

	@Override
	public <T> JdbcLiteralFormatter<T> getJdbcLiteralFormatter(JavaType<T> javaType) {
		// No literal support for now
		return null;
	}

	protected <X> X fromString(String string, JavaType<X> javaType, WrapperOptions options) throws SQLException {
		if ( string == null ) {
			return null;
		}
		if ( ((BasicPluralJavaType<?>) javaType).getElementJavaType() instanceof UnknownBasicJavaType<?> ) {
			return options.getJsonFormatMapper().fromString( string, javaType, options );
		}
		else {
			return JsonHelper.arrayFromString( javaType, this.getElementJdbcType(), string, options );
		}
	}

	protected <X> String toString(X value, JavaType<X> javaType, WrapperOptions options) {
		final JavaType<?> elementJavaType = ( (BasicPluralJavaType<?>) javaType ).getElementJavaType();
		if ( elementJavaType instanceof UnknownBasicJavaType<?> ) {
			return options.getJsonFormatMapper().toString( value, javaType, options);
		}
		else {
			final Object[] domainObjects = javaType.unwrap( value, Object[].class, options );
			final StringJsonDocumentWriter writer = new StringJsonDocumentWriter();
			JsonGeneratingVisitor.INSTANCE.visitArray( elementJavaType, getElementJdbcType(), domainObjects, options, writer );
			return writer.getJson();
		}
	}

	@Override
	public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
		return new BasicBinder<>( javaType, this ) {
			@Override
			protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options)
					throws SQLException {
				final String json = ( (ReactiveJsonArrayJdbcType) getJdbcType() ).toString( value, getJavaType(), options );
				st.setString( index, json );
			}

			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options)
					throws SQLException {
				final String json = ( (ReactiveJsonArrayJdbcType) getJdbcType() ).toString( value, getJavaType(), options );
				st.setString( name, json );
			}
		};
	}

	@Override
	public <X> ValueExtractor<X> getExtractor(JavaType<X> javaType) {
		return new BasicExtractor<>( javaType, this ) {
			@Override
			protected X doExtract(ResultSet rs, int paramIndex, WrapperOptions options) throws SQLException {
				return getObject( rs.getObject( paramIndex ), options );
			}

			@Override
			protected X doExtract(CallableStatement statement, int index, WrapperOptions options) throws SQLException {
				return getObject( statement.getObject( index ), options );
			}
			@Override
			protected X doExtract(CallableStatement statement, String name, WrapperOptions options)
					throws SQLException {
				return getObject( statement.getObject( name ), options );
			}

			private X getObject(Object array, WrapperOptions options) throws SQLException {
				final String json = array == null ? null : ( (JsonArray) array ).encode();
				return ( (ReactiveJsonArrayJdbcType) getJdbcType() ).fromString( json, getJavaType(), options );
			}
		};
	}
}
