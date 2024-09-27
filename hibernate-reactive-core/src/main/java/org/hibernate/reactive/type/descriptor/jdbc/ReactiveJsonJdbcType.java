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

import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.AggregateJdbcType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.BasicExtractor;
import org.hibernate.type.descriptor.jdbc.JsonJdbcType;

import io.vertx.core.json.JsonObject;

/**
 * Map a JSON column as {@link JsonObject}
 */
public class ReactiveJsonJdbcType extends JsonJdbcType {

	public static final ReactiveJsonJdbcType INSTANCE = new ReactiveJsonJdbcType( null );

	protected ReactiveJsonJdbcType(EmbeddableMappingType embeddableMappingType) {
		super( embeddableMappingType );
	}

	@Override
	public AggregateJdbcType resolveAggregateJdbcType(
			EmbeddableMappingType mappingType, String sqlType, RuntimeModelCreationContext creationContext) {
		return new ReactiveJsonJdbcType( mappingType );
	}

	@Override
	public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
		return new BasicBinder<>( javaType, this ) {
			@Override
			protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options)
					throws SQLException {
				st.setObject( index, toJsonObject( value, javaType, options ) );
			}

			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options)
					throws SQLException {
				st.setObject( name, toJsonObject( value, javaType, options ) );
			}
		};
	}

	protected <X> JsonObject toJsonObject(X value, JavaType<X> javaType, WrapperOptions options) {
		return new JsonObject( this.toString( value, javaType, options ) );
	}

	@Override
	public <X> ValueExtractor<X> getExtractor(JavaType<X> javaType) {
		return new BasicExtractor<>( javaType, this ) {
			@Override
			protected X doExtract(ResultSet rs, int paramIndex, WrapperOptions options) throws SQLException {
				return fromString( toJsonString( rs.getObject( paramIndex ) ), getJavaType(), options );
			}

			@Override
			protected X doExtract(CallableStatement statement, int index, WrapperOptions options) throws SQLException {
				return fromString( toJsonString( statement.getObject( index ) ), getJavaType(), options );
			}

			@Override
			protected X doExtract(CallableStatement statement, String name, WrapperOptions options)
					throws SQLException {
				return fromString( toJsonString( statement.getObject( name ) ), getJavaType(), options );
			}
		};
	}

	private static String toJsonString(Object value) {
		if ( value == null ) {
			return null;
		}
		return ( (JsonObject) value ).encode();
	}
}
