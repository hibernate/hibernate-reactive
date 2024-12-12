/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.type.descriptor.jdbc;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLXML;

import org.hibernate.dialect.XmlHelper;
import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.XmlJdbcType;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * @see XmlJdbcType
 */
public class ReactiveXmlJdbcType extends XmlJdbcType {

	private static final Log LOG = make( Log.class, lookup() );

	public static final ReactiveXmlJdbcType INSTANCE = new ReactiveXmlJdbcType( null );

	protected ReactiveXmlJdbcType(EmbeddableMappingType embeddableMappingType) {
		super( embeddableMappingType );
	}

	@Override
	protected <X> X fromString(String string, JavaType<X> javaType, WrapperOptions options) throws SQLException {
		if ( getEmbeddableMappingType() != null ) {
			return XmlHelper.fromString(
					getEmbeddableMappingType(),
					string,
					javaType.getJavaTypeClass() != Object[].class,
					options
			);
		}
		if ( javaType.getJavaType() == SQLXML.class ) {
			throw LOG.unsupportedXmlType();
		}
		return options.getSessionFactory().getFastSessionServices().getXmlFormatMapper()
				.fromString( string, javaType, options );
	}

	@Override
	public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
		return new BasicBinder<>( javaType, this ) {
			@Override
			protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options) {
				throw LOG.unsupportedXmlType();
			}

			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options) {
				throw LOG.unsupportedXmlType();
			}
		};
	}
}
