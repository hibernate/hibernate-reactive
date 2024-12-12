/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.type.descriptor.jdbc;

import org.hibernate.dialect.Dialect;
import org.hibernate.tool.schema.extract.spi.ColumnTypeInformation;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.XmlArrayJdbcTypeConstructor;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * @see XmlArrayJdbcTypeConstructor
 * @see ReactiveArrayJdbcType
 */
public class ReactiveXmlArrayJdbcTypeConstructor extends XmlArrayJdbcTypeConstructor {
	public static final ReactiveXmlArrayJdbcTypeConstructor INSTANCE = new ReactiveXmlArrayJdbcTypeConstructor();

	@Override
	public JdbcType resolveType(
			TypeConfiguration typeConfiguration,
			Dialect dialect,
			BasicType<?> elementType,
			ColumnTypeInformation columnTypeInformation) {
		return resolveType( typeConfiguration, dialect, elementType.getJdbcType(), columnTypeInformation );
	}

	@Override
	public JdbcType resolveType(
			TypeConfiguration typeConfiguration,
			Dialect dialect,
			JdbcType elementType,
			ColumnTypeInformation columnTypeInformation) {
		return new ReactiveXmlArrayJdbcType( elementType );
	}
}
