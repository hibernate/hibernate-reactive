/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.type.descriptor.jdbc;

import org.hibernate.dialect.Dialect;
import org.hibernate.tool.schema.extract.spi.ColumnTypeInformation;
import org.hibernate.type.BasicType;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcTypeConstructor;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * @see org.hibernate.type.descriptor.jdbc.JsonArrayJdbcType
 * @see ReactiveJsonArrayJdbcType
 */
public class ReactiveJsonArrayJdbcTypeConstructor implements JdbcTypeConstructor {
	public static final ReactiveJsonArrayJdbcTypeConstructor INSTANCE = new ReactiveJsonArrayJdbcTypeConstructor();

	public JdbcType resolveType(
			TypeConfiguration typeConfiguration,
			Dialect dialect,
			BasicType<?> elementType,
			ColumnTypeInformation columnTypeInformation) {
		return resolveType( typeConfiguration, dialect, elementType.getJdbcType(), columnTypeInformation );
	}

	public JdbcType resolveType(
			TypeConfiguration typeConfiguration,
			Dialect dialect,
			JdbcType elementType,
			ColumnTypeInformation columnTypeInformation) {
		return new ReactiveJsonArrayJdbcType( elementType );
	}

	@Override
	public int getDefaultSqlTypeCode() {
		return SqlTypes.JSON_ARRAY;
	}
}
