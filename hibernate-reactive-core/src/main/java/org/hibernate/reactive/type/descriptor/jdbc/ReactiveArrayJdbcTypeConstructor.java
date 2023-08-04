/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.type.descriptor.jdbc;

import java.sql.Types;

import org.hibernate.dialect.Dialect;
import org.hibernate.reactive.type.descriptor.jdbc.ReactiveArrayJdbcType;
import org.hibernate.tool.schema.extract.spi.ColumnTypeInformation;
import org.hibernate.type.BasicType;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Factory for {@link ReactiveArrayJdbcType}.
 */
public class ReactiveArrayJdbcTypeConstructor implements JdbcTypeConstructor {
	public static final ReactiveArrayJdbcTypeConstructor INSTANCE = new ReactiveArrayJdbcTypeConstructor();

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
		return new ReactiveArrayJdbcType( elementType );
	}

	@Override
	public int getDefaultSqlTypeCode() {
		return Types.ARRAY;
	}
}
